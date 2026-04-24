"""
pipeline.stages.s6_normalize — Stage 6: Content Normalisation & Size Discipline.

For each selected article this stage:

  1. Fetches its HTML from the ZIM file.
  2. Strips editorial scaffolding (maintenance boxes, edit links, footers,
     navboxes, dispute banners, image tags).
  3. Rewrites internal ``<a href>`` links:
     - Links to other *selected* articles → relative path to their output file.
     - Links to non-selected articles → ``<span class="mw-deadlink">`` plain text.
  4. Rebuilds a clean table-of-contents from H2/H3 headings.
  5. Wraps the result in a minimal HTML5 document referencing the shared CSS.
  6. Writes cleaned HTML to ``build/normalized/{domain}/{slug}.html``.
  7. Estimates compressed size (gzip); aborts the build if the running total
     would exceed ``max_total_compressed_bytes``.

Transforms are content-preserving:
  - No summarisation, tone changes, or editorial rewrites.
  - Inline math SVG is kept as-is (already rendered by mwoffliner).
  - Tables are kept; only display attributes are normalised.

Forbidden artefacts removed
---------------------------
CSS classes / IDs stripped unconditionally:
  .ambox, .mbox-small, .tmbox, .ombox, .cmbox, .fmbox  (maintenance/dispute)
  .navbox, .navbox-inner, .navbox-list                  (navigation boxes)
  .sistersitebox, .noprint                              (sidebar clutter)
  .mw-editsection, .mw-editsection-bracket             (edit buttons)
  #catlinks, #footer, #mw-navigation, #mw-head          (page chrome)
  #coordinates                                           (geo coords box)
  .hatnote (disambiguation hatnotes — these refer to other articles)
  img (all raster images; math SVG is inline and kept)
"""

from __future__ import annotations

import gzip
import hashlib
import logging
import os
import re
import sqlite3
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import unquote, quote

from bs4 import BeautifulSoup, NavigableString, Tag
from tqdm import tqdm

from pipeline.config import Config
from pipeline.db import record_stage_complete, iter_rows
from pipeline.zim_reader import ZimReader

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Elements to remove entirely (by CSS class — any matching class removes it)
# ---------------------------------------------------------------------------
_REMOVE_CLASSES: set[str] = {
    "ambox", "mbox-small", "tmbox", "ombox", "cmbox", "fmbox",
    "navbox", "navbox-inner", "navbox-list", "navbox-title",
    "sistersitebox", "noprint",
    "mw-editsection", "mw-editsection-bracket",
    "hatnote",
    "stub", "stub-article",
    "printfooter",
    "mw-authority-control",
    "dmbox", "dmbox-disambig",
}

_REMOVE_IDS: set[str] = {
    "catlinks",
    "footer",
    "mw-navigation",
    "mw-head",
    "mw-head-base",
    "mw-page-base",
    "coordinates",
    "mw-panel",
    "siteSub",
    "contentSub",
    "jump-to-nav",
    "toc",   # We rebuild TOC ourselves.
}


# ---------------------------------------------------------------------------
# HTML cleaning
# ---------------------------------------------------------------------------

def _element_has_bad_class(tag: Tag) -> bool:
    classes = tag.get("class", [])
    if isinstance(classes, str):
        classes = classes.split()
    return bool(set(classes) & _REMOVE_CLASSES)


def _element_has_bad_id(tag: Tag) -> bool:
    return tag.get("id", "") in _REMOVE_IDS


def _strip_bad_elements(soup: BeautifulSoup) -> None:
    """Remove all elements that match the forbidden class/id lists."""
    # Two-phase: collect first, then remove.  Decomposing a parent mid-iteration
    # leaves its children as orphaned tags in the result list; calling .get() on a
    # partially-destroyed tag raises AttributeError in some BS4 versions.
    to_remove = [
        tag for tag in soup.find_all(True)
        if not isinstance(tag, NavigableString)
        and (_element_has_bad_class(tag) or _element_has_bad_id(tag))
    ]
    for tag in to_remove:
        # Skip if already removed as a child of a previously decomposed parent.
        if getattr(tag, "parent", None) is not None:
            tag.decompose()


def _strip_images(soup: BeautifulSoup) -> None:
    """Remove all <img> tags. Keep inline SVG (math rendering)."""
    for img in soup.find_all("img"):
        img.decompose()


def _strip_style_and_script(soup: BeautifulSoup) -> None:
    """Remove all <style> and <script> tags."""
    for tag in soup.find_all(["style", "script"]):
        tag.decompose()


def _rewrite_links(
    soup: BeautifulSoup,
    path_to_output: dict[str, str],
    current_article_domain: str,
) -> None:
    """
    Rewrite internal <a href> links.

    Links to selected articles get a relative href pointing to their
    output file.  Links to excluded articles become plain <span> elements
    with class "mw-deadlink" to preserve text without broken navigation.
    """
    for a in soup.find_all("a", href=True):
        href: str = a["href"].strip()

        # Leave external links and anchors alone.
        if href.startswith(("http://", "https://", "//", "mailto:", "#")):
            continue

        # Strip fragment.
        fragment = ""
        if "#" in href:
            href, fragment = href.split("#", 1)

        if not href:
            continue

        try:
            norm = unquote(href, errors="strict").lstrip("./")
        except UnicodeDecodeError:
            a.replace_with(a.get_text())
            continue

        if norm in path_to_output:
            rel = _relative_path(current_article_domain, path_to_output[norm])
            a["href"] = rel + ("#" + fragment if fragment else "")
            # Remove external-link class decorators.
            a.attrs = {k: v for k, v in a.attrs.items() if k in ("href", "title")}
        else:
            # Replace with <span class="mw-deadlink"> containing the text.
            # Build via mini-parse to avoid BS4 Tag.__getattr__ quirk when
            # `soup` is a Tag rather than a BeautifulSoup root.
            span = BeautifulSoup(
                '<span class="mw-deadlink"></span>', "html.parser"
            ).find("span")
            span.string = a.get_text()
            a.replace_with(span)


def _relative_path(from_domain: str, to_output_path: str) -> str:
    """
    Compute a relative href from an article in ``from_domain`` to
    ``to_output_path`` (which is relative to the corpus root).

    e.g. from_domain="Geography", to_output_path="Biography/Ada_Lovelace.html"
    → "../Biography/Ada_Lovelace.html"
    """
    # Output paths are relative to corpus root: "{domain}/{slug}.html"
    # Normalised articles live one directory deep.
    return "../" + to_output_path


def _build_toc(content_div: Tag, root: BeautifulSoup) -> Tag | None:
    """Build a simple TOC from H2/H3 headings; return the nav element."""
    headings = content_div.find_all(["h2", "h3"])
    if len(headings) < 3:
        return None

    nav = root.new_tag("nav", attrs={"id": "toc", "class": "toc"})
    toc_title = root.new_tag("div", attrs={"class": "toc-title"})
    toc_title.string = "Contents"
    nav.append(toc_title)
    ol = root.new_tag("ol")
    nav.append(ol)

    h2_counter = 0
    h3_counter = 0
    current_h2_li = None

    for heading in headings:
        anchor_id = heading.get("id") or re.sub(r"\W+", "_", heading.get_text()).strip("_")
        heading["id"] = anchor_id
        text = heading.get_text().strip()

        if heading.name == "h2":
            h2_counter += 1
            h3_counter = 0
            li = root.new_tag("li", attrs={"class": "toc-h2"})
            a = root.new_tag("a", href=f"#{anchor_id}")
            a.string = f"{h2_counter}. {text}"
            li.append(a)
            current_h2_li = root.new_tag("ol")
            li.append(current_h2_li)
            ol.append(li)
        elif heading.name == "h3" and current_h2_li is not None:
            h3_counter += 1
            li = root.new_tag("li", attrs={"class": "toc-h3"})
            a = root.new_tag("a", href=f"#{anchor_id}")
            a.string = f"{h2_counter}.{h3_counter} {text}"
            li.append(a)
            current_h2_li.append(li)

    return nav


# ---------------------------------------------------------------------------
# HTML template
# ---------------------------------------------------------------------------

_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>{title}</title>
<link rel="stylesheet" href="{css_rel}">
</head>
<body>
<article class="mw-body">
<h1 class="firstHeading">{title}</h1>
{toc}
{content}
</article>
</body>
</html>
"""


def _build_output_html(title: str, toc_html: str, content_html: str, css_rel: str) -> str:
    return _HTML_TEMPLATE.format(
        title=_html_escape(title),
        toc=toc_html,
        content=content_html,
        css_rel=css_rel,
    )


def _html_escape(s: str) -> str:
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


# ---------------------------------------------------------------------------
# Slug generation
# ---------------------------------------------------------------------------

def _make_slug(path: str) -> str:
    """Turn an article path into a safe filename (no special chars)."""
    slug = path.replace(" ", "_")
    slug = re.sub(r"[^\w\-.]", "_", slug)
    return slug[:200]  # hard cap to stay within filesystem limits


def _estimate_gzip_size(html: str) -> int:
    """Return the gzip-compressed byte count as a size estimate."""
    return len(gzip.compress(html.encode("utf-8"), compresslevel=6))


# ---------------------------------------------------------------------------
# Worker pool — process initializer and per-article function
# ---------------------------------------------------------------------------

# Globals set once per worker process via initializer; avoids pickling the
# full path_to_output dict with every task.
_W_PATH_TO_OUTPUT: dict[str, str] = {}
_W_CSS_REL: str = "../assets/style.css"


def _init_normalize_worker(path_to_output: dict, css_rel: str) -> None:
    global _W_PATH_TO_OUTPUT, _W_CSS_REL  # noqa: PLW0603
    _W_PATH_TO_OUTPUT = path_to_output
    _W_CSS_REL = css_rel


def _normalize_worker(args: tuple) -> tuple[str, str] | None:
    """
    Normalize one article's HTML.  Runs in a worker process.
    Returns (path, output_html) or None on failure.
    """
    path, title, domain, html_bytes = args
    try:
        from bs4 import BeautifulSoup as _BS4  # noqa: PLC0415
        soup = _BS4(html_bytes, "lxml")
        _strip_bad_elements(soup)
        _strip_images(soup)
        _strip_style_and_script(soup)
        content_div = (
            soup.find(id="mw-content-text") or soup.find("body") or soup
        )
        _rewrite_links(content_div, _W_PATH_TO_OUTPUT, domain)
        toc = _build_toc(content_div, soup)
        output_html = _build_output_html(
            title=title,
            toc_html=str(toc) if toc else "",
            content_html=str(content_div),
            css_rel=_W_CSS_REL,
        )
        return (path, output_html)
    except Exception as exc:  # noqa: BLE001
        import logging as _log  # noqa: PLC0415
        import traceback as _tb  # noqa: PLC0415
        _log.getLogger(__name__).warning(
            "Stage 6: normalize failed %s: %s\n%s",
            path, exc, _tb.format_exc(),
        )
        return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run(cfg: Config, conn: sqlite3.Connection) -> None:
    """Execute Stage 6: normalise selected articles and write HTML files."""
    out_dir = cfg.normalized_path
    corpus_dir = cfg.corpus_path
    out_dir.mkdir(parents=True, exist_ok=True)
    corpus_dir.mkdir(parents=True, exist_ok=True)
    cfg.assets_path.mkdir(parents=True, exist_ok=True)

    # ------------------------------------------------------------------
    # Build path → output_path map for selected articles (needed for link
    # rewriting before we process each article).
    # ------------------------------------------------------------------
    logger.info("Stage 6: building output path map for selected articles …")
    path_to_row: dict[str, dict] = {}
    for row in iter_rows(
        conn,
        "SELECT id, path, title, domain FROM articles WHERE selected = 1",
    ):
        path_to_row[row["path"]] = dict(row)

    selected_paths: list[str] = list(path_to_row.keys())

    # Assign output paths: "{domain}/{slug}.html"
    path_to_output: dict[str, str] = {}
    for path, row in path_to_row.items():
        slug = _make_slug(path)
        output_path = f"{row['domain']}/{slug}.html"
        path_to_output[path] = output_path

    # Write output_path back to the DB.
    logger.info("Stage 6: writing output paths to DB …")
    conn.executemany(
        "UPDATE articles SET output_path = ? WHERE path = ?",
        [(op, p) for p, op in path_to_output.items()],
    )
    conn.commit()

    # ------------------------------------------------------------------
    # Open ZIM reader (main thread only — not shared with workers)
    # ------------------------------------------------------------------
    reader = ZimReader(cfg.input.zim_path)
    css_rel = "../assets/style.css"
    n_workers: int = cfg.build.workers or min(os.cpu_count() or 2, 8)
    logger.info("Stage 6: normalising %d articles with %d workers ...",
                len(selected_paths), n_workers)

    # ------------------------------------------------------------------
    # Process articles in parallel.
    # Main thread reads HTML from ZIM (single-threaded), submits batches
    # to worker pool.  Workers do the BS4 heavy lifting.
    # path_to_output is shared once via process initializer.
    # ------------------------------------------------------------------
    running_compressed_bytes = 0
    max_bytes = cfg.build.max_total_compressed_bytes
    normalised_count = 0
    failed_count = 0
    _CHUNK = 200  # articles per pool.map chunk

    def _write_result(path: str, output_html: str) -> None:
        nonlocal running_compressed_bytes, normalised_count
        compressed_estimate = _estimate_gzip_size(output_html)
        if running_compressed_bytes + compressed_estimate > max_bytes:
            raise RuntimeError(
                f"Stage 6: compressed size budget exceeded at article '{path}'."
                f"  Running={running_compressed_bytes / 1e6:.1f} MB +"
                f" {compressed_estimate / 1024:.1f} KB >"
                f" {max_bytes / 1e6:.1f} MB limit."
            )
        running_compressed_bytes += compressed_estimate
        output_rel = path_to_output[path]
        output_abs = cfg.normalized_path / output_rel
        output_abs.parent.mkdir(parents=True, exist_ok=True)
        output_abs.write_text(output_html, encoding="utf-8")
        normalised_count += 1

    with ProcessPoolExecutor(
        max_workers=n_workers,
        initializer=_init_normalize_worker,
        initargs=(path_to_output, css_rel),
    ) as pool:
        # Build task args in chunks to keep ZIM reads and RSS bounded.
        for chunk_start in tqdm(
            range(0, len(selected_paths), _CHUNK),
            desc="s6 normalize", unit="chunk"
        ):
            chunk = selected_paths[chunk_start: chunk_start + _CHUNK]
            tasks = []
            for path in chunk:
                row = path_to_row[path]
                article = reader.get_article_by_path(path)
                if article is None:
                    logger.warning("Stage 6: article not found in ZIM: %s", path)
                    failed_count += 1
                    continue
                tasks.append((path, row["title"], row["domain"], article.html))

            for result in pool.map(_normalize_worker, tasks, chunksize=10):
                if result is None:
                    failed_count += 1
                    continue
                _write_result(result[0], result[1])

    logger.info(
        "Stage 6: complete.  %d normalised, %d failed.  "
        "Estimated compressed size: %.1f MB.",
        normalised_count, failed_count,
        running_compressed_bytes / 1e6,
    )

    record_stage_complete(
        conn,
        "s6",
        article_count=normalised_count,
        notes=f"estimated_compressed_mb={running_compressed_bytes / 1e6:.1f}",
    )


# ---------------------------------------------------------------------------
# __main__
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse
    import logging as _logging

    from pipeline.config import load_config
    from pipeline.db import open_db

    _logging.basicConfig(level=_logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Run Stage 6: Content normalisation")
    parser.add_argument("--config", default=None)
    args = parser.parse_args()
    _cfg = load_config(args.config)
    _conn = open_db(_cfg.db_path)
    run(_cfg, _conn)
    _conn.close()
