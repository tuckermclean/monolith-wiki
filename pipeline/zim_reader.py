"""
pipeline.zim_reader — abstraction layer over python-libzim.

Key design decisions:
  - We use ``libzim.reader.Archive`` from the ``libzim`` PyPI package.
  - Iteration uses the private ``_get_entry_by_id`` method because the
    public Python bindings do not expose a stable iterator as of libzim 3.x.
    This is intentional and documented here; it is the only available approach.
  - Modern English Wikipedia ZIM files (2020+) use the "new namespace scheme"
    where paths have no namespace prefix (e.g. "Albert_Einstein" not
    "A/Albert_Einstein").  Older ZIM files used a namespace prefix; both are
    handled transparently.
  - Category pages, redirect pages, and non-article entries are skipped by
    ``iter_main_articles()``.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Generator

# libzim is an optional dependency at import time so that unit tests that
# do not need a real ZIM file can still import the rest of the pipeline.
try:
    from libzim.reader import Archive as _Archive  # type: ignore[import]
    _LIBZIM_AVAILABLE = True
except ImportError:
    _LIBZIM_AVAILABLE = False
    _Archive = None  # type: ignore[assignment, misc]


# ---------------------------------------------------------------------------
# Path-normalisation helpers
# ---------------------------------------------------------------------------

# Prefixes that indicate non-article entries in old-scheme ZIM files.
_NON_ARTICLE_PREFIXES = (
    "Category:",
    "Wikipedia:",
    "Help:",
    "Portal:",
    "File:",
    "Template:",
    "MediaWiki:",
    "Module:",
    "Special:",
    "Talk:",
    "User:",
    "User_talk:",
    "Draft:",
)

# Old-scheme namespace prefixes that are NOT the article namespace (A/).
_OLD_NS_SKIP = {"-", "I", "J", "U", "W", "X", "0", "1", "2", "3", "4", "5",
                "6", "7", "8", "9"}


def _is_main_article_path(path: str, has_new_ns: bool) -> bool:
    """Return True if *path* refers to a main-namespace article."""
    if has_new_ns:
        # New scheme: category pages appear as "Category:Foo", etc.
        return not any(path.startswith(p) for p in _NON_ARTICLE_PREFIXES)
    else:
        # Old scheme: articles are in the "A/" namespace.
        return path.startswith("A/")


def _normalise_path(path: str, has_new_ns: bool) -> str:
    """Strip the old-scheme 'A/' namespace prefix if present."""
    if not has_new_ns and path.startswith("A/"):
        return path[2:]
    return path


# ---------------------------------------------------------------------------
# Dataclass for a parsed article entry
# ---------------------------------------------------------------------------

@dataclass(slots=True)
class ZimArticle:
    path: str          # normalised path (no A/ prefix)
    title: str
    html_size: int     # raw bytes from ZIM
    html: bytes        # full HTML content


# ---------------------------------------------------------------------------
# ZimReader
# ---------------------------------------------------------------------------

class ZimReader:
    """
    Lightweight wrapper around ``libzim.reader.Archive``.

    Parameters
    ----------
    zim_path : path-like
        Path to the ZIM file.
    """

    def __init__(self, zim_path: str | Path) -> None:
        if not _LIBZIM_AVAILABLE:
            raise ImportError(
                "libzim is not installed.  Run: pip install libzim"
            )
        zim_path = Path(zim_path)
        if not zim_path.exists():
            raise FileNotFoundError(f"ZIM file not found: {zim_path.resolve()}")

        self._archive = _Archive(str(zim_path))
        self._has_new_ns: bool = self._archive.has_new_namespace_scheme

    # ------------------------------------------------------------------
    # Public properties
    # ------------------------------------------------------------------

    @property
    def entry_count(self) -> int:
        """Total entry count (includes all namespaces, redirects, assets)."""
        return self._archive.entry_count

    @property
    def has_new_namespace_scheme(self) -> bool:
        return self._has_new_ns

    # ------------------------------------------------------------------
    # Iteration
    # ------------------------------------------------------------------

    def iter_main_articles(
        self, progress_callback=None
    ) -> Generator[ZimArticle, None, None]:
        """
        Yield every main-namespace, non-redirect article in the ZIM.

        ``progress_callback(i, total)`` is called for every entry if provided
        (useful for progress bars).
        """
        total = self._archive.entry_count
        for i in range(total):
            # _get_entry_by_id is the only available iteration method in the
            # Python bindings (no public iterator exposed as of libzim 3.x).
            entry = self._archive._get_entry_by_id(i)  # noqa: SLF001

            if progress_callback is not None:
                progress_callback(i, total)

            if entry.is_redirect:
                continue

            path = entry.path
            if not _is_main_article_path(path, self._has_new_ns):
                continue

            # Skip non-HTML MIME types (images, CSS, JS embedded in ZIM).
            try:
                item = entry.get_item()
            except Exception:
                continue

            if "html" not in item.mimetype.lower():
                continue

            norm_path = _normalise_path(path, self._has_new_ns)
            yield ZimArticle(
                path=norm_path,
                title=entry.title or norm_path.replace("_", " "),
                html_size=item.size,
                html=bytes(item.content),
            )

    def get_article_by_path(self, path: str) -> ZimArticle | None:
        """
        Fetch a single article by its normalised path.

        Returns ``None`` if the path is not found or is not a main article.
        """
        # In new-scheme ZIMs the path is used directly; old-scheme uses A/ prefix.
        lookup = path if self._has_new_ns else f"A/{path}"
        try:
            entry = self._archive.get_entry_by_path(lookup)
        except KeyError:
            return None

        if entry.is_redirect:
            return None

        try:
            item = entry.get_item()
        except Exception:
            return None

        if "html" not in item.mimetype.lower():
            return None

        return ZimArticle(
            path=path,
            title=entry.title or path.replace("_", " "),
            html_size=item.size,
            html=bytes(item.content),
        )
