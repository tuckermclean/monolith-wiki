"""tests/test_normalize.py — unit tests for Stage 6 HTML normalisation helpers."""

from __future__ import annotations

import pytest
from bs4 import BeautifulSoup

from pipeline.stages.s6_normalize import (
    _strip_bad_elements,
    _strip_images,
    _strip_style_and_script,
    _rewrite_links,
    _build_toc,
    _make_slug,
    _build_output_html,
)


def _soup(html: str) -> BeautifulSoup:
    return BeautifulSoup(html, "lxml")


# ---------------------------------------------------------------------------
# Strip bad elements
# ---------------------------------------------------------------------------

def test_strips_ambox():
    soup = _soup('<div id="mw-content-text"><div class="ambox">Dispute</div><p>Content</p></div>')
    _strip_bad_elements(soup)
    assert soup.find(class_="ambox") is None
    assert soup.find("p") is not None


def test_strips_navbox():
    soup = _soup('<div id="mw-content-text"><div class="navbox">Nav</div><p>Text</p></div>')
    _strip_bad_elements(soup)
    assert soup.find(class_="navbox") is None


def test_strips_mw_editsection():
    soup = _soup('<p>Para <span class="mw-editsection">[edit]</span></p>')
    _strip_bad_elements(soup)
    assert soup.find(class_="mw-editsection") is None


def test_strips_footer_by_id():
    soup = _soup('<div id="footer">Footer text</div><p>Content</p>')
    _strip_bad_elements(soup)
    assert soup.find(id="footer") is None


def test_strips_catlinks_by_id():
    soup = _soup('<div id="catlinks">Categories: ...</div><p>Art</p>')
    _strip_bad_elements(soup)
    assert soup.find(id="catlinks") is None


def test_keeps_paragraph():
    soup = _soup('<p class="intro">Hello world</p>')
    _strip_bad_elements(soup)
    assert soup.find("p") is not None


def test_keeps_math_svg():
    html = '<span class="mwe-math-element"><svg><text>x²</text></svg></span>'
    soup = _soup(html)
    _strip_bad_elements(soup)
    assert soup.find("svg") is not None


# ---------------------------------------------------------------------------
# Strip images
# ---------------------------------------------------------------------------

def test_strips_img_tags():
    soup = _soup('<p><img src="photo.jpg" alt="Photo"/></p><p>Text</p>')
    _strip_images(soup)
    assert soup.find("img") is None


def test_keeps_svg_after_strip_images():
    soup = _soup('<span class="mwe-math-element"><svg><rect/></svg></span>')
    _strip_images(soup)
    # SVG should survive — only <img> is removed.
    assert soup.find("svg") is not None


# ---------------------------------------------------------------------------
# Strip style / script
# ---------------------------------------------------------------------------

def test_strips_style_tag():
    soup = _soup('<style>.foo{color:red}</style><p>Text</p>')
    _strip_style_and_script(soup)
    assert soup.find("style") is None


def test_strips_script_tag():
    soup = _soup('<script>alert(1)</script><p>Text</p>')
    _strip_style_and_script(soup)
    assert soup.find("script") is None


# ---------------------------------------------------------------------------
# Link rewriting
# ---------------------------------------------------------------------------

def test_rewrites_selected_link():
    soup = _soup('<div id="mw-content-text"><a href="France">France</a></div>')
    body = soup.find(id="mw-content-text")
    path_to_output = {"France": "Geography/France.html"}
    _rewrite_links(body, path_to_output, "Geography")
    a = body.find("a")
    assert a is not None
    assert "France.html" in a["href"]


def test_converts_unresolved_link_to_span():
    soup = _soup('<div id="mw-content-text"><a href="Missing_Article">Missing</a></div>')
    body = soup.find(id="mw-content-text")
    _rewrite_links(body, {}, "Geography")
    assert body.find("a") is None
    span = body.find("span", class_="mw-deadlink")
    assert span is not None
    assert span.get_text() == "Missing"


def test_leaves_external_links_alone():
    soup = _soup('<div id="mw-content-text"><a href="https://example.com">Ext</a></div>')
    body = soup.find(id="mw-content-text")
    _rewrite_links(body, {}, "Geography")
    a = body.find("a")
    assert a is not None
    assert a["href"] == "https://example.com"


def test_strips_fragment_for_lookup_but_keeps_in_href():
    soup = _soup('<div id="mw-content-text"><a href="France#History">France History</a></div>')
    body = soup.find(id="mw-content-text")
    path_to_output = {"France": "Geography/France.html"}
    _rewrite_links(body, path_to_output, "Geography")
    a = body.find("a")
    assert a is not None
    assert "#History" in a["href"]


# ---------------------------------------------------------------------------
# TOC builder
# ---------------------------------------------------------------------------

def test_toc_built_from_headings():
    soup = _soup("""
    <div>
      <h2 id="h2a">Section One</h2><p>Para</p>
      <h3 id="h3a">Subsection</h3><p>Sub</p>
      <h2 id="h2b">Section Two</h2><p>Para</p>
    </div>
    """)
    toc = _build_toc(soup)
    assert toc is not None
    toc_text = toc.get_text()
    assert "Section One" in toc_text
    assert "Section Two" in toc_text
    assert "Subsection" in toc_text


def test_toc_returns_none_for_few_headings():
    soup = _soup('<div><h2>Only</h2></div>')
    toc = _build_toc(soup)
    assert toc is None


# ---------------------------------------------------------------------------
# Slug / output HTML
# ---------------------------------------------------------------------------

def test_make_slug_safe():
    assert "/" not in _make_slug("Albert Einstein")
    assert "?" not in _make_slug("What?")
    assert len(_make_slug("x" * 300)) <= 200


def test_build_output_html_has_title():
    html = _build_output_html("Test Title", "", "<p>content</p>", "../assets/style.css")
    assert "Test Title" in html
    assert "<!DOCTYPE html>" in html
    assert "../assets/style.css" in html
