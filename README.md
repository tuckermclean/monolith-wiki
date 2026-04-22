# Monolith Wiki

> *"Here is what humanity decided to teach you first."*

A deterministic pipeline that distills the full English Wikipedia into a curated,
~30,000-article offline corpus — finite, authoritative, and navigable without a
network connection.

Encarta didn't win because it was exhaustive.
It won because it felt *finished*.
Monolith Wiki reproduces that feeling with 2026 Wikipedia as the source material.

---

## What it is

Monolith Wiki takes a no-pictures Wikipedia ZIM file (~22 GB) and runs it
through an 8-stage attenuation pipeline.
The output is a self-contained corpus of ~30,000 articles covering eight
knowledge domains, compressed to ~250–400 MB, with every internal link either
resolved to a corpus member or quietly killed.

It is not a summary. Every article is the full Wikipedia text, stripped of
navigation chrome, dead references, and edit-history noise, then stored as
clean HTML.

You get the depth of Wikipedia with the *surface area* of Encarta.

---

## Why ~30,000 articles

Encarta 97 shipped with roughly 30,000 articles.
That number is not arbitrary — it is the approximate size of a knowledge graph
that feels complete without feeling infinite.
Below it, obvious gaps break the illusion. Above it, the illusion of
*completeness* dissolves into the feeling of a search engine.

The domain allocation mirrors the way an educated generalist thinks about the
world:

| Domain | Articles | Why |
|---|---|---|
| Geography & Places | ~6,000 | Encarta felt planetary |
| Biography | ~6,000 | People are how knowledge becomes memorable |
| History | ~4,000 | Narrative spine for everything else |
| Science & Math | ~4,000 | Explanatory power, not enumeration |
| Arts & Culture | ~4,000 | Human continuity, mythology, philosophy |
| Technology & Computing | ~3,000 | Structural, not commercial |
| Society, Politics & Economics | ~2,000 | Context, not news |
| Meta / Reference | ~1,000 | Navigation stability for offline use |

---

## How selection works

The pipeline does not write new content or choose articles by topic keyword.
It curates by *graph topology*.

The core idea: **select articles that form a stable knowledge graph when
severed from the rest of Wikipedia.**

An article earns its place by being:

1. **Non-stub** — substantial lead section, at least one explanatory section,
   meaningful in/out link count.
2. **A hub** — heavily linked to and from related concepts, not a leaf node.
3. **Temporally sober** — relevant because of what it explains, not because of
   recent news velocity.

This naturally produces a corpus that feels exploreable: you can keep asking
questions inside it and never feel lost.

---

## Pipeline stages

```
Stage 0  Index       Stream the ZIM; record every article, link, and category
Stage 1  Pre-filter  Drop redirects, disambiguation pages, stubs, and orphans
Stage 2  Domain      Classify each article into one of the eight domains
Stage 3  KSS         Score each article for Knowledge Stability (graph + length + longevity)
Stage 4  Quota       Select top-KSS articles up to each domain's article count
Stage 5  Repair      Pull in missing high-KSS articles linked by selected ones
Stage 6  Normalize   Strip chrome; rewrite links; rebuild TOC; enforce size budget
Stage 7  Package     Compress to zstd; write manifest and BUILD_HASH
Stage 8  Validate    Verify coverage, quotas, dead-end rate, and hash integrity
```

Every stage is idempotent. The pipeline can be interrupted and resumed at any
stage without reprocessing earlier work.

---

## Quickstart

**Requirements:** Python ≥ 3.11, `wget`, Docker (optional).

```bash
# 1. Download the latest no-pictures Wikipedia ZIM (~22 GB)
make fetch-zim

# 2. Run the pipeline
make all

# The corpus lands in data/build/corpus/
# Each article is data/build/corpus/{Domain}/{slug}.html.zst
```

To run inside Docker (no local Python setup needed):

```bash
make docker-build
make fetch-zim            # still runs on host; ZIM is bind-mounted in
make docker-run
```

To override the ZIM variant (e.g. the full with-images version):

```bash
make fetch-zim ZIM_BOOK_NAME=wikipedia_en_all_maxi
```

---

## Output structure

```
data/build/corpus/
├── Geography/
│   ├── France.html.zst
│   └── …
├── Biography/
├── History/
├── …
├── assets/
│   └── style.css
├── manifest.json       # sorted list of all articles + metadata
├── report.json         # stage statistics and quota breakdown
└── BUILD_HASH          # SHA-256 of manifest (reproducibility seal)
```

Each `.html.zst` file decompresses to a self-contained HTML5 page with:
- relative links to other corpus articles
- unresolved links replaced with plain `<span class="mw-deadlink">` text
- math rendered as inline SVG (from the ZIM source)
- a rebuilt table of contents
- no images, no external dependencies

---

## Reproducibility

The `BUILD_HASH` file is a SHA-256 over the sorted manifest. Given the same
input ZIM and config, the pipeline produces a byte-identical corpus.

To pin a build: record the ZIM filename (e.g.
`wikipedia_en_all_nopic_2026-03.zim`) and the `BUILD_HASH`. That pair
identifies the exact corpus forever.

---

## CI / automated builds

The included [GitHub Actions workflow](.github/workflows/build-corpus.yml):

- Runs monthly on the 5th (after Kiwix publishes the new ZIM)
- Caches the ZIM by versioned filename (auto-invalidates on new releases)
- Uploads the corpus as a workflow artifact (retained 30 days)
- Publishes a GitHub Release when triggered by a `corpus-v*` tag push, or
  when `release: true` is set in a manual `workflow_dispatch`

```bash
# Trigger a manually-released build:
git tag corpus-v2026-04
git push origin corpus-v2026-04
```

---

## Configuration

All tunable parameters live in [`config/default.yaml`](config/default.yaml).
Domain quotas, KSS weights, size budgets, and ZIM path are all there.

Override via environment variable or CLI flag:

```bash
monolith-wiki --config my-config.yaml run-all
MONOLITH_CONFIG=my-config.yaml monolith-wiki run-all
```

---

## Development

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
make test
```

Tests run entirely against in-memory SQLite — no ZIM file needed.

---

## What it is not

- Not a search engine. There is no query interface.
- Not a summarizer. Articles are Wikipedia text, cleaned but unedited.
- Not a news archive. Recency is a negative signal in the KSS scoring.
- Not a replacement for the internet. It is a replacement for the feeling that
  one is necessary.

---

## The sanity check

If a curious ten-year-old could sit down with the corpus and answer all of
these questions from within it, the build is good:

- *Where am I on Earth?*
- *How did things get this way?*
- *What is the universe made of?*
- *Who figured this out?*
- *How does the machine I'm using actually work?*
- *What don't we know yet?*

If all six land — you have Encarta parity, and then some.
