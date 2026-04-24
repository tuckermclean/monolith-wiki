# ---------------------------------------------------------------------------
# Monolith Wiki — Build Orchestration
# ---------------------------------------------------------------------------
# Usage:
#   make fetch-zim    — download latest Wikipedia no-pictures ZIM from Kiwix
#   make all          — run full pipeline (resume-safe via sentinels)
#   make stage0       — ZIM indexing only
#   make mini         — download Ray Charles mini ZIM + run full pipeline (CI)
#   make mini-run     — run pipeline with existing mini ZIM (no download)
#   make clean        — remove all build artefacts (keeps raw ZIM input)
#   make clean-mini   — remove mini build dir and downloaded mini ZIM
#   make dev          — download top-100 nopic ZIM (~13 MB) + run pipeline
#   make dev-run      — run pipeline with existing dev ZIM (no download)
#   make clean-dev    — remove dev build dir and downloaded dev ZIM
#   make clean-stage  STAGE=s3  — rerun from a specific stage onward
#   make test         — run pytest suite
#   make docker-build — build Docker image
#   make docker-run   — run full pipeline inside Docker
#
# Requires: MONOLITH_CONFIG env var or config/default.yaml present.
# ---------------------------------------------------------------------------

VENV           ?= $(HOME)/monolith-venv
PYTHON         ?= $(VENV)/bin/python3
CONFIG         ?= config/default.yaml
BUILD_DIR      ?= data/build
SENTINEL       := $(BUILD_DIR)/.sentinels

# ZIM acquisition settings
ZIM_INPUT_DIR  ?= data/input
ZIM_DEST       ?= $(ZIM_INPUT_DIR)/wikipedia_en.zim
KIWIX_DOWNLOAD := https://download.kiwix.org/zim/wikipedia
# Use the no-pictures variant (smallest, sufficient — pipeline strips images anyway).
# Override to e.g. wikipedia_en_all_maxi for the image-inclusive ZIM.
ZIM_BOOK_NAME  ?= wikipedia_en_all_nopic

MINI_ZIM_BOOK  := wikipedia_en_ray-charles
MINI_ZIM_DEST  := data/input/wikipedia_en_mini.zim
MINI_CONFIG    := config/mini.yaml
MINI_BUILD_DIR := data/build-mini

DEV_ZIM_BOOK   := wikipedia_en_100_nopic
DEV_ZIM_DEST   := data/input/wikipedia_en_dev.zim
DEV_CONFIG     := config/dev.yaml
DEV_BUILD_DIR  := data/build-dev

ZIM_FILE    ?= $(shell $(PYTHON) -c \
    "import yaml; c=yaml.safe_load(open('$(CONFIG)')); print(c['input']['zim_path'])" 2>/dev/null || echo "UNKNOWN")

.PHONY: all stage0 stage1 stage2 stage3 stage4 stage5 stage6 stage7 stage8 \
        validate test clean clean-mini clean-dev clean-stage fetch-zim \
        mini mini-run dev dev-run docker-build docker-run

# ------------------------------------------------------------------
# Full pipeline
# ------------------------------------------------------------------
all: stage8

$(BUILD_DIR):
	mkdir -p $(BUILD_DIR)

$(SENTINEL):
	mkdir -p $(SENTINEL)

# ------------------------------------------------------------------
# Individual stages (each depends on the previous sentinel)
# ------------------------------------------------------------------
$(SENTINEL)/s0.done: $(ZIM_FILE) $(SENTINEL)
	$(PYTHON) -m pipeline.stages.s0_index --config $(CONFIG)
	touch $@

$(SENTINEL)/s1.done: $(SENTINEL)/s0.done
	$(PYTHON) -m pipeline.stages.s1_prefilter --config $(CONFIG)
	touch $@

$(SENTINEL)/s2.done: $(SENTINEL)/s1.done
	$(PYTHON) -m pipeline.stages.s2_domain --config $(CONFIG)
	touch $@

$(SENTINEL)/s3.done: $(SENTINEL)/s2.done
	$(PYTHON) -m pipeline.stages.s3_kss --config $(CONFIG)
	touch $@

$(SENTINEL)/s4.done: $(SENTINEL)/s3.done
	$(PYTHON) -m pipeline.stages.s4_quota --config $(CONFIG)
	touch $@

$(SENTINEL)/s5.done: $(SENTINEL)/s4.done
	$(PYTHON) -m pipeline.stages.s5_repair --config $(CONFIG)
	touch $@

$(SENTINEL)/s6.done: $(SENTINEL)/s5.done
	$(PYTHON) -m pipeline.stages.s6_normalize --config $(CONFIG)
	touch $@

$(SENTINEL)/s7.done: $(SENTINEL)/s6.done
	$(PYTHON) -m pipeline.stages.s7_package --config $(CONFIG)
	touch $@

$(SENTINEL)/s8.done: $(SENTINEL)/s7.done
	$(PYTHON) -m pipeline.stages.s8_validate --config $(CONFIG)
	touch $@

stage0: $(SENTINEL)/s0.done
stage1: $(SENTINEL)/s1.done
stage2: $(SENTINEL)/s2.done
stage3: $(SENTINEL)/s3.done
stage4: $(SENTINEL)/s4.done
stage5: $(SENTINEL)/s5.done
stage6: $(SENTINEL)/s6.done
stage7: $(SENTINEL)/s7.done
stage8: $(SENTINEL)/s8.done

# ------------------------------------------------------------------
# Clean
# ------------------------------------------------------------------
clean:
	rm -rf $(BUILD_DIR)

clean-mini:
	rm -rf $(MINI_BUILD_DIR)
	rm -f  $(MINI_ZIM_DEST) $(MINI_ZIM_DEST).tmp $(MINI_CONFIG)

# Rerun from a given stage onward. Example: make clean-stage STAGE=s3
clean-stage:
	@if [ -z "$(STAGE)" ]; then echo "Usage: make clean-stage STAGE=sN"; exit 1; fi
	@stages="s0 s1 s2 s3 s4 s5 s6 s7 s8"; \
	found=0; \
	for s in $$stages; do \
	    if [ $$found -eq 1 ] || [ "$$s" = "$(STAGE)" ]; then \
	        found=1; \
	        rm -f $(SENTINEL)/$$s.done; \
	        echo "Removed sentinel for $$s"; \
	    fi; \
	done

# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------
test:
	$(PYTHON) -m pytest tests/ -v --tb=short

# ------------------------------------------------------------------
# ZIM Acquisition
# ------------------------------------------------------------------
# Downloads the latest Kiwix no-pictures English Wikipedia ZIM and saves it
# to $(ZIM_DEST).  Supports resume (wget --continue) so a partial download
# can be retried safely.  Set http_proxy / https_proxy if behind a proxy.
#
# Example:
#   make fetch-zim
#   make fetch-zim ZIM_BOOK_NAME=wikipedia_en_all_maxi   # with images
fetch-zim:
	@mkdir -p $(ZIM_INPUT_DIR)
	@echo "==> [fetch-zim] Resolving latest $(ZIM_BOOK_NAME) from Kiwix download index ..."
	@url=$$($(PYTHON) -c \
	    "import urllib.request,re,sys; \
	     data=urllib.request.urlopen('$(KIWIX_DOWNLOAD)/').read().decode(); \
	     hits=sorted(set(re.findall(r'$(ZIM_BOOK_NAME)_[0-9]{4}-[0-9]{2}\.zim', data))); \
	     sys.stdout.write('$(KIWIX_DOWNLOAD)/' + hits[-1] if hits else '')"); \
	if [ -z "$$url" ]; then \
	    echo "ERROR: no ZIM found for '$(ZIM_BOOK_NAME)' at $(KIWIX_DOWNLOAD)/" >&2; \
	    echo "       Check network/proxy, or inspect $(KIWIX_DOWNLOAD)/ manually." >&2; \
	    exit 1; \
	fi; \
	echo "==> [fetch-zim] URL  : $$url"; \
	echo "==> [fetch-zim] Dest : $(ZIM_DEST)"; \
	wget --continue --progress=dot:giga -O "$(ZIM_DEST)" "$$url"

# ------------------------------------------------------------------
# Mini ZIM (Ray Charles test fixture, ~50 MB)
# ------------------------------------------------------------------
# Writes config/mini.yaml if it doesn't exist, downloads the ZIM if
# it isn't already present, then runs the full pipeline.
#
#   make mini          — fetch (once) + run
#   make mini-run      — run only (ZIM must already be downloaded)

$(MINI_CONFIG):
	@mkdir -p $(dir $(MINI_CONFIG))
	@echo "==> [mini] Writing $(MINI_CONFIG) ..."
	@$(PYTHON) -c "\
import textwrap, pathlib; \
pathlib.Path('$(MINI_CONFIG)').write_text(textwrap.dedent('''\
input:\n\
  zim_path: \"$(MINI_ZIM_DEST)\"\n\
  zim_sha256: null\n\
build:\n\
  build_dir: \"$(MINI_BUILD_DIR)\"\n\
  workers: 0\n\
prefilter:\n\
  min_text_chars: 500\n\
  min_inbound_links: 0\n\
quotas:\n\
  Geography: 10\n\
  Biography: 10\n\
  History: 10\n\
  Science_Math: 10\n\
  Arts_Culture: 10\n\
  Technology_Computing: 10\n\
  Society_Politics_Economics: 10\n\
  Meta_Reference: 10\n\
validation:\n\
  quota_tolerance: 3.0\n\
  max_dead_end_rate: 0.999\n\
  max_inbound_orphans: 50\n\
  max_outbound_orphans: 50\n\
  random_walk_count: 200\n\
'''))"

$(MINI_ZIM_DEST):
	@mkdir -p $(dir $(MINI_ZIM_DEST))
	@echo "==> [mini] Resolving latest $(MINI_ZIM_BOOK) from Kiwix ..."
	@url=$$($(PYTHON) -c \
	    "import urllib.request,re,sys; \
	     data=urllib.request.urlopen('$(KIWIX_DOWNLOAD)/').read().decode(); \
	     hits=sorted(set(re.findall(r'$(MINI_ZIM_BOOK)_mini_[0-9]{4}-[0-9]{2}\\.zim', data))); \
	     sys.stdout.write('$(KIWIX_DOWNLOAD)/' + hits[-1] if hits else '')"); \
	if [ -z "$$url" ]; then \
	    echo "ERROR: no ZIM found for '$(MINI_ZIM_BOOK)' at $(KIWIX_DOWNLOAD)/" >&2; exit 1; \
	fi; \
	echo "==> [mini] Downloading $$url ..."; \
	wget --continue --progress=dot:mega -O "$(MINI_ZIM_DEST).tmp" "$$url" && \
	mv "$(MINI_ZIM_DEST).tmp" "$(MINI_ZIM_DEST)"

mini: $(MINI_ZIM_DEST) $(MINI_CONFIG)
	$(PYTHON) -m pipeline.cli --config $(MINI_CONFIG) run-all

mini-run: $(MINI_CONFIG)
	$(PYTHON) -m pipeline.cli --config $(MINI_CONFIG) run-all

# ------------------------------------------------------------------
# Dev ZIM (wikipedia_en_100_nopic — top 100 articles, ~13 MB, no images)
# ------------------------------------------------------------------
# Full article text across many domains — good for local classification
# and content quality testing without downloading gigabytes.
#
#   make dev          — fetch (once) + run
#   make dev-run      — run only (ZIM must already be downloaded)
#   make clean-dev    — remove dev build dir and downloaded dev ZIM

$(DEV_CONFIG):
	@mkdir -p $(dir $(DEV_CONFIG))
	@echo "==> [dev] Writing $(DEV_CONFIG) ..."
	@$(PYTHON) -c "\
import textwrap, pathlib; \
pathlib.Path('$(DEV_CONFIG)').write_text(textwrap.dedent('''\
input:\n\
  zim_path: \"$(DEV_ZIM_DEST)\"\n\
  zim_sha256: null\n\
build:\n\
  build_dir: \"$(DEV_BUILD_DIR)\"\n\
  workers: 0\n\
prefilter:\n\
  min_text_chars: 500\n\
  min_inbound_links: 0\n\
quotas:\n\
  Geography: 999\n\
  Biography: 999\n\
  History: 999\n\
  Science_Math: 999\n\
  Arts_Culture: 999\n\
  Technology_Computing: 999\n\
  Society_Politics_Economics: 999\n\
  Meta_Reference: 999\n\
validation:\n\
  quota_tolerance: 3.0\n\
  max_dead_end_rate: 0.999\n\
  max_inbound_orphans: 999\n\
  max_outbound_orphans: 999\n\
  random_walk_count: 200\n\
'''))"

$(DEV_ZIM_DEST):
	@mkdir -p $(dir $(DEV_ZIM_DEST))
	@echo "==> [dev] Resolving latest $(DEV_ZIM_BOOK) from Kiwix ..."
	@url=$$($(PYTHON) -c \
	    "import urllib.request,re,sys; \
	     data=urllib.request.urlopen('$(KIWIX_DOWNLOAD)/').read().decode(); \
	     hits=sorted(set(re.findall(r'$(DEV_ZIM_BOOK)_[0-9]{4}-[0-9]{2}\\.zim', data))); \
	     sys.stdout.write('$(KIWIX_DOWNLOAD)/' + hits[-1] if hits else '')"); \
	if [ -z "$$url" ]; then \
	    echo "ERROR: no ZIM found for '$(DEV_ZIM_BOOK)' at $(KIWIX_DOWNLOAD)/" >&2; exit 1; \
	fi; \
	echo "==> [dev] Downloading $$url ..."; \
	wget --continue --progress=dot:mega -O "$(DEV_ZIM_DEST).tmp" "$$url" && \
	mv "$(DEV_ZIM_DEST).tmp" "$(DEV_ZIM_DEST)"

dev: $(DEV_ZIM_DEST) $(DEV_CONFIG)
	$(PYTHON) -m pipeline.cli --config $(DEV_CONFIG) run-all

dev-run: $(DEV_CONFIG)
	$(PYTHON) -m pipeline.cli --config $(DEV_CONFIG) run-all

clean-dev:
	rm -rf $(DEV_BUILD_DIR)
	rm -f  $(DEV_ZIM_DEST) $(DEV_ZIM_DEST).tmp $(DEV_CONFIG)

# ------------------------------------------------------------------
# Docker
# ------------------------------------------------------------------
docker-build:
	docker build -t monolith-wiki:latest .

docker-run:
	docker compose run --rm pipeline run-all
