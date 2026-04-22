# ---------------------------------------------------------------------------
# Monolith Wiki — Build Orchestration
# ---------------------------------------------------------------------------
# Usage:
#   make fetch-zim    — download latest Wikipedia no-pictures ZIM from Kiwix
#   make all          — run full pipeline (resume-safe via sentinels)
#   make stage0       — ZIM indexing only
#   make clean        — remove all build artefacts (keeps raw ZIM input)
#   make clean-stage  STAGE=s3  — rerun from a specific stage onward
#   make test         — run pytest suite
#   make docker-build — build Docker image
#   make docker-run   — run full pipeline inside Docker
#
# Requires: MONOLITH_CONFIG env var or config/default.yaml present.
# ---------------------------------------------------------------------------

PYTHON         ?= python
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

ZIM_FILE    ?= $(shell $(PYTHON) -c \
    "import yaml; c=yaml.safe_load(open('$(CONFIG)')); print(c['input']['zim_path'])" 2>/dev/null || echo "UNKNOWN")

.PHONY: all stage0 stage1 stage2 stage3 stage4 stage5 stage6 stage7 stage8 \
        validate test clean clean-stage fetch-zim docker-build docker-run

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
	wget --continue --progress=dot:giga -O "$(ZIM_DEST)" "$$url"

# ------------------------------------------------------------------
# Docker
# ------------------------------------------------------------------
docker-build:
	docker build -t monolith-wiki:latest .

docker-run:
	docker compose run --rm pipeline run-all
