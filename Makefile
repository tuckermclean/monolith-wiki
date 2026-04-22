# ---------------------------------------------------------------------------
# Monolith Wiki Slice — Build Orchestration
# ---------------------------------------------------------------------------
# Usage:
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

PYTHON      ?= python
CONFIG      ?= config/default.yaml
BUILD_DIR   ?= data/build
SENTINEL    := $(BUILD_DIR)/.sentinels
ZIM_FILE    ?= $(shell $(PYTHON) -c \
    "import yaml; c=yaml.safe_load(open('$(CONFIG)')); print(c['input']['zim_path'])" 2>/dev/null || echo "UNKNOWN")

.PHONY: all stage0 stage1 stage2 stage3 stage4 stage5 stage6 stage7 stage8 \
        validate test clean clean-stage docker-build docker-run

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
# Docker
# ------------------------------------------------------------------
docker-build:
	docker build -t monolith-wiki:latest .

docker-run:
	docker compose run --rm pipeline run-all
