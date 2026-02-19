VENV := $(HOME)/.virtualenvs/orchestrator
PY   := $(VENV)/bin/python
PIP  := $(VENV)/bin/pip

install:
	$(PIP) install -e ".[dev]"

test:
	$(VENV)/bin/pytest tests/ -v

lint:
	$(PY) -m py_compile \
		orchestrator/cli.py \
		orchestrator/pipeline.py \
		orchestrator/registry.py \
		orchestrator/validator.py \
		orchestrator/utils/hashing.py \
		orchestrator/stages/stage1_generate_script.py \
		orchestrator/stages/stage2_script_to_shotlist.py \
		orchestrator/stages/stage3_shotlist_to_assetmanifest.py \
		orchestrator/stages/stage4_build_renderplan.py \
		orchestrator/stages/stage5_render_preview.py

run-example:
	$(VENV)/bin/orchestrator run --project examples/phase0/project.json

clean:
	rm -rf artifacts/ __pycache__ .pytest_cache *.egg-info

.PHONY: install test lint run-example clean
