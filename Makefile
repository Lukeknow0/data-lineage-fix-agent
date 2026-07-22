.PHONY: bootstrap start demo demo-lineagetx \
	demo-lineagetx-abort verify \
	seed-datahub seed-lineagetx gate-live gate-lineagetx-live clean

bootstrap:
	./scripts/bootstrap.sh

start: demo

demo:
	.venv/bin/datalineage-fix run --mode fixture

demo-lineagetx:
	.venv/bin/datalineage-fix lineagetx replay --project-root "$(CURDIR)" --work-root "$(CURDIR)/artifacts/runs/lineagetx-local-replay" --reset

demo-lineagetx-abort:
	.venv/bin/datalineage-fix lineagetx replay --project-root "$(CURDIR)" --work-root "$(CURDIR)/artifacts/runs/lineagetx-local-replay-abort" --reset --outcome abort

verify:
	.venv/bin/python -m pytest

seed-datahub:
	.venv/bin/python scripts/seed_datahub.py

seed-lineagetx:
	.venv/bin/python scripts/seed_lineagetx_datahub.py

gate-live:
	./scripts/gate_live.sh

gate-lineagetx-live:
	./scripts/gate_lineagetx_live.sh

clean:
	rm -rf fixture_pipeline/workspace .pytest_cache
