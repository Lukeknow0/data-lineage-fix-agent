.PHONY: bootstrap start demo verify seed-datahub gate-live clean

bootstrap:
	./scripts/bootstrap.sh

start: demo

demo:
	.venv/bin/datalineage-fix run --mode fixture

verify:
	.venv/bin/python -m pytest

seed-datahub:
	.venv/bin/python scripts/seed_datahub.py

gate-live:
	./scripts/gate_live.sh

clean:
	rm -rf fixture_pipeline/workspace .pytest_cache
