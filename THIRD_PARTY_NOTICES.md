# Third-Party and Pre-Existing Work Disclosure

## New work in this repository

All product source, controlled pipeline fixture, tests, evidence packaging, DataHub Lite compatibility bridge, documentation, and submission material in this repository were created during the Build with DataHub submission period.

No source code was copied from the pre-existing local projects named below.

## Pre-existing local inspiration

Pre-existing private utilities informed three architectural preferences: packaging
external context into a durable evidence bundle, using a bounded test/fix/test
loop, and clearly labeling offline demonstration paths. No source code from those
utilities is included in this repository.

Only architectural ideas were reused. The DataHub adapters, schema-drift planner, SQL patch guard, regression generator, write-back, evidence manifest, fixtures, and tests are new implementations.

## Direct runtime dependencies

| Component | Pinned/range | Use | License / source |
|---|---|---|---|
| DataHub Python SDK / CLI (`acryl-datahub`) | `1.6.0.14` | DataHub metadata model, DataHub Lite, seeding | Apache-2.0; https://github.com/datahub-project/datahub |
| Official DataHub MCP Server (`mcp-server-datahub`) | `0.6.0` | Schema, entity, lineage, tag mutation/read-back tools | Apache-2.0; https://github.com/acryldata/mcp-server-datahub |
| Model Context Protocol Python SDK (`mcp`) | `1.28.1` | MCP stdio client | MIT; https://github.com/modelcontextprotocol/python-sdk |
| SQLGlot | `28.10.1` | SQL parsing and exact identifier validation | MIT; https://github.com/tobymao/sqlglot |
| DuckDB | `1.5.4` | DataHub Lite embedded storage | MIT; https://github.com/duckdb/duckdb |
| Starlette | `1.3.1` | Narrow local compatibility-bridge HTTP routes | BSD-3-Clause; https://github.com/Kludex/starlette |
| Uvicorn | `0.51.0` | Local compatibility-bridge ASGI server | BSD-3-Clause; https://github.com/Kludex/uvicorn |

Development-only dependencies are `pytest` and `pytest-cov`, both under MIT-family licenses. Transitive dependencies are installed from their package indexes and are not vendored.

## Data and generated artifacts

- All fixture entities, owners, tags, schemas, SQL, and rows are synthetic.
- `example.invalid` is used for the synthetic owner email.
- The committed offline replay was captured from the local DataHub Lite + official MCP gate and is labeled as a replay. It is not represented as a live call.
- No private production data, credential, token, or API key is included.

## Repository license

Original repository content is licensed under Apache License 2.0. Dependency licenses continue to govern their respective packages.
