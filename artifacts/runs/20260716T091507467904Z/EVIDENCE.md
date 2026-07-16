# Evidence — 20260716T091507467904Z

Status: **verified-fixed**

## DataHub grounding

- Context transport: `datahub-oss-lite-v1.6.0+official-mcp-server-v0.6.0`
- Source entity: `urn:li:dataset:(urn:li:dataPlatform:postgres,ecommerce.raw.orders,PROD)`
- Affected downstream entity: `urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.mart_customer_revenue,PROD)`
- Owner(s): `urn:li:corpuser:data-platform-oncall`
- Quality/governance signals: `quality_signal=breaking-schema-drift`, `schema_contract=v2`, `urn:li:tag:SchemaDriftDetected`
- DataHub tool trace: `get_entities, list_schema_fields, get_lineage`

## Finding

`downstream-schema-drift` in `pipeline/customer_revenue.sql`: the code referenced `customer_id`, which is absent from the current DataHub schema. DataHub explicitly identifies `customer_key` as its replacement and lineage identifies the mapped asset as downstream.

## Minimal patch

```diff
--- a/pipeline/customer_revenue.sql
+++ b/pipeline/customer_revenue.sql
@@ -1,6 +1,6 @@
 SELECT
-  customer_id,
+  customer_key,
   SUM(total_amount) AS lifetime_value
 FROM orders
-GROUP BY customer_id
-ORDER BY customer_id;
+GROUP BY customer_key
+ORDER BY customer_key;
```

## Regression: red before patch

```text
test_customer_revenue_query_matches_datahub_schema (test_datahub_schema_contract.DataHubSchemaContractRegression.test_customer_revenue_query_matches_datahub_schema) ... ERROR

======================================================================
ERROR: test_customer_revenue_query_matches_datahub_schema (test_datahub_schema_contract.DataHubSchemaContractRegression.test_customer_revenue_query_matches_datahub_schema)
----------------------------------------------------------------------
Traceback (most recent call last):
  File "$PROJECT_ROOT/fixture_pipeline/workspace/tests/test_datahub_schema_contract.py", line 26, in test_customer_revenue_query_matches_datahub_schema
    rows = connection.execute(query).fetchall()
           ^^^^^^^^^^^^^^^^^^^^^^^^^
sqlite3.OperationalError: no such column: customer_id

----------------------------------------------------------------------
Ran 1 test in 0.000s

FAILED (errors=1)
```

## Regression: green after patch

```text
test_customer_revenue_query_matches_datahub_schema (test_datahub_schema_contract.DataHubSchemaContractRegression.test_customer_revenue_query_matches_datahub_schema) ... ok

----------------------------------------------------------------------
Ran 1 test in 0.000s

OK
```

## DataHub write-back

- Status: `written-and-read-back-via-datahub-mcp`
- Entity: `urn:li:dataset:(urn:li:dataPlatform:postgres,analytics.mart_customer_revenue,PROD)`
- Tag/status: `urn:li:tag:DataLineageFixVerified`
- Verification: `get_entities returned the verified tag after add_tags`

The sibling JSON/text/diff files and `manifest.json` form the machine-verifiable evidence bundle.
