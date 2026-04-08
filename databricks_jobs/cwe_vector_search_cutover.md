# CWE Vector Search Cutover (Bronze -> Silver)

## 1) Run Pipeline
Run `databricks_jobs/run_cwe_delta_merge.py` with:

- `target_table=3dt2ndteam5.cwe.cwe_weaknesses`
- `silver_table=3dt2ndteam5.cwe.cwe_weaknesses_silver` (optional; default is `${target_table}_silver`)

This run now does:
1. Bronze merge (`cwe_weaknesses`)
2. Silver sync (`cwe_weaknesses_silver`) with `is_deprecated` and `search_text`
3. CDF enablement on Silver table

## 2) Validate Silver Table
```sql
SELECT COUNT(*) AS bronze_count
FROM 3dt2ndteam5.cwe.cwe_weaknesses;

SELECT COUNT(*) AS silver_count
FROM 3dt2ndteam5.cwe.cwe_weaknesses_silver;

SELECT COUNT(*) AS null_deprecated
FROM 3dt2ndteam5.cwe.cwe_weaknesses_silver
WHERE is_deprecated IS NULL;

SELECT COUNT(*) AS empty_search_text
FROM 3dt2ndteam5.cwe.cwe_weaknesses_silver
WHERE search_text IS NULL OR TRIM(search_text) = '';
```

## 3) Recreate Vector Search Index (Same Name)
In Catalog Explorer -> Vector Search:

1. Delete `cwe-weaknesses-vs-idx`
2. Create `cwe-weaknesses-vs-idx` again with:
   - Source table: `3dt2ndteam5.cwe.cwe_weaknesses_silver`
   - Primary key: `weakness_id`
   - Embedding source column: `search_text`
   - Sync columns: `weakness_id,title,description,extended_description,is_deprecated`

## 4) Query Contract (for retrieval owners)
- Always exclude deprecated records:
  - Standard endpoint filters: `{"is_deprecated": false}`
  - Storage-optimized filters: `is_deprecated = false`
- If query contains `CWE-<number>`, use `weakness_id` equality filter as primary path.
