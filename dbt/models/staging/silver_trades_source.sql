-- Source model: exposes the Silver Parquet written by the Python pipeline
-- (src/transforms/silver_pipeline.py) as a dbt relation. Materialised as a
-- view over read_parquet so dbt always sees the current Silver files.
{{ config(materialized='view') }}
SELECT * FROM read_parquet('../data/silver/**/*.parquet', union_by_name=true)
