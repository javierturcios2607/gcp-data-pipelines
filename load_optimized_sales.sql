CREATE OR REPLACE TABLE `{project_id}.{dataset_id}.sales_optimized`
PARTITION BY sale_date
CLUSTER BY product_id
AS
SELECT
  *
FROM
  `{project_id}.{dataset_id}.sales_raw_external`
WHERE
  sale_date = @execution_date;