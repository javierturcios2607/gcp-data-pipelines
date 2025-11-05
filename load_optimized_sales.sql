-- Paso 1: Borrar explícitamente la partición de destino para asegurar la idempotencia.
-- Si la partición no existe, este comando simplemente no hace nada, no da error.
DELETE FROM `{project_id}.{dataset_id}.sales_optimized`
WHERE sale_date = @execution_date;

-- Paso 2: Insertar los nuevos datos limpios en la tabla existente.
INSERT INTO `{project_id}.{dataset_id}.sales_optimized` (
    order_id,
    product_id,
    amount,
    timestamp,
    sale_date
)
SELECT
    order_id,
    product_id,
    amount,
    timestamp,
    sale_date
FROM
    `{project_id}.{dataset_id}.sales_raw_external`
WHERE
    sale_date = @execution_date;