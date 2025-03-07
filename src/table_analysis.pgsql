/* Buscar as tabelas com colunas de data */
SELECT table_name
FROM information_schema.COLUMNS
WHERE data_type = 'date'or data_type = 'timestamp';

/* Checar se as datas estão no formato YY-MM-DD */
SELECT birth_date, hire_date
FROM employees
LIMIT 5;

SELECT 
    COUNT(*) AS total_rows,
    COUNT(birth_date) AS non_null_birth_date,
    COUNT(hire_date) AS non_null_hire_date
FROM employees;

SELECT order_date, required_date, shipped_date
FROM orders
LIMIT 5;

SELECT
    COUNT(*) AS total_rows,
    COUNT(order_date) AS non_null_order_date,
    COUNT(required_date) AS non_null_required_date,
    COUNT(shipped_date) AS non_null_shipped_date
FROM orders;

SELECT *
FROM orders
WHERE shipped_date IS NULL;

SELECT table_name
FROM information_schema.COLUMNS
WHERE data_type = 'money' or data_type = 'numeric' or data_type = 'decimal'

