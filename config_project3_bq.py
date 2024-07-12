#!python config

#connection id
db_source_conn_id = 'db_source_conn'
dwh_conn_id = 'dwh_bigquery_conn'

# db_source_to_target_mapping = {source_table_name : target_table_name}
db_source_to_target_mapping = {
    "public.categories": "dim_categories",
    "public.shippers": "dim_shippers",
    "public.customers": "dim_customers",
    "public.regions": "dim_regions",
    "public.suppliers": "dim_suppliers",
    "public.employees": "dim_employees",
    "public.products": "dim_products",
    "public.territories": "dim_territories",
    "public.employee_territories": "dim_employee_territories",
    "public.orders": "fact_orders",
    "public.order_details": "fact_order_details"
}

#dwh spesification
project_id = 'project3-digital-skola'
dataset_id = 'dwh_project3'

#dwh_fields = {table_name : fields}
dwh_fields = {
    "dim_categories": [
        {'name': 'categoryID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'categoryName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'description', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'picture', 'type': 'STRING', 'mode': 'REQUIRED'}
    ],
    "dim_shippers": [
        {'name': 'shipperID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'companyName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'phone', 'type': 'STRING', 'mode': 'REQUIRED'}
    ],
    "dim_customers": [
        {'name': 'customerID', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'companyName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'contactName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'contactTitle', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'address', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'city', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'region', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'postalCode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'phone', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'fax', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    "dim_regions": [
        {'name': 'regionID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'regionDescription', 'type': 'STRING', 'mode': 'REQUIRED'}
    ],
    "dim_suppliers": [
        {'name': 'supplierID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'companyName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'contactName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'contactTitle', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'address', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'city', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'region', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'postalCode', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'country', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'phone', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'fax', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'homePage', 'type': 'STRING', 'mode': 'NULLABLE'}
    ],
    "dim_employees": [
        {'name': 'employeeID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'lastName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'firstName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'title', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'titleOfCourtesy', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'birthDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'hireDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'address', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'city', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'region', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'postalCode', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'country', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'homePhone', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'extension', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'photo', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'notes', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'reportsTo', 'type': 'INT64', 'mode': 'NULLABLE'},
        {'name': 'photoPath', 'type': 'STRING', 'mode': 'REQUIRED'}
    ],
    "dim_products": [
        {'name': 'productID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'productName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'supplierID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'categoryID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'quantityPerUnit', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'unitPrice', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
        {'name': 'unitsInStock', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'unitsOnOrder', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'reorderLevel', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'discontinued', 'type': 'INT64', 'mode': 'REQUIRED'}
    ],
    "dim_territories": [
        {'name': 'territoryID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'territoryDescription', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'regionID', 'type': 'INT64', 'mode': 'REQUIRED'}
    ],
    "dim_employee_territories": [
        {'name': 'employeeID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'territoryID', 'type': 'INT64', 'mode': 'REQUIRED'}
    ],
    "fact_orders": [
        {'name': 'orderID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'customerID', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'employeeID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'orderDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'requiredDate', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'shippedDate', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'shipVia', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'freight', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
        {'name': 'shipName', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'shipAddress', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'shipCity', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'shipRegion', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'shipPostalCode', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'shipCountry', 'type': 'STRING', 'mode': 'REQUIRED'}
    ],
    "fact_order_details": [
        {'name': 'orderID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'productID', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'unitPrice', 'type': 'FLOAT64', 'mode': 'REQUIRED'},
        {'name': 'quantity', 'type': 'INT64', 'mode': 'REQUIRED'},
        {'name': 'discount', 'type': 'FLOAT64', 'mode': 'REQUIRED'}
    ]
}
 
#data_marts = {table_name : dml_statements}
data_marts = {
    "data_mart_suppliers_monthly_gross_rev": """
        CREATE TABLE IF NOT EXISTS `project3-digital-skola.dwh_project3.data_mart_suppliers_monthly_gross_rev` AS
        SELECT
            DATE_TRUNC(PARSE_DATE('%Y-%m-%d', o.orderDate), MONTH) AS month,
            s.companyName,
            SUM((od.unitPrice - (od.unitPrice * od.discount)) * od.quantity) AS grossRevenue
        FROM `project3-digital-skola.dwh_project3.fact_order_details` od
        LEFT JOIN `project3-digital-skola.dwh_project3.fact_orders` o ON od.orderID = o.orderID
        LEFT JOIN `project3-digital-skola.dwh_project3.dim_products` pd ON od.productID = pd.productID
        LEFT JOIN `project3-digital-skola.dwh_project3.dim_suppliers` s ON pd.supplierID = s.supplierID
        GROUP BY month, s.companyName
    """,
    "data_mart_monthly_top_product_category": """
        CREATE TABLE IF NOT EXISTS `project3-digital-skola.dwh_project3.data_mart_monthly_top_product_category` AS
        WITH a AS (
            SELECT
                DATE_TRUNC(PARSE_DATE('%Y-%m-%d', o.orderDate), MONTH) AS month,
                ca.categoryName AS topProductCategory,
                SUM(od.quantity) AS totalSold
            FROM `project3-digital-skola.dwh_project3.fact_order_details` od
            LEFT JOIN `project3-digital-skola.dwh_project3.fact_orders` o ON od.orderID = o.orderID
            LEFT JOIN `project3-digital-skola.dwh_project3.dim_products` pd ON od.productID = pd.productID
            LEFT JOIN `project3-digital-skola.dwh_project3.dim_categories` ca ON pd.categoryID = ca.categoryID
            GROUP BY month, topProductCategory
        ),
        b AS (
            SELECT
              month,
              topProductCategory,
              totalSold,
              DENSE_RANK() OVER (
                    PARTITION BY month
                    ORDER BY totalSold DESC
                ) AS rank
            FROM a
        )
            SELECT
                month,
                topProductCategory,
                totalSold
            FROM b
            WHERE rank = 1
            ORDER BY month

    """,
    "data_mart_monthly_best_employee": """
        CREATE TABLE IF NOT EXISTS `project3-digital-skola.dwh_project3.data_mart_monthly_best_employee` AS
        WITH a AS (
            SELECT
                DATE_TRUNC(PARSE_DATE('%Y-%m-%d', o.orderDate), MONTH) AS month,
                CONCAT(e.firstName, ' ', e.lastName) AS bestEmployee,
                SUM((od.unitPrice - (od.unitPrice * od.discount)) * od.quantity) AS grossRevenue
            FROM `project3-digital-skola.dwh_project3.fact_order_details` od
            LEFT JOIN `project3-digital-skola.dwh_project3.fact_orders` o ON od.orderID = o.orderID
            LEFT JOIN `project3-digital-skola.dwh_project3.dim_employees` e ON o.employeeID = e.employeeID
            GROUP BY month, bestEmployee
        ),
        b AS(
            SELECT
                month,
                bestEmployee,
                grossRevenue,
                DENSE_RANK() OVER (
                    PARTITION BY month 
                    ORDER BY grossRevenue DESC
                ) AS rank
            FROM a
        )
            SELECT
                month,
                bestEmployee,
                grossRevenue
            FROM b
            WHERE rank = 1
            ORDER BY month
    """
}