from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator
)
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import pandas as pd
import pandas_gbq
import json
from google.oauth2 import service_account
import os
from config_project3_bq import (
    db_source_conn_id,
    dwh_conn_id,
    db_source_to_target_mapping,
    project_id,
    dataset_id,
    dwh_fields,
    data_marts
)

# Define default_args for the DAG
default_args = {
    'owner': 'Ahmad Faishal Akbar',
    'retries': 1,
}

# Function to extract data from PostgreSQL
def extract_data_from_postgres(source_table, target_table):
    postgres_hook = PostgresHook(postgres_conn_id=db_source_conn_id)
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(f"SELECT DISTINCT * FROM {source_table}")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    cursor.close()
    connection.close()

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=columns)
    # Save dataframe to temporary CSV file in /tmp
    df.to_csv(f'/tmp/{target_table}.csv', index=False)

# Function to load data into BigQuery
def load_data_to_bigquery(target_table):
    # Get connection from Airflow
    connection = BaseHook.get_connection(dwh_conn_id)
    keyfile_dict = json.loads(connection.extra_dejson.get('keyfile_dict'))

    # Create credentials from keyfile_dict
    credentials = service_account.Credentials.from_service_account_info(keyfile_dict)

    df = pd.read_csv(f'/tmp/{target_table}.csv')
    table_id = f'{project_id}.{dataset_id}.{target_table}'  # Specify full table name in BigQuery

    # Use credentials to upload data to BigQuery
    pandas_gbq.to_gbq(df, table_id, project_id=project_id, if_exists='replace', credentials=credentials)

    os.remove(f'/tmp/{target_table}.csv')  # Delete file after done

# Initialize the DAG
with DAG(
    dag_id='elt_project3_bigquery_dag',
    default_args=default_args,
    schedule_interval='0 17 * * *',  # Run every day at 5 PM UTC (which is midnight in WIB)
    start_date=days_ago(1),
    catchup=False,
) as dag:

    previous_tasks = {}

    # Loop over the tables and create the necessary tasks
    for source_table, target_table in db_source_to_target_mapping.items():
        create_bigquery_table = BigQueryCreateEmptyTableOperator(
            task_id=f'create_{target_table}',
            project_id=project_id,
            dataset_id=dataset_id,
            table_id=target_table,
            schema_fields=dwh_fields[target_table],
            gcp_conn_id=dwh_conn_id,
        )

        extract_data = PythonOperator(
            task_id=f'extract_data_from_{source_table}',
            python_callable=extract_data_from_postgres,
            op_args=[source_table, target_table],
        )

        load_data = PythonOperator(
            task_id=f'load_data_to_{target_table}',
            python_callable=load_data_to_bigquery,
            op_args=[target_table],
        )

        if target_table in ['dim_products', 'dim_territories', 'dim_employee_territories', 'fact_orders', 'fact_order_details']:
            dependencies = {
                'dim_products': ['dim_suppliers', 'dim_categories'],
                'dim_territories': ['dim_regions'],
                'dim_employee_territories': ['dim_employees', 'dim_territories'],
                'fact_orders': ['dim_customers', 'dim_employees', 'dim_shippers'],
                'fact_order_details': ['dim_products', 'fact_orders'],
            }
            for dep_table in dependencies[target_table]:
                if dep_table in previous_tasks:
                    previous_tasks[dep_table] >> create_bigquery_table

        create_bigquery_table >> extract_data >> load_data
        previous_tasks[target_table] = load_data

    # Linking datamart creation tasks to their dependencies
    datamart_dependencies = {
        'data_mart_suppliers_monthly_gross_rev': ['fact_order_details', 'fact_orders', 'dim_products', 'dim_suppliers'],
        'data_mart_monthly_top_product_category': ['fact_order_details', 'fact_orders', 'dim_products', 'dim_categories'],
        'data_mart_monthly_best_employee': ['fact_order_details', 'fact_orders', 'dim_employees'],
    }

    for table_name, dml_statements in data_marts.items():
        create_datamart = BigQueryInsertJobOperator(
            task_id=f'create_{table_name}',
            configuration={
                "query": {
                    "query": dml_statements,
                    "useLegacySql": False,
                }
            },
            gcp_conn_id=dwh_conn_id,
        )

        for dep_table in datamart_dependencies.get(table_name, []):
            if dep_table in previous_tasks:
                previous_tasks[dep_table] >> create_datamart

        previous_tasks[table_name] = create_datamart
