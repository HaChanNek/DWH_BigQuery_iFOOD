import os
from datetime import timedelta
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.bigquery_to_gcs import BigQueryToGCSOperator
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from google.cloud import bigquery
import airflow
from Clustering_Kmeans_Model import train_model
from model_predicted_response import predict_model
from google.cloud import storage

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = '/opt/airflow/dags/datawarehouse-422504-39505bda63f7.json'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'oltp_to_olap_transform',
    default_args=default_args,
    description='Transform OLTP to OLAP schema in BigQuery',
    template_searchpath='/opt/airflow/dags',
    schedule_interval=None,
)

# Function to read SQL query from file
def read_sql_file(file_path):
    with open(file_path, 'r') as file:
        return file.read()

# Read the SQL queries from files
DimCustomer_sql_query = read_sql_file('/opt/airflow/dags/dim_customer.sql')
DimDateTime_sql_query = read_sql_file('/opt/airflow/dags/dim_datetime.sql')
FactOrder_sql_query = read_sql_file('/opt/airflow/dags/fact.sql')
DimCampaignResponse_sql_query = read_sql_file('/opt/airflow/dags/dim_Campaigns_Response.sql')
DimPurchases_sql_query = read_sql_file('/opt/airflow/dags/dim_purchases.sql')
DimProductConsumption_sql_query = read_sql_file('/opt/airflow/dags/dim_product_consumption.sql')
# Define the tasks
t1 = BigQueryInsertJobOperator(
    task_id='create_dim_customer',
    configuration={
        "query": {
            "query": DimCustomer_sql_query,
            "useLegacySql": False
        }
    },
    dag=dag,
)

t2 = BigQueryInsertJobOperator(
    task_id='create_update_dim_Campaign_Response',
    configuration={
        "query": {
            "query": DimCampaignResponse_sql_query,
            "useLegacySql": False
        }
    },
    dag=dag,
)

t3 = BigQueryInsertJobOperator(
    task_id='create_update_dim_datetime',
    configuration={
        "query": {
            "query": DimDateTime_sql_query,
            "useLegacySql": False
        }
    },
    dag=dag,
)

t4 = BigQueryInsertJobOperator(
    task_id='create_update_dim_purchases',
    configuration={
        "query": {
            "query": DimPurchases_sql_query,
            "useLegacySql": False
        }
    },
    dag=dag,
)

t5 = BigQueryInsertJobOperator(
    task_id='create_update_dim_product_consumption',
    configuration={
        "query": {
            "query": DimProductConsumption_sql_query,
            "useLegacySql": False
        }
    },
    dag=dag,
)


t6 = BigQueryInsertJobOperator(
    task_id='create_update_fact_table',
    configuration={
        "query": {
            "query": FactOrder_sql_query,
            "useLegacySql": False
        }
    },
    dag=dag,
)



def model_training():
    train_model()

t7 = PythonOperator(
    task_id='train_model',
    python_callable=model_training,
    dag=dag,
)
def model_predict():
    predict_model()

t8 = PythonOperator(
    task_id='predict_model',
    python_callable=model_predict,
    dag=dag,
)

TaskDelay = BashOperator(task_id="delay_bash_task",
                         dag=dag,
                         bash_command="sleep 5s")

bq_to_gcs_task = BigQueryToGCSOperator(
    task_id='bigquery_to_gcs_task',
    source_project_dataset_table='datawarehouse-422504.OLAP.fact_MarketingCampaignResponse',
    destination_cloud_storage_uris=['gs://datawarehouse12/fact_MarketingCampaignResponse.avro'],
    compression='NONE',
    export_format='AVRO',
    field_delimiter=',',
    print_header=True,
    dag=dag
)



# Define task dependencies
[t1, t2, t3, t4, t5] >> TaskDelay >> t6 >> [t7, t8] >> bq_to_gcs_task

if __name__ == "__main__":
    dag.cli()
