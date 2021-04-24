from datetime import datetime, timedelta

from airflow import models
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")

sql_store = """ SELECT  transaction_id.value.int_value `transaction_id`,
                        transaction_detail_id.value.int_value `transaction_detail_id`,
                        transaction_number.value.string_value `transaction_number`,
                        event_datetime `transaction_datetime`,
                        purchase_quantity.value.int_value `purchase_quantity`,
                        purchase_amount.value.float_value `purchase_amount`,
                        purchase_payment_method.value.string_value `purchase_payment_method`,
                        purchase_source.value.string_value `purchase_source`,
                        product_id.value.int_value `product_id`,
                        user_id, state, city, created_at, current_datetime `ext_created_at`
                FROM    `academi-cloud-etl.transactions.raw`
                WHERE   DATE(event_datetime) BETWEEN '{{ ds }}' AND DATE_ADD('{{ ds }}', INTERVAL 2 DAY)"""

default_args = {
    "start_date": datetime(2021,3,21),
    "end_date": datetime(2021,3,27),
    "depends_on_past": True,
    "dataflow_default_options": {
        "project": project_id,
        "temp_location": bucket_path + "/tmp/",
        "numWorkers": 1,
    },
}

with models.DAG(
    # The id you will see in the DAG airflow page
    "transactions_table_dag",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval=timedelta(days=3),  # Override to match your needs
) as dag:

    store = BigQueryOperator(
        task_id='storing_final_table',
        sql=sql_store,
        write_disposition='WRITE_APPEND',
        destination_dataset_table=project_id + ":transactions.transactions_table",
        use_legacy_sql=False,
    )

    store