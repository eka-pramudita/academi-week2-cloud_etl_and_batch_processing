"""Example Airflow DAG that creates a Cloud Dataflow workflow which takes a
text file and adds the rows to a BigQuery table.

This DAG relies on four Airflow variables
https://airflow.apache.org/concepts.html#variables
* project_id - Google Cloud Project ID to use for the Cloud Dataflow cluster.
* gce_zone - Google Compute Engine zone where Cloud Dataflow cluster should be
  created.
* gce_region - Google Compute Engine region where Cloud Dataflow cluster should be
  created.
Learn more about the difference between the two here:
https://cloud.google.com/compute/docs/regions-zones
* bucket_path - Google Cloud Storage bucket where you've stored the User Defined
Function (.js), the input file (.txt), and the JSON schema (.json).
"""

import datetime

from airflow import models
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")


default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": datetime.datetime(2021,3,10),
    "end_date": datetime.datetime(2021,3,15),
    'email': ['ekaapramudita7@gmail.com'],
    'email_on_failure': True,
    "dataflow_default_options": {
        "project": project_id,
        # Set to your region
        "region": gce_region,
        # Set to your zone
        "zone": gce_zone,
        # This is a subfolder for storing temporary files, like the staged pipeline job.
        "temp_location": bucket_path + "/tmp/",
    },
}

# Define a DAG (directed acyclic graph) of tasks.
# Any task you create within the context manager is automatically added to the
# DAG object.
with models.DAG(
    # The id you will see in the DAG airflow page
    "composer_dataflow_dag",
    default_args=default_args,
    # The interval with which to schedule the DAG
    schedule_interval=datetime.timedelta(days=1),  # Override to match your needs
) as dag:

    # store execution date for dataflow to XCom
    def exec_date_dataflow(**kwargs):
        return kwargs['ds_nodash']

    # task instance of exec date for dataflow
    t1 = PythonOperator(
        task_id='get_date_dataflow',
        python_callable=exec_date_dataflow,
        provide_context=True)

    get_date_dataflow = "{{ task_instance.xcom_pull(task_ids='get_date_dataflow') }}"

    dataflow_job = DataflowTemplateOperator(
        # The task id of your job
        task_id="dataflow_operator_transform_csv_to_bq",
        # The name of the template that you're using.
        # Below is a list of all the templates you can use.
        # For versions in non-production environments, use the subfolder 'latest'
        # https://cloud.google.com/dataflow/docs/guides/templates/provided-batch#gcstexttobigquery
        template="gs://dataflow-templates/latest/GCS_Text_to_BigQuery",
        # Use the link above to specify the correct parameters for your template.
        parameters={
            "javascriptTextTransformFunctionName": "transformCSVtoJSON",
            "JSONPath": bucket_path + "/jsonSchema.json",
            "javascriptTextTransformGcsPath": bucket_path + "/transformCSVtoJSON.js",
            "inputFilePattern": bucket_path + "/keyword_search_search_" + get_date_dataflow,
            "outputTable": project_id + ":daily_search_history.daily_search_history",
            "bigQueryLoadingTemporaryDirectory": bucket_path + "/tmp/",
        },
    )
    print("dataflow succeed")

    # store execution date for bigquery to XCom
    def exec_date_bigquery(**kwargs):
        return kwargs['ds']

    # task instance of exec date for bigquery
    t2 = PythonOperator(
        task_id='get_date_bigquery',
        python_callable=exec_date_bigquery,
        provide_context=True)

    get_date_bigquery = "{{ task_instance.xcom_pull(task_ids='get_date_bigquery') }}"

    bigquery_job = BigQueryOperator(
        task_id = 'bigquery_most_searched_keywords',
        sql = f'''
        SELECT  date(created_at) AS `created_date`, search_keyword AS `most_searched_keyword`, search_result_count
        FROM    `{project_id}.daily_search_history.most_searched_each_day`
        WHERE   date(created_at) = {get_date_bigquery}
        ORDER BY    search_result_count
        LIMIT 1
        ''',
        write_disposition = 'WRITE_APPEND',
        destination_dataset_table = project_id + ":daily_search_history.most_searched_each_day",
        use_legacy_sql = False,
    )
    print("bigquery succeed")

    t1 >> dataflow_job >> t2 >> bigquery_job