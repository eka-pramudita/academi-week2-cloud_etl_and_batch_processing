from google.cloud import bigquery
from google.oauth2 import service_account
from airflow import models
import pandas as pd
import datetime

bucket_path = models.Variable.get("bucket_path")
project_id = models.Variable.get("project_id")
gce_zone = models.Variable.get("gce_zone")
gce_region = models.Variable.get("gce_region")

credentials = service_account.Credentials.from_service_account_file(bucket_path + 'pkl-playing-fields-7314d23dc2d0.json')

project_id = 'pkl-playing-fields'
client = bigquery.Client(credentials= credentials,project=project_id)


def gcp2df(sql):
    query = client.query(sql)
    results = query.result()
    return results.to_dataframe()


def pull_from_bq():
    sql = """
        SELECT * FROM pkl-playing-fields.unified_events.event WHERE event_name = 'purchase_item'
    """
    event_table = gcp2df(sql)
    return event_table


def transform_table(event_table, **kwargs):
    print("processing table")
    columns = ['transaction_id', 'transaction_detail_id', 'transaction_number', 'transaction_datetime',
               'purchase_quantity', 'purchase_amount', 'purchase_payment_method', 'purchase_source',
               'product_id', 'user_id', 'state', 'city', 'created_at', 'ext_created_at']
    result_table = pd.DataFrame(columns=columns)
    result_table[['user_id', 'state', 'city']] = event_table[['user_id', 'state', 'city']]
    result_table['transaction_datetime'] = pd.to_datetime(event_table['event_datetime']).dt.date
    result_table['created_at'] = pd.to_datetime(event_table['created_at']).dt.date
    result_table['ext_created_at'] = pd.to_datetime(event_table['created_at']).dt.date

    for i in range(len(result_table)):
        try:
            event_params = pd.json_normalize(event_table['event_params'][i])
            result_table['transaction_id'][i] = int(event_params['value.int_value'][0])
            result_table['transaction_detail_id'][i] = int(event_params['value.int_value'][1])
            result_table['transaction_number'][i] = str(event_params['value.string_value'][2])
            result_table['purchase_quantity'][i] = int(event_params['value.int_value'][3])
            result_table['purchase_amount'][i] = float(event_params['value.float_value'][4])
            result_table['purchase_payment_method'][i] = str(event_params['value.string_value'][5])
            result_table['purchase_source'][i] = str(event_params['value.string_value'][6])
            result_table['product_id'][i] = int(event_params['value.int_value'][7])
        except ValueError:
            pass

    return result_table


def store_table(result_table, **kwargs):
    credentials = service_account.Credentials.from_service_account_file(bucket_path + 'academi-cloud-etl-792cf79c7663.json')
    project_id = 'academi-cloud-etl'
    client = bigquery.Client(credentials=credentials, project=project_id)
    client.load_table_from_dataframe(result_table, destination='academi-cloud-etl.transactional.transaction_table')


default_args = {
    # Tell airflow to start one day ago, so that it runs as soon as you upload it
    "start_date": datetime.datetime(2021,3,21),
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
    schedule_interval=datetime.timedelta(days=3),  # Override to match your needs
) as dag:
