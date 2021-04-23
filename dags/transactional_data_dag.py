from google.cloud import bigquery
from google.oauth2 import service_account
from airflow import models
import pandas as pd

bucket_path = models.Variable.get("bucket_path")

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
    job = client.load_table_from_dataframe(result_table, destination='academi-cloud-etl.transactional.transaction_table')




tables = client.list_tables('pkl-playing-fields:unified_events')

print("Tables contained in '{}':".format('pkl-playing-fields:unified_events'))
for table in tables:
    print("{}.{}.{}".format(table.project, table.dataset_id, table.table_id))

# test query to table
sql = """
    SELECT * FROM pkl-playing-fields.unified_events.event WHERE event_name = 'purchase_item'
"""

event_table = gcp2df(sql)

pd = gcp2df(sql)

print(pd.head())

