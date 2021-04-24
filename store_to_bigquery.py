from google.cloud import bigquery
from google.oauth2 import service_account
import numpy as np
import os
from dotenv import load_dotenv

# Load .env file
load_dotenv()

credentials = service_account.Credentials.from_service_account_file(os.getenv('GOOGLE_SERVICE_CREDENTIALS_PATH'))

project_id = 'pkl-playing-fields'
client_source = bigquery.Client(credentials= credentials,project=project_id)


def gcp2df(sql):
    query = client_source.query(sql)
    results = query.result()
    return results.to_dataframe()


def pull_from_bq():
    sql = """SELECT * FROM pkl-playing-fields.unified_events.event WHERE event_name = 'purchase_item'"""
    event_table = gcp2df(sql)
    event_table['transaction_id'], event_table['transaction_detail_id'], event_table['transaction_number'] = '', '', ''
    event_table['purchase_quantity'], event_table['purchase_amount'], event_table['purchase_payment_method'] = '', '', ''
    event_table['purchase_source'], event_table['product_id'] = '', ''
    for i in range(len(event_table)):
        try:
            if len(event_table['event_params'][i]) == 21:
                event_table['transaction_id'][i] = event_table['event_params'][i][0]
                event_table['transaction_detail_id'][i] = event_table['event_params'][i][1]
                event_table['transaction_number'][i] = event_table['event_params'][i][2]
                event_table['purchase_quantity'][i] = event_table['event_params'][i][3]
                event_table['purchase_amount'][i] = event_table['event_params'][i][4]
                event_table['purchase_payment_method'][i] = event_table['event_params'][i][5]
                event_table['purchase_source'][i] = event_table['event_params'][i][6]
                event_table['product_id'][i] = event_table['event_params'][i][7]
            else:
                event_table['transaction_id'][i] = np.NaN
                event_table['transaction_detail_id'][i] = np.NaN
                event_table['transaction_number'][i] = event_table['event_params'][i][0]
                event_table['purchase_quantity'][i] = np.NaN
                event_table['purchase_amount'][i] = np.NaN
                event_table['purchase_payment_method'][i] = np.NaN
                event_table['purchase_source'][i] = np.NaN
                event_table['product_id'][i] = event_table['event_params'][i][1]
        except ValueError:
            pass
    event_table = event_table.drop('event_params', axis=1)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
    table_id = "academi-cloud-etl.transactions.raw"
    credentials = service_account.Credentials.from_service_account_file(os.getenv('GOOGLE_SERVICE_CREDENTIALS_OWN_PATH'))
    project_id = 'academi-cloud-etl'
    client_target = bigquery.Client(credentials=credentials, project=project_id)
    client_target.load_table_from_dataframe(event_table, table_id, job_config=job_config)


if __name__ == "__main__":
    pull_from_bq()