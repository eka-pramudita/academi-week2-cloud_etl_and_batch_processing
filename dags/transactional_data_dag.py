from google.cloud import bigquery
from google.oauth2 import service_account
credentials = service_account.Credentials.from_service_account_file('pkl-playing-fields-7314d23dc2d0.json')

project_id = 'pkl-playing-fields'
client = bigquery.Client(credentials= credentials,project=project_id)


def gcp2df(sql):
    query = client.query(sql)
    results = query.result()
    return results.to_dataframe()

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