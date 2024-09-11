from google.cloud import bigquery

client = bigquery.Client()

project_id = 'superops-poc'
dataset_id = 'shyamDataset'
table_id = 'time'

full_table_id = f"{project_id}.{dataset_id}.{table_id}"

query = f"SELECT * FROM `{full_table_id}`"

query_job = client.query(query)

results = query_job.result()

print("Contents of the table:")
for row in results:
    print(dict(row))
