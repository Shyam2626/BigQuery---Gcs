from google.cloud import bigquery

client = bigquery.Client()

project_id = 'superops-poc'
dataset_id = 'shyamDataset' 
table_id = 'product'

full_table_id = f"{project_id}.{dataset_id}.{table_id}"

rows_to_insert = [
    {"productname": "abc", "productprice": 123},
    {"productname": "xyz", "productprice": 456},
]

errors = client.insert_rows_json(full_table_id, rows_to_insert)

if errors == []:
    print("New rows have been added.")
else:
    print("Encountered errors while inserting rows: {}".format(errors))
