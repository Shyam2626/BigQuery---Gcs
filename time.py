import time
from google.cloud import bigquery
from datetime import datetime

client = bigquery.Client()

project_id = 'superops-poc'
dataset_id = 'shyamDataset'
table_id = 'times'

counter = 1

try:
    while True:
        current_timestamp = datetime.utcnow().isoformat() + 'Z'

        row_to_insert = [
            {"currtime": current_timestamp, "counter": counter, "samplestring": f"Sample {counter}"},
        ]

        errors = client.insert_rows_json(f"{project_id}.{dataset_id}.{table_id}", row_to_insert)

        if errors:
            print("Encountered errors while inserting rows: {}".format(errors))
        else:
            print(f"Inserted row with counter {counter} at timestamp {current_timestamp}")

        counter += 1

        time.sleep(10)

except KeyboardInterrupt:
    print("Stopped inserting rows.")
