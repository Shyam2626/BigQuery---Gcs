from google.cloud import bigquery

def upload_csv_to_bigquery(dataset_id, table_id, csv_file_path, project_id):
    client = bigquery.Client()

    # Set the dataset and table details
    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)

    # Define table creation if it doesn't exist
    try:
        client.get_table(table_ref)  # Check if the table exists
        print(f"Table {table_id} already exists.")
    except Exception:
        # Table doesn't exist, create a new table
        print(f"Table {table_id} does not exist. Creating table...")
        schema = None  # We are using autodetect
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)  # Create an empty table
        print(f"Table {table_id} created.")

    # Configure the job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,  # Set format as CSV
        skip_leading_rows=1,  # Skip header row
        autodetect=True,  # Automatically infer schema
    )

    # Read CSV and load it into BigQuery
    with open(csv_file_path, "rb") as file:
        job = client.load_table_from_file(file, table_ref, job_config=job_config)

    job.result()  # Wait for the job to complete

    print(f"Loaded {job.output_rows} rows into {dataset_id}:{table_id}.")

# Usage
dataset_id = "shyamDataset"
table_id = "time"
csv_file_path = "/home/shyam/Downloads/output.csv"
project_id = "superops-poc"

upload_csv_to_bigquery(dataset_id, table_id, csv_file_path, project_id)
