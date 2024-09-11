from google.cloud import bigquery

def load_local_csv_to_bigquery(project_id, dataset_id, table_id, csv_file_path):
    client = bigquery.Client()

    # Define the destination table
    table_ref = client.dataset(dataset_id).table(table_id)

    # Define the schema for the table
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip the header row if you have one
        schema=[
            bigquery.SchemaField("job_link", "STRING"),
            bigquery.SchemaField("job_summary", "STRING"),
        ],
    )

    # Load data from local CSV file into BigQuery
    with open(csv_file_path, "rb") as source_file:
        load_job = client.load_table_from_file(source_file, table_ref, job_config=job_config)

    # Wait for the job to complete
    load_job.result()

    print(f"Loaded {load_job.output_rows} rows from {csv_file_path} into {table_id}")

# Usage example
project_id = "superops-poc"
dataset_id = "shyamDataset"
table_id = "job"
csv_file_path = "/home/shyam/Downloads/archive/job_summary.csv"  # Path to the CSV file on your local system

load_local_csv_to_bigquery(project_id, dataset_id, table_id, csv_file_path)
