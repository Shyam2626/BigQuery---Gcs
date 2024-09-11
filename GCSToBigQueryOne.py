from google.cloud import bigquery

def load_csv_from_gcs_to_bigquery(project_id, dataset_id, table_id, gcs_path):
    client = bigquery.Client()

    # Define table details
    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)

    # Define the schema for the BigQuery table
    schema = [
        bigquery.SchemaField("opentime", "TIMESTAMP"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "FLOAT"),
        bigquery.SchemaField("closetime", "TIMESTAMP"),
        bigquery.SchemaField("assetvolume", "FLOAT"),
        bigquery.SchemaField("trades", "INTEGER"),
        bigquery.SchemaField("baseassetvolume", "FLOAT"),
        bigquery.SchemaField("quotaassestvolume", "FLOAT"),
        bigquery.SchemaField("ignore", "INTEGER"),
    ]

    # Configure the load job for CSV files
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip the header row if your CSV has one
        autodetect=False,  # We are using a predefined schema, so no need for autodetect
    )

    # Load CSV file(s) from GCS into the BigQuery table
    load_job = client.load_table_from_uri(gcs_path, table_ref, job_config=job_config)

    # Wait for the job to complete
    load_job.result()

    print(f"Loaded data from {gcs_path} into {table_id}")

# Usage example
project_id = "superops-poc"
dataset_id = "shyamDataset"
table_id = "bitcoin"
gcs_path = "gs://shyam-big-query/bitcoin.csv"

load_csv_from_gcs_to_bigquery(project_id, dataset_id, table_id, gcs_path)
