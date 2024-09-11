from google.cloud import bigquery

def load_parquet_from_gcs_to_bigquery(project_id, dataset_id, table_id, gcs_uri):
    client = bigquery.Client()

    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)
  
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        autodetect=True  # Automatically detect schema from the Parquet files
    )

    # Load Parquet file(s) from GCS into the BigQuery table
    load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)

    load_job.result()  # Wait for the job to complete

    print(f"Loaded data from {gcs_uri} into {table_id}")

# Usage example
project_id = "superops-poc"
dataset_id = "shyamDataset"
table_id = "bitcoin-copy"
gcs_uri = "gs://shyam-big-query/parquet/bitcoin-*.parquet"  # GCS path to your Parquet files

load_parquet_from_gcs_to_bigquery(project_id, dataset_id, table_id, gcs_uri)