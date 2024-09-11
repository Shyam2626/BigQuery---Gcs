from google.cloud import bigquery

def export_bigquery_to_gcs_parquet(project_id, dataset_id, table_id, gcs_uri):
    client = bigquery.Client()

    # Define the table details
    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)

    # Configure the extract job for Parquet format
    job_config = bigquery.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.PARQUET
    )

    # Create and start the extract job
    extract_job = client.extract_table(
        table_ref,
        gcs_uri,  # Use * to shard the output files, e.g., gs://your-bucket/backup-folder/your-table-*.parquet
        job_config=job_config
    )

    extract_job.result()  # Wait for the job to finish
    print(f"Table {table_id} exported to {gcs_uri} in Parquet format")

# Usage example
project_id = "superops-poc"
dataset_id = "shyamDataset"
table_id = "bitcoin"
gcs_uri = "gs://shyam-big-query/parquet/bitcoin-*.parquet"  # Use * to shard the output

export_bigquery_to_gcs_parquet(project_id, dataset_id, table_id, gcs_uri)
