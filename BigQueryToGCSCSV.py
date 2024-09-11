from google.cloud import bigquery

def export_bigquery_to_gcs(project_id, dataset_id, table_id, gcs_uri):
    client = bigquery.Client()

    # Define the table details
    dataset_ref = client.dataset(dataset_id, project=project_id)
    table_ref = dataset_ref.table(table_id)

    # Configure the extract job
    job_config = bigquery.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.CSV,
        field_delimiter=",",  # Use comma as a delimiter for CSV
        print_header=True     # Include header row
    )

    # Create and start the extract job
    extract_job = client.extract_table(
        table_ref,
        gcs_uri,  # e.g., 'gs://your-bucket/backup-folder/your-table-*.csv' (use * to shard the output)
        job_config=job_config
    )

    extract_job.result()  # Wait for the job to finish
    print(f"Table {table_id} exported to {gcs_uri}")

# Usage example
project_id = "superops-poc"
dataset_id = "shyamDataset"
table_id = "bitcoin"
gcs_uri = "gs://shyam-big-query/backup-folder/bitcoin-*.csv"  # Use * for sharding the output files

export_bigquery_to_gcs(project_id, dataset_id, table_id, gcs_uri)
