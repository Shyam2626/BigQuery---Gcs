from google.cloud import bigquery, storage

def get_gcs_subdirectories(bucket_name, prefix):
    storage_client = storage.Client()
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix, delimiter='/')
    return blobs.prefixes

def load_parquet_from_gcs_to_bigquery(project_id, dataset_id, gcs_bucket, source_base_path):
    client = bigquery.Client()

    # Automatically get table names from GCS
    table_folders = get_gcs_subdirectories(gcs_bucket, source_base_path)

    for table_folder in table_folders:
        table_id = table_folder.strip('/').split('/')[-1]  # Extract the table name from folder path
        gcs_path = f"gs://{gcs_bucket}/{table_folder}/*.parquet"
        dataset_ref = client.dataset(dataset_id, project=project_id)
        table_ref = dataset_ref.table(table_id)

        # Configure the load job for Parquet files
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            autodetect=True
        )

        try:
            load_job = client.load_table_from_uri(gcs_path, table_ref, job_config=job_config)
            load_job.result()  # Wait for the job to complete
            print(f"Loaded data from {gcs_path} into {table_id}")
        except Exception as e:
            print(f"Error loading data from {gcs_path} into {table_id}: {e}")

# Usage example
project_id = "superops-poc"
dataset_id = "shyamDataset"
gcs_bucket = "shyam-big-query"
source_base_path = "exports/shyamDataset"

load_parquet_from_gcs_to_bigquery(project_id, dataset_id, gcs_bucket, source_base_path)
