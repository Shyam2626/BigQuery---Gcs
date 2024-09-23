import os
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, timedelta
import json

def backup_and_cleanup(request):
    
    request_json = request.get_json(silent=True)
    if request_json is None:
        return "Invalid request", 400

    datasets = request_json.get("datasets")
    if datasets is None:
        return "No datasets provided", 400

    bq_client = bigquery.Client()
    gcs_client = storage.Client()
    
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    for dataset in datasets:
        dataset_name = dataset.get("dataset_name")
        retention_days = dataset.get("retention_days")
        backup_type = dataset.get("backup_type")
        bucket_name = dataset.get("bucket_name")

        if not bucket_name:
            return f"No bucket name provided for dataset {dataset_name}", 400

        bucket = gcs_client.bucket(bucket_name)

        if backup_type == "incremental":
            retention_date = today - timedelta(days=retention_days)
            
            cleanup_old_backups(bucket, dataset_name, retention_date)

            tables = bq_client.list_tables(dataset_name)
            for table in tables:
                extract_yesterdays_data(bq_client, dataset_name, table.table_id, bucket, yesterday)

        elif backup_type == "full":
            tables = bq_client.list_tables(dataset_name)
            for table in tables:
                extract_and_upload_to_gcs(bq_client, dataset_name, table.table_id, bucket, today, "full")
    
    return "Backup completed", 200

def extract_yesterdays_data(bq_client, dataset_name, table_id, bucket, yesterday):
    query = f"""
        SELECT *
        FROM `{dataset_name}.{table_id}`
        WHERE today_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """
    query_job = bq_client.query(query)
    
    results = query_job.result()
    
    if results.total_rows > 0:
        df = results.to_dataframe()
        destination_blob_name = f"incremental/{table_id}/{yesterday.strftime('%Y-%m-%d')}/"
        
        df.to_parquet(f"gs://{bucket.name}/{destination_blob_name}data.parquet")
        print(f"Uploaded yesterday's data for {table_id} to gs://{bucket.name}/{destination_blob_name}data.parquet")

def extract_and_upload_to_gcs(bq_client, dataset_name, table_id, bucket, date, backup_type):
    project_id = bq_client.project
    fully_qualified_table_id = f"{project_id}.{dataset_name}.{table_id}"

    destination_blob_name = f"{backup_type}/{table_id}/{date.strftime('%Y-%m-%d')}/"
    
    extract_job = bq_client.extract_table(
        fully_qualified_table_id,
        f"gs://{bucket.name}/{destination_blob_name}*.parquet",
        location="US"
    )
    extract_job.result()
    print(f"Extracted {table_id} to gs://{bucket.name}/{destination_blob_name}")

def cleanup_old_backups(bucket, dataset_name, retention_date):
    blobs = bucket.list_blobs(prefix=dataset_name)
    for blob in blobs:
        if blob.time_created < retention_date:
            print(f"Deleting {blob.name} as it is older than the retention period.")
            blob.delete()

def entry_point(request):
    return backup_and_cleanup(request)


# Payload Example
# {
#   "datasets": [
#     {
#       "dataset_name": "dataset1",
#       "retention_days": 180,
#       "backup_type": "incremental",
#       "bucket_name": "bucket1"
#     },
#     {
#       "dataset_name": "dataset2",
#       "retention_days": 365,
#       "backup_type": "full",
#       "bucket_name": "bucket2"
#     }
#   ]
# }
