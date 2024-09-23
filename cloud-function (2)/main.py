import os
from google.cloud import bigquery
from google.cloud import storage
from datetime import datetime, timedelta
import json

def backup_and_cleanup(request):
    # Parse the request JSON
    request_json = request.get_json(silent=True)
    if request_json is None:
        return "Invalid request", 400

    datasets = request_json.get("datasets")
    if datasets is None:
        return "No datasets provided", 400

    # Initialize BigQuery and GCS clients
    bq_client = bigquery.Client()
    gcs_client = storage.Client()
    
    # Set the bucket name
    bucket_name = "shyam-big-query"  # Replace with your bucket name
    bucket = gcs_client.bucket(bucket_name)

    # Get current date and yesterday's date
    today = datetime.now()
    yesterday = today - timedelta(days=1)

    for dataset in datasets:
        dataset_name = dataset.get("dataset_name")
        retention_days = dataset.get("retention_days")
        backup_type = dataset.get("backup_type")

        if backup_type == "incremental":
            # Incremental Backup: Delete the data older than retention_days and add yesterday's data
            retention_date = today - timedelta(days=retention_days)
            
            # Cleanup old backups
            cleanup_old_backups(bucket, dataset_name, retention_date)

            # Query to extract yesterday's data for each table in the dataset
            tables = bq_client.list_tables(dataset_name)
            for table in tables:
                extract_yesterdays_data(bq_client, dataset_name, table.table_id, bucket, yesterday)

        elif backup_type == "full":
            # Full Backup: Copy all data from the dataset
            tables = bq_client.list_tables(dataset_name)
            for table in tables:
                extract_and_upload_to_gcs(bq_client, dataset_name, table.table_id, bucket, today, "full")
    
    return "Backup completed", 200

def extract_yesterdays_data(bq_client, dataset_name, table_id, bucket, yesterday):
    # Prepare the query to get yesterday's data
    query = f"""
        SELECT *
        FROM `{dataset_name}.{table_id}`
        WHERE today_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """
    query_job = bq_client.query(query)
    
    # Get the results
    results = query_job.result()
    
    # Check if there are any rows
    if results.total_rows > 0:
        # Convert results to a DataFrame and upload to GCS
        df = results.to_dataframe()
        destination_blob_name = f"incremental/{table_id}/{yesterday.strftime('%Y-%m-%d')}/"
        
        # Upload DataFrame to GCS as Parquet
        df.to_parquet(f"gs://{bucket.name}/{destination_blob_name}data.parquet")
        print(f"Uploaded yesterday's data for {table_id} to gs://{bucket.name}/{destination_blob_name}data.parquet")

def extract_and_upload_to_gcs(bq_client, dataset_name, table_id, bucket, date, backup_type):
    # Get the project ID from the BigQuery client
    project_id = bq_client.project
    fully_qualified_table_id = f"{project_id}.{dataset_name}.{table_id}"

    # Format destination blob name
    destination_blob_name = f"{backup_type}/{table_id}/{date.strftime('%Y-%m-%d')}/"
    
    # Extract data from BigQuery table to GCS
    extract_job = bq_client.extract_table(
        fully_qualified_table_id,
        f"gs://{bucket.name}/{destination_blob_name}*.parquet",  # Save as Parquet
        location="US"  # Adjust if your dataset is in a different region
    )
    extract_job.result()  # Wait for the job to complete
    print(f"Extracted {table_id} to gs://{bucket.name}/{destination_blob_name}")

def cleanup_old_backups(bucket, dataset_name, retention_date):
    # Cleanup function to remove blobs older than retention_days
    blobs = bucket.list_blobs(prefix=dataset_name)
    for blob in blobs:
        if blob.time_created < retention_date:
            print(f"Deleting {blob.name} as it is older than the retention period.")
            blob.delete()

# Entry point for the Cloud Function
def entry_point(request):
    return backup_and_cleanup(request)
