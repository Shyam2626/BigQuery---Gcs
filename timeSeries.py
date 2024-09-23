import os
from datetime import datetime, timedelta
from google.cloud import storage, bigquery

def manage_gcs_data(request):
    bucket_name = os.environ['BUCKET_NAME']
    project_id = os.environ['PROJECT_ID']
    dataset_id = os.environ['DATASET_ID']
    table_id = os.environ['TABLE_ID']
    today_date = datetime.utcnow().date().isoformat()
    gcs_destination_uri = f"gs://{bucket_name}/{today_date}/"

    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bigquery_client = bigquery.Client(project=project_id)

    threshold_date = datetime.utcnow() - timedelta(days=180)

    blobs = bucket.list_blobs()
    for blob in blobs:
        if blob.name.startswith(f"{(threshold_date.date().isoformat())}/"):
            print(f"Deleting folder: {blob.name}")
            blob.delete()

    query = f"""
    SELECT *
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE DATE(timestamp_column) = DATE(CURRENT_DATE())
    """
    
    job_config = bigquery.QueryJobConfig(destination=f"{gcs_destination_uri}current-data.parquet",
                                          write_disposition='WRITE_TRUNCATE',
                                          destination_format=bigquery.DestinationFormat.PARQUET)
    query_job = bigquery_client.query(query, job_config=job_config)
    query_job.result()

    print(f"Current day's data exported to GCS: {gcs_destination_uri}")
    return "Data management completed successfully."
