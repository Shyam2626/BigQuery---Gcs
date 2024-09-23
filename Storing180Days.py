import os
from datetime import datetime, timedelta
from google.cloud import storage, bigquery

def manage_gcs_data(request):
    bucket_name = os.environ['BUCKET_NAME']
    project_id = os.environ['PROJECT_ID']
    dataset_id = os.environ['DATASET_ID']
    table_id = os.environ['TABLE_ID']
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    bigquery_client = bigquery.Client(project=project_id)

    # Set threshold date for the last 180 days
    threshold_date = datetime.utcnow() - timedelta(days=180)

    # Transfer existing 180 days of data from BigQuery to GCS
    for days in range(180):
        date = (threshold_date + timedelta(days=days)).date()
        gcs_folder = f"{date.isoformat()}/"

        query = f"""
        SELECT *
        FROM `{project_id}.{dataset_id}.{table_id}`
        WHERE TIMESTAMP_TRUNC(timestamp_column, DAY) = TIMESTAMP('{date.isoformat()}')
        """

        gcs_destination_uri = f"gs://{bucket_name}/{gcs_folder}data-{date.isoformat()}.parquet"
        job_config = bigquery.QueryJobConfig(destination=gcs_destination_uri,
                                              write_disposition='WRITE_TRUNCATE',
                                              destination_format=bigquery.DestinationFormat.PARQUET)
        query_job = bigquery_client.query(query, job_config=job_config)
        query_job.result()

        print(f"Data for {date} exported to GCS: {gcs_destination_uri}")

    # For today's data, create a new folder and store current day's data
    today_date = datetime.utcnow().date().isoformat()
    gcs_today_folder = f"{today_date}/"
    gcs_today_destination_uri = f"gs://{bucket_name}/{gcs_today_folder}current-data.parquet"

    query_today = f"""
    SELECT *
    FROM `{project_id}.{dataset_id}.{table_id}`
    WHERE DATE(timestamp_column) = CURRENT_DATE()
    """

    job_config_today = bigquery.QueryJobConfig(destination=gcs_today_destination_uri,
                                                write_disposition='WRITE_TRUNCATE',
                                                destination_format=bigquery.DestinationFormat.PARQUET)
    query_job_today = bigquery_client.query(query_today, job_config=job_config_today)
    query_job_today.result()

    print(f"Current day's data exported to GCS: {gcs_today_destination_uri}")
    return "Data management completed successfully."
