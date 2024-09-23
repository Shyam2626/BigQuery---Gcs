from google.cloud import bigquery
import os

def bigquery_to_gcs(request):

    project_id = os.environ['superops-poc']  
    dataset_id = os.environ['shyamDataset'] 
    table_id = os.environ['parquet-testing'] 
    bucket_name = os.environ['shyam-big-query']
    gcs_destination_uri = f"gs://{bucket_name}/text-parquet/your-file-name-*.parquet"

    client = bigquery.Client()

    full_table_id = f"{project_id}.{dataset_id}.{table_id}"

    extract_job_config = bigquery.ExtractJobConfig(
        destination_format=bigquery.DestinationFormat.PARQUET
    )

    extract_job = client.extract_table(
        full_table_id,
        gcs_destination_uri,
        job_config=extract_job_config
    )

    extract_job.result()

    return f"Data exported to {gcs_destination_uri} in Parquet format."

