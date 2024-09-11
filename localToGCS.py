from google.cloud import storage

def upload_to_gcs(bucket_name, source_file_name, destination_blob_name):
    # Initialize a GCS client
    storage_client = storage.Client()

    # Get the bucket
    bucket = storage_client.bucket(bucket_name)
    
    # Create a new blob and upload the file's content
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_name)
    
    print(f"File {source_file_name} uploaded to gs://{bucket_name}/{destination_blob_name}.")

# Usage example
bucket_name = 'shyam-big-query'
source_file_name = '/home/shyam/Downloads/bitcoin/bitcoin.csv'
destination_blob_name = 'bitcoin.csv'  # Where to store the file in the GCS bucket

upload_to_gcs(bucket_name, source_file_name, destination_blob_name)
