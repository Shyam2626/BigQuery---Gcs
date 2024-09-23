import os
from google.cloud import storage
from datetime import datetime, timedelta

def delete_folders_in_gcs(bucket_name, start_date, end_date):
    # Initialize a Google Cloud Storage client
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Generate date range from start_date to end_date
    current_date = start_date
    while current_date <= end_date:
        folder_name = current_date.date().isoformat() + '/'
        blobs = bucket.list_blobs(prefix=folder_name)

        # Delete each blob in the folder
        for blob in blobs:
            print(f'Deleting {blob.name}...')
            blob.delete()

        print(f'Deleted folder: {folder_name}')
        current_date += timedelta(days=1)

if __name__ == '__main__':
    bucket_name = 'shyam-big-query'
    start_date = datetime(2024, 3, 22)
    end_date = datetime(2024, 9, 9)

    delete_folders_in_gcs(bucket_name, start_date, end_date)
    print('Deletion process completed.')
