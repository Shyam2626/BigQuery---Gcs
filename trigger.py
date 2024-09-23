import requests
import json

def trigger_backup_function():
    # Define the Cloud Function URL
    function_url = "https://us-central1-superops-poc.cloudfunctions.net/testing-backup"  # Replace with your Cloud Function URL

    # Define the datasets and their corresponding backup configurations
    datasets = [
        {
            "dataset_name": "shyamDataset",  # Replace with your actual dataset name
            "retention_days": 30,  # Retention days for the incremental backup
            "backup_type": "incremental"  # Change to "full" for full backups
        },
        {
            "dataset_name": "shyamDataset",  # Another dataset for full backup
            "retention_days": 7,  # Retention days for the full backup
            "backup_type": "full"  # Full backup
        }
    ]

    # Construct the payload
    payload = {
        "datasets": datasets
    }

    # Send a POST request to the Cloud Function
    response = requests.post(function_url, json=payload)

    # Check the response
    if response.status_code == 200:
        print("Backup triggered successfully.")
    else:
        print(f"Error triggering backup: {response.status_code}, {response.text}")

# Call the function to trigger the backup
trigger_backup_function()
