from google.cloud import bigquery
from datetime import datetime, timedelta

# Initialize BigQuery client
client = bigquery.Client()

# Specify dataset ID (replace with your dataset ID)
dataset_id = "superops-poc.shyamDataset"

# Define start and end date
start_date = datetime(2024, 3, 20)
end_date = datetime(2024, 9, 16)

# Iterate over the date range and construct table names
current_date = start_date
while current_date <= end_date:
    # Construct table name as temp_table_YYYY-MM-DD
    table_name = f"temp_table_{current_date.strftime('%Y-%m-%d')}"
    table_id = f"{dataset_id}.{table_name}"
    
    try:
        # Delete the table
        client.delete_table(table_id)  # Make an API request.
        print(f"Deleted table: {table_id}")
    except Exception as e:
        print(f"Could not delete table {table_id}: {e}")

    # Move to the next day
    current_date += timedelta(days=1)

print("Table deletion process completed.")
