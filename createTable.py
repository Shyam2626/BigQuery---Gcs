from google.cloud import bigquery

def update_table_schema(project_id, dataset_id, table_id):
    # Initialize a BigQuery client
    client = bigquery.Client(project=project_id)

    # Define the new schema for the table
    new_schema = [
        bigquery.SchemaField("opentime", "TIMESTAMP"),
        bigquery.SchemaField("open", "FLOAT"),
        bigquery.SchemaField("high", "FLOAT"),
        bigquery.SchemaField("low", "FLOAT"),
        bigquery.SchemaField("close", "FLOAT"),
        bigquery.SchemaField("volume", "FLOAT"),
        bigquery.SchemaField("closetime", "TIMESTAMP"),
        bigquery.SchemaField("assetvolume", "FLOAT"),
        bigquery.SchemaField("trades", "INTEGER"),
        bigquery.SchemaField("baseassetvolume", "FLOAT"),
        bigquery.SchemaField("quotaassetvolume", "FLOAT"),
        bigquery.SchemaField("ignore", "INTEGER"),
        bigquery.SchemaField("today_date", "DATE"),
    ]

    # Define the table reference
    table_ref = client.dataset(dataset_id).table(table_id)

    # Fetch the existing table
    try:
        table = client.get_table(table_ref)  # Fetch the table
        print(f"Fetched table {table_id}.")
        
        # Update the table schema
        table.schema = new_schema
        updated_table = client.update_table(table, ["schema"])  # Update only the schema
        
        print(f"Updated table {updated_table.table_id} schema.")
    except Exception as e:
        print(f"Error updating table schema: {e}")

if __name__ == "__main__":
    project_id = "superops-poc"  # Replace with your Google Cloud project ID
    dataset_id = "shyamDataset"   # Replace with your BigQuery dataset ID
    table_id = "recovery"          # Replace with your existing table ID

    update_table_schema(project_id, dataset_id, table_id)
