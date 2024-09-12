import pandas as pd

# Replace with the path to your Parquet file
parquet_file_path = '/home/shyam/Downloads/p.parquet'

# Load the Parquet file into a DataFrame
df = pd.read_parquet(parquet_file_path)

# Display the DataFrame
print(df.head())
