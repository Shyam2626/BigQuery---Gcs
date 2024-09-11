from google.cloud import bigquery

client = bigquery.Client()

query = query = """
    SELECT  
        COUNT(*) AS total_count,
        AVG(counter) AS avg_counter,
        MAX(counter) AS max_counter,
        MIN(counter) AS min_counter
    FROM
        `superops-poc.shyamDataset.time`
    WHERE
        currtime BETWEEN '2024-09-06T11:33:20.191681Z' AND '2024-09-06T11:37:51.524228Z'
"""

query_job = client.query(query)

for row in query_job:
    print(f"Total Count: {row.total_count}, Average Counter: {row.avg_counter}, Max Counter: {row.max_counter}, Min Counter: {row.min_counter}")
