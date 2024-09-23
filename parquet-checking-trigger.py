import requests

# Replace with your Cloud Function URL
function_url = "https://us-central1-superops-poc.cloudfunctions.net/parquet-testing"

# Trigger the Cloud Function
response = requests.post(function_url)

# Print the response
print(response.text)
