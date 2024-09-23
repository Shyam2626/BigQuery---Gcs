import requests

# URL of the deployed Cloud Function (Update with your actual function's URL)
cloud_function_url = "https://us-central1-superops-poc.cloudfunctions.net/GcsTOBqText"

# Send a POST request to trigger the Cloud Function
response = requests.post(cloud_function_url)

# Check the response status
if response.status_code == 200:
    print("Cloud Function triggered successfully.")
    print("Response:", response.text)
else:
    print(f"Failed to trigger Cloud Function. Status code: {response.status_code}")
    print("Response:", response.text)
