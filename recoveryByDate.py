import requests

def trigger_cloud_function(date):
    function_url = "https://us-central1-superops-poc.cloudfunctions.net/recoverByName"  # Replace with your function URL
    params = {'date': date}

    response = requests.get(function_url, params=params)

    if response.status_code == 200:
        print("Function triggered successfully:", response.text)
    else:
        print(f"Failed to trigger function. Status code: {response.status_code}, Response: {response.text}")

# Example usage
trigger_cloud_function('2024-09-11')  # Replace with the desired date
