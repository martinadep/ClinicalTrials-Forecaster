import requests

response = requests.get("https://clinicaltrials.gov/api/v2/studies")

if response.status_code == 200:
    data = response.json()
    print(data)
else:
    print(f"Error: {response.status_code}")

