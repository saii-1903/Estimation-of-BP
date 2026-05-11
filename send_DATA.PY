import requests

payload = {
    "BPSystolic": 122,
    "BPDiastolic": 78,
    "PlethWave": [45, 50, 55, 48, 42, 38, 44, 51, 56, 49],  # your PPG array
    "Name": "Test Patient",
    "Age": 40,
    "Gender": "Male"
}

response = requests.post(
    "http://172.16.22.247:5001/api/receive_data",
    json=payload
)

print(response.status_code)
print(response.json())
