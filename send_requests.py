import requests

devices = [f"PLAT1-DEV-{i:04d}-000" for i in range(200)]

device_string = ",".join(devices)

url = "http://localhost:8001/pcid/PLATCUST0005/acid/APPCUST0001/id/" + device_string + "/export"

res = requests.post(url)

print(res.status_code)
print("DONE")