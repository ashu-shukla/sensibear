from numpy import rint
import requests
import json
import access_postgres as pg


def add_to_mongo(data):
    url = "mongo_endpoint"

    payload = json.dumps({
        "collection": "nm",
        "database": "db",
        "dataSource": "ds",
        "documents": data,
    })
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Request-Headers': '*',
        'api-key': 'key'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    print(response.json())


dates = pg.get_list_of_dates()
print('Dates fetched!')
datalist = []
# print(dates)
for date in dates:
    data = pg.get_days_data(date)
    datalist.append(data)
print('Data Fetched')
add_to_mongo(datalist)
