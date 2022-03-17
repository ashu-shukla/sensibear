from numpy import rint
import requests
import json
import access_postgres as pg


def add_to_mongo(data):
    url = "https://data.mongodb-api.com/app/data-tupvs/endpoint/data/beta/action/insertMany"

    payload = json.dumps({
        "collection": "daily",
        "database": "sensibear",
        "dataSource": "snowflake",
        "documents": data,
    })
    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Request-Headers': '*',
        'api-key': 'JBSrylO5CCrXRY8xbeI2RoMDOijzmR0TlXZK3dRjVIz9UvnTtXRd7GkfyD1PMyI1'
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
