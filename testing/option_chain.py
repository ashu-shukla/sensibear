from nsepy.derivatives import get_expiry_date
from datetime import date
from nsepy import get_history
import requests
import json


def nsefetch(payload):
    headers = {
        'Connection': 'keep-alive',
        'Cache-Control': 'max-age=0',
        'DNT': '1',
        'Upgrade-Insecure-Requests': '1',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.79 Safari/537.36',
        'Sec-Fetch-User': '?1',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'Sec-Fetch-Site': 'none',
        'Sec-Fetch-Mode': 'navigate',
        'Accept-Encoding': 'gzip, deflate, br',
        'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
    }
    try:
        output = requests.get(payload, headers=headers).json()
        # print(output)
    except ValueError:
        s = requests.Session()
        output = s.get("http://nseindia.com", headers=headers)
        output = s.get(payload, headers=headers).json()
    return output


option_chain_url = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'
r = nsefetch(option_chain_url)
with open('chain.json', 'w', encoding='utf-8') as f:
    json.dump(r, f, ensure_ascii=False, indent=4)

# with open('sensi_bull.json', 'r', encoding='utf-8') as f:
#     yo = json.load(f)
#     with open('formatted_sensi.json', 'w', encoding='utf-8') as q:
#         json.dump(json.JSONDecoder().decode(yo),
#                   q, ensure_ascii=False, indent=4)
# expiry = get_expiry_date(year=2022, month=3)
# exp = None
# for e in expiry:
#     exp = e
# nifty_opt = get_history(symbol="NIFTY",
#                         start=date(2022, 1, 7),
#                         end=date.today(),
#                         index=True,
#                         option_type='PE',
#                         strike_price=16400,
#                         expiry_date=date(2022, 2, 24))
# print(nifty_opt)

# 122-07
# 127-10
# 118-11
# 127-12
# 113-13
# 91-14
# 100-17
# 98-18
# 89-19
# 59-20
# 175-21
# 114-24
# 208-25
