import json
from msilib.schema import tables
from operator import index
import requests
from datetime import datetime, timedelta, time, date
import pandas as pd
import patterns_detect as cs
import yesterdays_data as yd
import add_daily_data_to_db as add

# Get previous days data from DB
last_date, recent_oi_data, list_of_dates = yd.get_yesterdays_data()

# To Scrape any NSE Link.


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
        output = requests.get(payload, headers=headers, timeout=2).json()
        # print(output)
    except ValueError:
        s = requests.Session()
        output = s.get("http://nseindia.com", headers=headers, timeout=2)
        output = s.get(payload, headers=headers, timeout=2).json()
    return output

# Function to prevent any false data numbers which may cause error.


def time_in_range():
    return time(19, 0, 0) <= datetime.now().time() <= time(23, 59, 0)


if time_in_range():
    today = datetime.today()
else:
    today = datetime.today()-timedelta(days=1)

# Different date formats.
fao_oi_date_format = today.strftime('%d%m%Y')  # DDMMYYYY
fii_stats_date_format = today.strftime('%d-%b-%Y')  # DD-MMM-YYYY
json_date_format = today.strftime('%Y-%m-%d')  # YYYY-MM-DD


def get_fao_oi(date):
    main = {}
    fii = {}
    retail = {}
    dii = {}
    prop = {}
    fao_oi_url = f'https://archives.nseindia.com/content/nsccl/fao_participant_oi_{date}.csv'
    df = pd.read_csv(fao_oi_url)
    # Some CSV didn't have header only.
    if True in df.columns.str.contains('^Unnamed'):
        new_header = df.iloc[0]  # grab the first row for the header
        df = df[1:-1]  # take the data less the header row
        df.columns = new_header  # set the header row as the df header
        df = df.dropna(axis=1, how='all')
    df = df[['Client Type', 'Future Index Long', 'Future Index Short', 'Option Index Call Long', 'Option Index Call Short',
            'Option Index Put Long', 'Option Index Put Short']]
    # To remove any commas in the numbers as they will be considered as string.
    df = df.apply(lambda x: x.astype(str).str.replace(',', ''))
    # Extracting Future OI
    ff = df[['Future Index Long', 'Future Index Short']]
    ff = ff.apply(pd.to_numeric)
    ff['net_oi'] = ff['Future Index Long']-ff['Future Index Short']
    ff.columns = ['long_oi', 'short_oi', 'net_oi']
    future_js = json.loads(ff.to_json(orient='records'))
    retail['fut_oi'] = future_js[0]
    dii['fut_oi'] = future_js[1]
    fii['fut_oi'] = future_js[2]
    prop['fut_oi'] = future_js[3]

    # Extracting Call OI
    cf = df[['Option Index Call Long', 'Option Index Call Short']]
    cf = cf.apply(pd.to_numeric)
    cf['net_oi'] = cf['Option Index Call Long']-cf['Option Index Call Short']
    cf.columns = ['long_oi', 'short_oi', 'net_oi']
    call_js = json.loads(cf.to_json(orient='records'))
    retail['call'] = call_js[0]
    dii['call'] = call_js[1]
    fii['call'] = call_js[2]
    prop['call'] = call_js[3]

    # Extracting Put OI
    pf = df[['Option Index Put Long', 'Option Index Put Short']]
    pf = pf.apply(pd.to_numeric)
    pf['net_oi'] = pf['Option Index Put Long']-pf['Option Index Put Short']
    pf.columns = ['long_oi', 'short_oi', 'net_oi']
    put_js = json.loads(pf.to_json(orient='records'))
    retail['put'] = put_js[0]
    dii['put'] = put_js[1]
    fii['put'] = put_js[2]
    prop['put'] = put_js[3]

    # Final Data
    main['retail'] = retail
    main['dii'] = dii
    main['fii'] = fii
    main['prop'] = prop
    # print(df)
    return main

# Get FII futures in crores.


def get_fii_stats(date):
    fii_stats = f'https://www1.nseindia.com/content/fo/fii_stats_{date}.xls'
    df = pd.read_excel(fii_stats, nrows=6)
    df = df[2:]
    df.columns = ['Derivative Products', 'Contracts Bought', 'Buy in Crores',
                  'Contracts Sold', 'Sell in Crores', 'OI', 'OI in Crores']
    df['Buy in Crores'] = pd.to_numeric(df['Buy in Crores'])
    df['Sell in Crores'] = pd.to_numeric(df['Sell in Crores'])
    df['Net in Crores'] = df['Buy in Crores'] - df['Sell in Crores']
    fii_fut = df[0:1]
    fii_fut = fii_fut[['Buy in Crores',
                       'Sell in Crores', 'Net in Crores', 'OI']]
    fii_fut.columns = ['buy', 'sell', 'net', 'oi']
    fii_fut = fii_fut.apply(pd.to_numeric)
    js = json.loads(fii_fut.to_json(orient='records'))
    # print(df)
    return js[0]


# IDK about NSDL bro.
def get_nsdl_fii_data():
    nsdl_url = 'https://www.fpi.nsdl.co.in/web/Reports/Latest.aspx'
    tables = pd.read_html(nsdl_url)
    df1 = tables[0]
    df1.columns = ['Date', 'Segment', 'Sector', 'Buy Value', 'Sell Value',
                   'Net Value Rs Crores', 'Net Value $ Millions', 'INR']
    df1 = df1[:-1]
    # Cash Market FII
    print(df1)
    df2 = tables[1]
    df2.columns = ['Date', 'Derivative Products', 'Contracts Bought', 'Buy in Crores',
                   'Contracts Sold', 'Sell in Crores', 'OI', 'OI in Crores']
    df2 = df2[:-2]
    # Future Market FII
    print(df2)
# get_nsdl_fii_data()

# Function to get cash data of the day.


def get_fii_dii_eqt():
    main = {}
    dii_data = {}
    fii_data = {}
    url = 'https://www.nseindia.com/api/fiidiiTradeReact'
    js = nsefetch(url)
    # print(js[0]['date'], fii_stats_date_format)
    if js[0]['date'] == fii_stats_date_format:
        dii_data['buy'] = float(js[0]['buyValue'])
        dii_data['sell'] = float(js[0]['sellValue'])
        dii_data['net'] = float(js[0]['netValue'])
        fii_data['buy'] = float(js[1]['buyValue'])
        fii_data['sell'] = float(js[1]['sellValue'])
        fii_data['net'] = float(js[1]['netValue'])
        main['dii'] = dii_data
        main['fii'] = fii_data
        return main
    else:
        print('FII DII Cash data not updated yet!')
        quit()


# Function to calculate Interday changes.
def previous_data_compare(new_data):
    new_oi_data = new_data['oi']
    participants = recent_oi_data.keys()
    # Calculating and Adding data of inter-day changes to all the participants.
    for participant in participants:
        recent_derivatives = recent_oi_data[participant]
        new_derivatives = new_oi_data[participant]
        derivatives_keys = recent_derivatives.keys()
        # For FUT,Call and Put of every participant.
        for derivative in derivatives_keys:
            new_derivatives[derivative]['interday_change_in_long_oi'] = new_derivatives[derivative]['long_oi'] - \
                recent_derivatives[derivative]['long_oi']
            # Percentage change, if divide by zero error then 0 as of now.
            new_derivatives[derivative]['interday_percentage_change_in_long_oi'] = (
                new_derivatives[derivative]['interday_change_in_long_oi'] / recent_derivatives[derivative]['long_oi'])*100 if recent_derivatives[derivative]['long_oi'] else 0
            new_derivatives[derivative]['interday_change_in_short_oi'] = new_derivatives[derivative]['short_oi'] - \
                recent_derivatives[derivative]['short_oi']
            new_derivatives[derivative]['interday_percentage_change_in_short_oi'] = (
                new_derivatives[derivative]['interday_change_in_short_oi'] / recent_derivatives[derivative]['short_oi'])*100 if recent_derivatives[derivative]['short_oi'] else 0
            new_derivatives[derivative]['interday_change_in_net_oi'] = new_derivatives[derivative]['interday_change_in_long_oi'] - \
                new_derivatives[derivative]['interday_change_in_short_oi']
    return new_data


def get_data():
    final = {}
    # To get candlestick pattern of the day and close price.
    final['candlesticks'], final['close'] = cs.candlesticks()
    print("\nAdded Date")
    final['date'] = json_date_format
    # To get Cash Data of FII and DII
    final['cash'] = get_fii_dii_eqt()
    print("\nAdded Cash info")
    # To get OI of all paticipants.
    final['oi'] = get_fao_oi(fao_oi_date_format)
    print("\nAdded OI data")
    # To get Future Buy and Sell of FII in Crores.
    final['fii_future_crores'] = get_fii_stats(fii_stats_date_format)
    print("\nAdded FUT Data")
    # print(final)
    # Calculating change from previos day.
    done = previous_data_compare(final)
    # print(done)
    # Adding data to DB
    add.add_daily_data(done)


def prelims():
    if date.today().weekday() == 6 or date.today().weekday() == 5:
        print('Saturday and Sunday nothing to do!')
        quit()
    else:
        get_data()


prelims()
