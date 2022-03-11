import psycopg2
from config import config
import requests
from datetime import datetime

# To scrape any NSE link.


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


# Obtaining the option chain data from NSE.
option_chain_url = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'
r = nsefetch(option_chain_url)

# Expiry Dates Dictionary.
unformatted_expiry_dates = r['records']['expiryDates']
expiry_dates = []
for date in unformatted_expiry_dates:
    expiry_dates.append(datetime.strptime(
        date, '%d-%b-%Y').strftime('%Y-%m-%d'))

# Data of option Chain
strikes = r['records']['data']
today = datetime.today()
json_date_format = today.strftime('%Y-%m-%d')

# PSQL query to add or update list of expiry dates in DB.


def add_expiry_dates(cur):
    sql = 'INSERT INTO fno_nifty_expiry_dates(day_id,expiry_date)VALUES(%s,%s)ON CONFLICT (day_id) DO UPDATE SET expiry_date= EXCLUDED.expiry_date;'
    for expiry_date in expiry_dates:
        day_id = expiry_date.replace('-', "")
        cur.execute(sql, (day_id, expiry_date))
    print(f'Expiry dates added or updated to DB!')

# PSQL query to add the entire option chain data to DB.


def add_option_chain(cur):
    sql = """INSERT INTO historical_nifty_option_chain(
                day_id,
                date,
                expiry_date,
                strikePrice,
                ce_openInterest ,
                ce_changeinOpenInterest ,
                ce_pchangeinOpenInterest ,
                ce_impliedVolatility ,
                ce_lastPrice ,
                ce_change ,
                ce_pChange ,
                ce_totalBuyQuantity ,
                ce_totalSellQuantity ,
                pe_openInterest ,
                pe_changeinOpenInterest ,
                pe_pchangeinOpenInterest ,
                pe_impliedVolatility ,
                pe_lastPrice ,
                pe_change ,
                pe_pChange ,
                pe_totalBuyQuantity ,
                pe_totalSellQuantity)VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    for strike in strikes:
        id = datetime.strptime(
            strike['expiryDate'], '%d-%b-%Y').strftime('%Y%m%d')
        # Few Strikes either dont contain CE or PE so adding 0 to those places.
        if 'CE' not in strike:
            ce_openInterest = 0
            ce_changeinOpenInterest = 0
            ce_pchangeinOpenInterest = 0
            ce_impliedVolatility = 0
            ce_lastPrice = 0
            ce_change = 0
            ce_pChange = 0
            ce_totalBuyQuantity = 0
            ce_totalSellQuantity = 0
        else:
            ce_openInterest = strike['CE']['openInterest']
            ce_changeinOpenInterest = strike['CE']['changeinOpenInterest']
            ce_pchangeinOpenInterest = strike['CE']['pchangeinOpenInterest']
            ce_impliedVolatility = strike['CE']['impliedVolatility']
            ce_lastPrice = strike['CE']['lastPrice']
            ce_change = strike['CE']['change']
            ce_pChange = strike['CE']['pChange']
            ce_totalBuyQuantity = strike['CE']['totalBuyQuantity']
            ce_totalSellQuantity = strike['CE']['totalSellQuantity']
        if 'PE' not in strike:
            pe_openInterest = 0
            pe_changeinOpenInterest = 0
            pe_pchangeinOpenInterest = 0
            pe_impliedVolatility = 0
            pe_lastPrice = 0
            pe_change = 0
            pe_pChange = 0
            pe_totalBuyQuantity = 0
            pe_totalSellQuantity = 0
        else:
            pe_openInterest = strike['PE']['openInterest']
            pe_changeinOpenInterest = strike['PE']['changeinOpenInterest']
            pe_pchangeinOpenInterest = strike['PE']['pchangeinOpenInterest']
            pe_impliedVolatility = strike['PE']['impliedVolatility']
            pe_lastPrice = strike['PE']['lastPrice']
            pe_change = strike['PE']['change']
            pe_pChange = strike['PE']['pChange']
            pe_totalBuyQuantity = strike['PE']['totalBuyQuantity']
            pe_totalSellQuantity = strike['PE']['totalSellQuantity']
            cur.execute(sql, (id,
                              json_date_format,
                              strike['expiryDate'],
                              strike['strikePrice'],
                              ce_openInterest,
                              ce_changeinOpenInterest,
                              ce_pchangeinOpenInterest,
                              ce_impliedVolatility,
                              ce_lastPrice,
                              ce_change,
                              ce_pChange,
                              ce_totalBuyQuantity,
                              ce_totalSellQuantity,
                              pe_openInterest,
                              pe_changeinOpenInterest,
                              pe_pchangeinOpenInterest,
                              pe_impliedVolatility,
                              pe_lastPrice,
                              pe_change,
                              pe_pChange,
                              pe_totalBuyQuantity,
                              pe_totalSellQuantity
                              )
                        )
    print(f'Strikes data added to DB!')


def insert_option_data_in_postgres():
    conn = None
    try:
        # read database configuration
        params = config()
        # connect to the PostgreSQL database
        conn = psycopg2.connect(**params)
        # create a new cursor
        cur = conn.cursor()
        # execute the INSERT statement
        add_expiry_dates(cur)
        add_option_chain(cur)
        conn.commit()
        print('Committed to DB!')
        # close communication with the database
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


insert_option_data_in_postgres()
