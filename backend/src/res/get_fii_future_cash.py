import pandas as pd
import json

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
