import pandas as pd
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
