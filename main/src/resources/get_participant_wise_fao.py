import pandas as pd
import json


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
