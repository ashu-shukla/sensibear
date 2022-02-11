import json
from datetime import date, datetime
import pandas as pd
import patterns_detect as pat
import yesterdays_data as yd
import add_daily_data as add
data = None
fildata = None
last_date, recent_oi_data, list_of_dates = yd.get_yesterdays_data()


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
    # To remove any commas in the numbers
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
            # Percentage change, if divide by zero then 0 as of now.
            new_derivatives[derivative]['interday_percentage_change_in_long_oi'] = (
                new_derivatives[derivative]['interday_change_in_long_oi'] / recent_derivatives[derivative]['long_oi'])*100 if recent_derivatives[derivative]['long_oi'] else 0
            new_derivatives[derivative]['interday_change_in_short_oi'] = new_derivatives[derivative]['short_oi'] - \
                recent_derivatives[derivative]['short_oi']
            new_derivatives[derivative]['interday_percentage_change_in_short_oi'] = (
                new_derivatives[derivative]['interday_change_in_short_oi'] / recent_derivatives[derivative]['short_oi'])*100 if recent_derivatives[derivative]['short_oi'] else 0
            new_derivatives[derivative]['interday_change_in_net_oi'] = new_derivatives[derivative]['interday_change_in_long_oi'] - \
                new_derivatives[derivative]['interday_change_in_short_oi']
    return new_data


with open('fii_dii.json', 'r+') as file:
    # First we load existing data into a dict.
    file_data = json.load(file)
    data = file_data


dates = list(data['fii_dii_data'])
dates.sort(key=lambda date: datetime.strptime(date, "%Y-%m-%d"))
for daate in dates:
    if daate not in list_of_dates:
        print(f'Adding data of {daate}.')
        final = {}
        # Add Cash Data
        dii_cash = {}
        fii_cash = {}
        cash = {}
        dii_cash['buy'] = data['fii_dii_data'][daate]['cash']['dii']['buy']
        dii_cash['sell'] = data['fii_dii_data'][daate]['cash']['dii']['sell']
        dii_cash['net'] = data['fii_dii_data'][daate]['cash']['dii']['buy_sell_difference']
        fii_cash['buy'] = data['fii_dii_data'][daate]['cash']['fii']['buy']
        fii_cash['sell'] = data['fii_dii_data'][daate]['cash']['fii']['sell']
        fii_cash['net'] = data['fii_dii_data'][daate]['cash']['fii']['buy_sell_difference']
        cash['dii'] = dii_cash
        cash['fii'] = fii_cash
        final['cash'] = cash
        # Get OI
        fao_oi_date_format = datetime.strptime(
            daate, '%Y-%m-%d').strftime('%d%m%Y')
        oi = get_fao_oi(fao_oi_date_format)
        final['oi'] = oi
        # Add FII Futures in Crores
        fii_future_crores = {}
        fii_future_crores['buy'] = data['fii_dii_data'][daate]['future']['fii']['buy']
        fii_future_crores['sell'] = data['fii_dii_data'][daate]['future']['fii']['sell']
        fii_future_crores['oi'] = data['fii_dii_data'][daate]['future']['fii']['oi']
        fii_future_crores['net'] = data['fii_dii_data'][daate]['future']['fii']['buy_sell_difference']
        final['fii_future_crores'] = fii_future_crores
        # Add Candlestick Patterns
        patts = pat.candlesticks(daate)
        final['candlesticks'] = patts
        # Add NIFTY close
        final['close'] = data['nifty_close_price'][daate]
        final['date'] = daate
        # Add previous OI data
        done = previous_data_compare(final)
        add.add_daily_data(done)
