from res.nsefetch import nsefetch
# Function to get cash data of the day.


def get_fii_dii_eqt(date_format_for_day_cash_and_fii_stats):
    main = {}
    dii_data = {}
    fii_data = {}
    url = 'https://www.nseindia.com/api/fiidiiTradeReact'
    js = nsefetch(url)
    # print(js[0]['date'], fii_stats_date_format)
    if js[0]['date'] == date_format_for_day_cash_and_fii_stats:
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
