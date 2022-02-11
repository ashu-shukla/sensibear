import psycopg2
from config import config
from datetime import datetime


def add_daily_data(data):
    cash = data['cash']
    fii_future_crores = data['fii_future_crores']
    date = data['date']
    close = data['close']
    retail = data['oi']['retail']
    fii = data['oi']['fii']
    dii = data['oi']['dii']
    prop = data['oi']['prop']
    bullish = data["candlesticks"]["bullish"]
    bearish = data["candlesticks"]["bearish"]

    participants = {'retail': retail, 'fii': fii,
                    'dii': dii, 'proprietary': prop}

    uid = date.replace('-', "")

    def add_cash_data(cur):
        daily_cash_sql = 'INSERT INTO daily_cash(day_id,date,nifty_close,dii_cash_buy,dii_cash_sell,dii_cash_net,fii_cash_buy,fii_cash_sell,fii_cash_net) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s) RETURNING date;'
        cur.execute(daily_cash_sql, (uid, date, close, cash['dii']['buy'], cash['dii']['sell'], cash
                                     ['dii']['net'], cash['fii']['buy'], cash['fii']['sell'], cash['fii']['net']))
        ret = cur.fetchone()[0]
        print(f'Data of {ret} added to DB!')

    def add_fii_future_crores(cur):
        sql = 'INSERT INTO fii_future_crores(day_id, buy, sell, net, oi) VALUES(%s,%s,%s,%s,%s);'
        cur.execute(sql, (uid, fii_future_crores['buy'], fii_future_crores['sell'],
                    fii_future_crores['net'], fii_future_crores['oi']))

    def add_oi_data(cur):
        for participant, data in participants.items():
            sectors = {'future': 'fut_oi', 'call': 'call', 'put': 'put'}
            for sector, name in sectors.items():
                oi_sql = f'INSERT INTO {participant}_{sector}_open_interest(day_id,long_oi,short_oi,net_oi,interday_change_in_long_oi,interday_change_in_short_oi,interday_change_in_net_oi,interday_percentage_change_in_long_oi,interday_percentage_change_in_short_oi) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s);'
                cur.execute(oi_sql, (uid, data[name]['long_oi'], data[name]['short_oi'], data[name]['net_oi'], data[name]['interday_change_in_long_oi'], data[name]
                                     ['interday_change_in_short_oi'], data[name]['interday_change_in_net_oi'], data[name]['interday_percentage_change_in_long_oi'], data[name]['interday_percentage_change_in_short_oi']))

    def add_candlesticks(cur):
        sql = 'INSERT INTO nifty_candlesticks(day_id,date,bullish,bearish) VALUES(%s,%s,%s,%s);'
        cur.execute(sql, (uid, date, bullish, bearish))

    def insert_data_in_postgres():
        conn = None
        vendor_id = None
        try:
            # read database configuration
            params = config()
            # connect to the PostgreSQL database
            conn = psycopg2.connect(**params)
            # create a new cursor
            cur = conn.cursor()
            # execute the INSERT statement
            add_cash_data(cur)
            add_oi_data(cur)
            add_fii_future_crores(cur)
            add_candlesticks(cur)
            conn.commit()
            print('Committed to DB!')
            # close communication with the database
            cur.close()
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            if conn is not None:
                conn.close()

        return vendor_id

    insert_data_in_postgres()


# data = {
#     "cash": {
#         "dii": {
#             "buy": 0,
#             "sell": 0,
#             "net": 0
#         },
#         "fii": {
#             "buy": 0,
#             "sell": 0,
#             "net": 0
#         }
#     },
#     "oi": {
#         "retail": {
#             "fut_oi": {
#                 "long_oi": 221211,
#                 "short_oi": 208689,
#                 "net_oi": 12522,
#                 "interday_change_in_long_oi": 11274.0,
#                 "interday_percentage_change_in_long_oi": 5.370182483316423,
#                 "interday_change_in_short_oi": -3991.0,
#                 "interday_percentage_change_in_short_oi": -1.876528117359413,
#                 "interday_change_in_net_oi": 15265.0
#             },
#             "call": {
#                 "long_oi": 396433,
#                 "short_oi": 533958,
#                 "net_oi": -137525,
#                 "interday_change_in_long_oi": -140327.0,
#                 "interday_percentage_change_in_long_oi": -26.143341530665477,
#                 "interday_change_in_short_oi": -79451.0,
#                 "interday_percentage_change_in_short_oi": -12.952369463115149,
#                 "interday_change_in_net_oi": -60876.0
#             },
#             "put": {
#                 "long_oi": 610181,
#                 "short_oi": 821182,
#                 "net_oi": -211001,
#                 "interday_change_in_long_oi": -8510.0,
#                 "interday_percentage_change_in_long_oi": -1.3754846926818074,
#                 "interday_change_in_short_oi": -41955.0,
#                 "interday_percentage_change_in_short_oi": -4.860757909810378,
#                 "interday_change_in_net_oi": 33445.0
#             }
#         },
#         "dii": {
#             "fut_oi": {
#                 "long_oi": 24502,
#                 "short_oi": 30226,
#                 "net_oi": -5724,
#                 "interday_change_in_long_oi": 1038.0,
#                 "interday_percentage_change_in_long_oi": 4.423798158881691,
#                 "interday_change_in_short_oi": -276.0,
#                 "interday_percentage_change_in_short_oi": -0.9048586977903087,
#                 "interday_change_in_net_oi": 1314.0
#             },
#             "call": {
#                 "long_oi": 62061,
#                 "short_oi": 0,
#                 "net_oi": 62061,
#                 "interday_change_in_long_oi": 0.0,
#                 "interday_percentage_change_in_long_oi": 0.0,
#                 "interday_change_in_short_oi": 0.0,
#                 "interday_percentage_change_in_short_oi": 0,
#                 "interday_change_in_net_oi": 0.0
#             },
#             "put": {
#                 "long_oi": 92099,
#                 "short_oi": 0,
#                 "net_oi": 92099,
#                 "interday_change_in_long_oi": 1769.0,
#                 "interday_percentage_change_in_long_oi": 1.958374847780361,
#                 "interday_change_in_short_oi": 0.0,
#                 "interday_percentage_change_in_short_oi": 0,
#                 "interday_change_in_net_oi": 1769.0
#             }
#         },
#         "fii": {
#             "fut_oi": {
#                 "long_oi": 112468,
#                 "short_oi": 72804,
#                 "net_oi": 39664,
#                 "interday_change_in_long_oi": -1195.0,
#                 "interday_percentage_change_in_long_oi": -1.0513535627248973,
#                 "interday_change_in_short_oi": 4684.0,
#                 "interday_percentage_change_in_short_oi": 6.876100998238403,
#                 "interday_change_in_net_oi": -5879.0
#             },
#             "call": {
#                 "long_oi": 186185,
#                 "short_oi": 79194,
#                 "net_oi": 106991,
#                 "interday_change_in_long_oi": 1495.0,
#                 "interday_percentage_change_in_long_oi": 0.809464508094645,
#                 "interday_change_in_short_oi": -4736.0,
#                 "interday_percentage_change_in_short_oi": -5.642797569403074,
#                 "interday_change_in_net_oi": 6231.0
#             },
#             "put": {
#                 "long_oi": 294452,
#                 "short_oi": 131706,
#                 "net_oi": 162746,
#                 "interday_change_in_long_oi": -10169.0,
#                 "interday_percentage_change_in_long_oi": -3.338246542424849,
#                 "interday_change_in_short_oi": 4208.0,
#                 "interday_percentage_change_in_short_oi": 3.3004439285322125,
#                 "interday_change_in_net_oi": -14377.0
#             }
#         },
#         "prop": {
#             "fut_oi": {
#                 "long_oi": 16467,
#                 "short_oi": 62929,
#                 "net_oi": -46462,
#                 "interday_change_in_long_oi": -4402.0,
#                 "interday_percentage_change_in_long_oi": -21.093487948631942,
#                 "interday_change_in_short_oi": 6298.0,
#                 "interday_percentage_change_in_short_oi": 11.121117409192845,
#                 "interday_change_in_net_oi": -10700.0
#             },
#             "call": {
#                 "long_oi": 161151,
#                 "short_oi": 192677,
#                 "net_oi": -31526,
#                 "interday_change_in_long_oi": 7996.0,
#                 "interday_percentage_change_in_long_oi": 5.220854689693448,
#                 "interday_change_in_short_oi": -46649.0,
#                 "interday_percentage_change_in_short_oi": -19.491822869224404,
#                 "interday_change_in_net_oi": 54645.0
#             },
#             "put": {
#                 "long_oi": 174172,
#                 "short_oi": 218016,
#                 "net_oi": -43844,
#                 "interday_change_in_long_oi": -16726.0,
#                 "interday_percentage_change_in_long_oi": -8.76174711102264,
#                 "interday_change_in_short_oi": 4111.0,
#                 "interday_percentage_change_in_short_oi": 1.9218812089478974,
#                 "interday_change_in_net_oi": -20837.0
#             }
#         }
#     },
#     "fii_future_crores": {
#         "buy": 840.7185,
#         "sell": 1288.9327,
#         "oi": 14982.564,
#         "net": -448.2142000000001
#     },
#     "candlesticks": {
#         "bullish": [],
#         "bearish": []
#     },
#     "close": "10504.8",
#     "date": "2018-01-04"
# }
# add_daily_data(data)
