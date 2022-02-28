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
#     "candlesticks": {
#         "bullish": [
#             "Inverted Hammer"
#         ],
#         "bearish": []
#     },
#     "close": 17276.30078125,
#     "date": "2022-02-18",
#     "cash": {
#         "dii": {
#             "buy": 4992.67,
#             "sell": 3063.59,
#             "net": 1929.08
#         },
#         "fii": {
#             "buy": 4314.48,
#             "sell": 6844.44,
#             "net": -2529.96
#         }
#     },
#     "oi": {
#         "retail": {
#             "fut_oi": {
#                 "long_oi": 178465,
#                 "short_oi": 171184,
#                 "net_oi": 7281,
#                 "interday_change_in_long_oi": -2451,
#                 "interday_percentage_change_in_long_oi": -1.354772380552300515156205090,
#                 "interday_change_in_short_oi": 9506,
#                 "interday_percentage_change_in_short_oi": 5.879587822709335840374076869,
#                 "interday_change_in_net_oi": -11957
#             },
#             "call": {
#                 "long_oi": 1717441,
#                 "short_oi": 1798550,
#                 "net_oi": -81109,
#                 "interday_change_in_long_oi": 368767,
#                 "interday_percentage_change_in_long_oi": 27.34293090843302384416100555,
#                 "interday_change_in_short_oi": 416514,
#                 "interday_percentage_change_in_short_oi": 30.13770987152288362965943000,
#                 "interday_change_in_net_oi": -47747
#             },
#             "put": {
#                 "long_oi": 1582197,
#                 "short_oi": 1861435,
#                 "net_oi": -279238,
#                 "interday_change_in_long_oi": 423753,
#                 "interday_percentage_change_in_long_oi": 36.57949801630462931311310689,
#                 "interday_change_in_short_oi": 414700,
#                 "interday_percentage_change_in_short_oi": 28.66454464708464231528234265,
#                 "interday_change_in_net_oi": 9053
#             }
#         },
#         "dii": {
#             "fut_oi": {
#                 "long_oi": 21209,
#                 "short_oi": 63340,
#                 "net_oi": -42131,
#                 "interday_change_in_long_oi": 0,
#                 "interday_percentage_change_in_long_oi": 0,
#                 "interday_change_in_short_oi": -999,
#                 "interday_percentage_change_in_short_oi": -1.552712973468658201091095603,
#                 "interday_change_in_net_oi": 999
#             },
#             "call": {
#                 "long_oi": 819,
#                 "short_oi": 0,
#                 "net_oi": 819,
#                 "interday_change_in_long_oi": 0,
#                 "interday_percentage_change_in_long_oi": 0,
#                 "interday_change_in_short_oi": 0,
#                 "interday_percentage_change_in_short_oi": 0,
#                 "interday_change_in_net_oi": 0
#             },
#             "put": {
#                 "long_oi": 41969,
#                 "short_oi": 0,
#                 "net_oi": 41969,
#                 "interday_change_in_long_oi": -5989,
#                 "interday_percentage_change_in_long_oi": -12.48801034238291838692188999,
#                 "interday_change_in_short_oi": 0,
#                 "interday_percentage_change_in_short_oi": 0,
#                 "interday_change_in_net_oi": -5989
#             }
#         },
#         "fii": {
#             "fut_oi": {
#                 "long_oi": 116656,
#                 "short_oi": 79193,
#                 "net_oi": 37463,
#                 "interday_change_in_long_oi": 5788,
#                 "interday_percentage_change_in_long_oi": 5.220622722516866904787675434,
#                 "interday_change_in_short_oi": -2517,
#                 "interday_percentage_change_in_short_oi": -3.080406315016521845551340105,
#                 "interday_change_in_net_oi": 8305
#             },
#             "call": {
#                 "long_oi": 378029,
#                 "short_oi": 271410,
#                 "net_oi": 106619,
#                 "interday_change_in_long_oi": 64520,
#                 "interday_percentage_change_in_long_oi": 20.57995145274936285720665116,
#                 "interday_change_in_short_oi": 76386,
#                 "interday_percentage_change_in_short_oi": 39.16748707851341373369431455,
#                 "interday_change_in_net_oi": -11866
#             },
#             "put": {
#                 "long_oi": 490909,
#                 "short_oi": 292326,
#                 "net_oi": 198583,
#                 "interday_change_in_long_oi": 26716,
#                 "interday_percentage_change_in_long_oi": 5.755364686671276818047665518,
#                 "interday_change_in_short_oi": 73056,
#                 "interday_percentage_change_in_short_oi": 33.31782733616089752360103981,
#                 "interday_change_in_net_oi": -46340
#             }
#         },
#         "prop": {
#             "fut_oi": {
#                 "long_oi": 26325,
#                 "short_oi": 28938,
#                 "net_oi": -2613,
#                 "interday_change_in_long_oi": -249,
#                 "interday_percentage_change_in_long_oi": -0.9370060961842402348159855498,
#                 "interday_change_in_short_oi": -2902,
#                 "interday_percentage_change_in_short_oi": -9.114321608040201005025125628,
#                 "interday_change_in_net_oi": 2653
#             },
#             "call": {
#                 "long_oi": 556252,
#                 "short_oi": 582581,
#                 "net_oi": -26329,
#                 "interday_change_in_long_oi": 133459,
#                 "interday_percentage_change_in_long_oi": 31.56603822674452982901798280,
#                 "interday_change_in_short_oi": 73846,
#                 "interday_percentage_change_in_short_oi": 14.51561225392394861764966043,
#                 "interday_change_in_net_oi": 59613
#             },
#             "put": {
#                 "long_oi": 580599,
#                 "short_oi": 541912,
#                 "net_oi": 38687,
#                 "interday_change_in_long_oi": 145589,
#                 "interday_percentage_change_in_long_oi": 33.46796625364934139445070228,
#                 "interday_change_in_short_oi": 102313,
#                 "interday_percentage_change_in_short_oi": 23.27416577380749273769958530,
#                 "interday_change_in_net_oi": 43276
#             }
#         }
#     },
#     "fii_future_crores": {
#         "buy": 3592.16,
#         "sell": 2863.0,
#         "net": 729.16,
#         "oi": 195849
#     }
# }
# add_daily_data(data)
