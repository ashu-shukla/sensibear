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
        print(f'Data of {ret} added!')

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
        sql = 'INSERT INTO nifty_candlesticks(day_id,date, bullish,bearish) VALUES(%s,%s,%s,%s);'
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
