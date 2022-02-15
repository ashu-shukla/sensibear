import psycopg2
import itertools
import json
from config import config


def get_oi(cur, previous_date):
    data = {}
    retail = {}
    fii = {}
    dii = {}
    pro = {}
    # Function will get OI for all the participants and return a JSON.
    participants = ['retail', 'fii', 'dii', 'proprietary']
    derivatives = {'future': 'fut_oi', 'call': 'call', 'put': 'put'}
    for participant in participants:
        for derivative, value in derivatives.items():
            sql = f"SELECT * FROM {participant}_{derivative}_open_interest WHERE day_id='{previous_date}'"
            cols = ('day_id', 'long_oi', 'short_oi', 'net_oi', 'interday_change_in_long_oi', 'interday_change_in_short_oi', 'interday_change_in_net_oi', 'interday_percentage_change_in_long_oi', 'interday_percentage_change_in_short_oi',
                    )
            cur.execute(sql)
            rec = cur.fetchone()
            dic = dict(zip(cols, rec))
            if participant == 'retail':
                retail[value] = dic
            if participant == 'fii':
                fii[value] = dic
            if participant == 'dii':
                dii[value] = dic
            if participant == 'proprietary':
                pro[value] = dic
    data['retail'] = retail
    data['fii'] = fii
    data['dii'] = dii
    data['prop'] = pro
    return data


def get_max_date(cur):
    # Fuction to get MAX date from DB
    sql = f'SELECT MAX(date) FROM daily_cash;'
    cur.execute(sql)
    rec = cur.fetchone()
    date = rec[0]
    return date.strftime('%Y%m%d')


def get_list_of_dates(cur):
    # Funtion to get list of all dates from DB
    sql = f'SELECT date FROM daily_cash;'
    cur.execute(sql)
    rec = cur.fetchall()
    datetime_list = list(itertools.chain(*rec))
    dates = [date_obj.strftime('%Y-%m-%d') for date_obj in datetime_list]
    return dates


def get_yesterdays_data():
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        print('Connected to DB!')
        cur = conn.cursor()
        # get data
        last_date = get_max_date(cur)
        print(f'Latest Date Obtained from DB: {last_date}')
        data = get_oi(cur, last_date)
        dates = get_list_of_dates(cur)
        print('Obtained list of dates, latest date and latest dates OI data!')
        cur.close()
        # commit the changes
        conn.commit()
        # Returning last date, yesterdays OI data and list of dates.
        return last_date, data, dates
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Closed DB Connection!')
