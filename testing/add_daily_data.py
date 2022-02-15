import psycopg2
from config import config
from datetime import datetime


def add_daily_data():
    # cash = data['cash']
    # fii_future_crores = data['fii_future_crores']
    # date = data['date']
    # close = data['close']
    # retail = data['oi']['retail']
    # fii = data['oi']['fii']
    # dii = data['oi']['dii']
    # prop = data['oi']['prop']
    # bullish = data["candlesticks"]["bullish"]
    # bearish = data["candlesticks"]["bearish"]

    # participants = {'retail': retail, 'fii': fii,
    #                 'dii': dii, 'proprietary': prop}

    # uid = date.replace('-', "")

    def getone(cur):
        sql = """select 
	e.date as "date",
	e.nifty_close as "close",
	json_build_object(
		'dii',json_build_object(
			'buy',e.dii_cash_buy,
			'sell', e.dii_cash_sell,
			'net', e.dii_cash_net
		),
		'fii',json_build_object(
			'buy',e.fii_cash_buy,
			'sell', e.fii_cash_sell,
			'net', e.fii_cash_net
		)
	) as cash,
	json_build_object(
		'bullish', ca.bullish,
		'bearish', ca.bearish
	) as candlesticks,
	json_build_object(
			'fut_oi',json_build_object(
				'long_oi',retf.long_oi,
				'short_oi',retf.short_oi,
				'net_oi',retf.net_oi,
				'interday_change_in_long_oi',retf.interday_change_in_long_oi,
				'interday_percentage_change_in_long_oi',retf.interday_percentage_change_in_long_oi,
				'interday_change_in_short_oi',retf.interday_change_in_short_oi,
				'interday_percentage_change_in_short_oi',retf.interday_percentage_change_in_short_oi,
				'interday_change_in_net_oi',retf.interday_change_in_net_oi
			),
			'call',json_build_object(
				'long_oi',retc.long_oi,
				'short_oi',retc.short_oi,
				'net_oi',retc.net_oi,
				'interday_change_in_long_oi',retc.interday_change_in_long_oi,
				'interday_percentage_change_in_long_oi',retc.interday_percentage_change_in_long_oi,
				'interday_change_in_short_oi',retc.interday_change_in_short_oi,
				'interday_percentage_change_in_short_oi',retc.interday_percentage_change_in_short_oi,
				'interday_change_in_net_oi',retc.interday_change_in_net_oi
			),
			'put',json_build_object(
				'long_oi',retp.long_oi,
				'short_oi',retp.short_oi,
				'net_oi',retp.net_oi,
				'interday_change_in_long_oi',retp.interday_change_in_long_oi,
				'interday_percentage_change_in_long_oi',retp.interday_percentage_change_in_long_oi,
				'interday_change_in_short_oi',retp.interday_change_in_short_oi,
				'interday_percentage_change_in_short_oi',retp.interday_percentage_change_in_short_oi,
				'interday_change_in_net_oi',retp.interday_change_in_net_oi
			)
		)as retail,
		json_build_object(
			'fut_oi',json_build_object(
				'long_oi',diif.long_oi,
				'short_oi',diif.short_oi,
				'net_oi',diif.net_oi,
				'interday_change_in_long_oi',diif.interday_change_in_long_oi,
				'interday_percentage_change_in_long_oi',diif.interday_percentage_change_in_long_oi,
				'interday_change_in_short_oi',diif.interday_change_in_short_oi,
				'interday_percentage_change_in_short_oi',diif.interday_percentage_change_in_short_oi,
				'interday_change_in_net_oi',diif.interday_change_in_net_oi
			),
			'call',json_build_object(
				'long_oi',diic.long_oi,
				'short_oi',diic.short_oi,
				'net_oi',diic.net_oi,
				'interday_change_in_long_oi',diic.interday_change_in_long_oi,
				'interday_percentage_change_in_long_oi',diic.interday_percentage_change_in_long_oi,
				'interday_change_in_short_oi',diic.interday_change_in_short_oi,
				'interday_percentage_change_in_short_oi',diic.interday_percentage_change_in_short_oi,
				'interday_change_in_net_oi',diic.interday_change_in_net_oi
			),
			'put',json_build_object(
				'long_oi',diip.long_oi,
				'short_oi',diip.short_oi,
				'net_oi',diip.net_oi,
				'interday_change_in_long_oi',diip.interday_change_in_long_oi,
				'interday_percentage_change_in_long_oi',diip.interday_percentage_change_in_long_oi,
				'interday_change_in_short_oi',diip.interday_change_in_short_oi,
				'interday_percentage_change_in_short_oi',diip.interday_percentage_change_in_short_oi,
				'interday_change_in_net_oi',diip.interday_change_in_net_oi
			)
		) as dii,
		json_build_object(
			'fut_oi',json_build_object(
				'long_oi',fiif.long_oi,
				'short_oi',fiif.short_oi,
				'net_oi',fiif.net_oi,
				'interday_change_in_long_oi',fiif.interday_change_in_long_oi,
				'interday_percentage_change_in_long_oi',fiif.interday_percentage_change_in_long_oi,
				'interday_change_in_short_oi',fiif.interday_change_in_short_oi,
				'interday_percentage_change_in_short_oi',fiif.interday_percentage_change_in_short_oi,
				'interday_change_in_net_oi',fiif.interday_change_in_net_oi
			),
			'call',json_build_object(
				'long_oi',fiic.long_oi,
				'short_oi',fiic.short_oi,
				'net_oi',fiic.net_oi,
				'interday_change_in_long_oi',fiic.interday_change_in_long_oi,
				'interday_percentage_change_in_long_oi',fiic.interday_percentage_change_in_long_oi,
				'interday_change_in_short_oi',fiic.interday_change_in_short_oi,
				'interday_percentage_change_in_short_oi',fiic.interday_percentage_change_in_short_oi,
				'interday_change_in_net_oi',fiic.interday_change_in_net_oi
			),
			'put',json_build_object(
				'long_oi',fiip.long_oi,
				'short_oi',fiip.short_oi,
				'net_oi',fiip.net_oi,
				'interday_change_in_long_oi',fiip.interday_change_in_long_oi,
				'interday_percentage_change_in_long_oi',fiip.interday_percentage_change_in_long_oi,
				'interday_change_in_short_oi',fiip.interday_change_in_short_oi,
				'interday_percentage_change_in_short_oi',fiip.interday_percentage_change_in_short_oi,
				'interday_change_in_net_oi',fiip.interday_change_in_net_oi
			)
		)as fii,
		json_build_object(
			'fut_oi',json_build_object(
				'long_oi',prof.long_oi,
				'short_oi',prof.short_oi,
				'net_oi',prof.net_oi,
				'interday_change_in_long_oi',prof.interday_change_in_long_oi,
				'interday_percentage_change_in_long_oi',prof.interday_percentage_change_in_long_oi,
				'interday_change_in_short_oi',prof.interday_change_in_short_oi,
				'interday_percentage_change_in_short_oi',prof.interday_percentage_change_in_short_oi,
				'interday_change_in_net_oi',prof.interday_change_in_net_oi
			),
			'call',json_build_object(
				'long_oi',proc.long_oi,
				'short_oi',proc.short_oi,
				'net_oi',proc.net_oi,
				'interday_change_in_long_oi',proc.interday_change_in_long_oi,
				'interday_percentage_change_in_long_oi',proc.interday_percentage_change_in_long_oi,
				'interday_change_in_short_oi',proc.interday_change_in_short_oi,
				'interday_percentage_change_in_short_oi',proc.interday_percentage_change_in_short_oi,
				'interday_change_in_net_oi',proc.interday_change_in_net_oi
			),
			'put',json_build_object(
				'long_oi',prop.long_oi,
				'short_oi',prop.short_oi,
				'net_oi',prop.net_oi,
				'interday_change_in_long_oi',prop.interday_change_in_long_oi,
				'interday_percentage_change_in_long_oi',prop.interday_percentage_change_in_long_oi,
				'interday_change_in_short_oi',prop.interday_change_in_short_oi,
				'interday_percentage_change_in_short_oi',prop.interday_percentage_change_in_short_oi,
				'interday_change_in_net_oi',prop.interday_change_in_net_oi
			)
		)as prop
from 
	daily_cash e, 
	nifty_candlesticks ca,
	retail_future_open_interest retf, 
	retail_call_open_interest retc, 
	retail_put_open_interest retp ,
	dii_future_open_interest diif, 
	dii_call_open_interest diic, 
	dii_put_open_interest diip,
	fii_future_open_interest fiif, 
	fii_call_open_interest fiic, 
	fii_put_open_interest fiip,
	proprietary_future_open_interest prof, 
	proprietary_call_open_interest proc, 
	proprietary_put_open_interest prop
where 
	e.date BETWEEN '2022-02-07' AND '2022-02-15'
	and e.day_id=ca.day_id 
	and e.day_id=retf.day_id 
	and e.day_id=retc.day_id 
	and e.day_id=retp.day_id 
	and e.day_id=diif.day_id 
	and e.day_id=diic.day_id 
	and e.day_id=diip.day_id
	and e.day_id=fiif.day_id 
	and e.day_id=fiic.day_id 
	and e.day_id=fiip.day_id
	and e.day_id=prof.day_id 
	and e.day_id=proc.day_id 
	and e.day_id=prop.day_id """
        cur.execute(sql)
        ret = cur.fetchall()
        print(ret)

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
            getone(cur)
            # add_cash_data(cur)
            # add_oi_data(cur)
            # add_fii_future_crores(cur)
            # add_candlesticks(cur)
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


add_daily_data()
