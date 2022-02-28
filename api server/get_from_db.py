from config import config
import psycopg2


def get_latest_five_days_data(cur):
    sql = f"""select json_agg(to_json(d))
from (
	select 
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
			'buy',f.buy,
			'sell',f.sell,
			'net',f.net,
            'oi',f.oi
		) as fii_fut_crores,
		json_build_object(
			'retail', json_build_object(
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
			),
			'dii', json_build_object(
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
			),
			'fii', json_build_object(
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
			),
			'prop', json_build_object(
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
			)
		) as oi
	from 
		daily_cash e, 
        fii_future_crores f,
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
        e.day_id=f.day_id
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
		and e.day_id=prop.day_id 
    ORDER BY e.date DESC LIMIT 5
) as d;
    """
    cur.execute(sql)
    ret = cur.fetchone()[0]
    return ret


def latest_data_in_postgres():
    conn = None
    try:
        params = config()
        conn = psycopg2.connect(**params)
        cur = conn.cursor()
        data = get_latest_five_days_data(cur)
        conn.commit()
        cur.close()
        return data
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
