import yfinance as yf
import talib
from patterns import candlestick_patterns
from nsepy import get_history
from datetime import datetime, timedelta, time, date

today = date.today()+timedelta(days=1)
start_day = date.today()-timedelta(days=50)
today_str = today.strftime('%Y-%m-%d')  # DD-MMM-YYYY
start_day_str = start_day.strftime('%Y-%m-%d')  # DD-MMM-YYYY


def candlesticks():
    # df = yf.download('^NSEI', start=start_day_str, end=today_str)
    df = get_history(symbol="NIFTY",
                     start=start_day,
                     end=today,
                     index=True)
    print(df)
    bullish = []
    bearish = []
    for key, value in candlestick_patterns.items():
        pattern_func = getattr(talib, key)
        res = pattern_func(df['Open'], df['High'], df['Low'], df['Close'])
        latest = res.tail(1).values[0]
        if latest > 0:
            bullish.append(value)
        if latest < 0:
            bearish.append(value)

    return {'bullish': bullish, 'bearish': bearish}
