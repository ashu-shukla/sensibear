import yfinance as yf
import talib
from nsepy import get_history
from datetime import timedelta, date

# One extra day as Yfinance likes it.
today = date.today()
start_day = date.today()-timedelta(days=50)
today_str = today.strftime('%Y-%m-%d')  # DD-MMM-YYYY
start_day_str = start_day.strftime('%Y-%m-%d')  # DD-MMM-YYYY

# All the patterns
candlestick_patterns = {
    'CDL2CROWS': 'Two Crows',
    'CDL3BLACKCROWS': 'Three Black Crows',
    'CDL3INSIDE': 'Three Inside Up/Down',
    'CDL3LINESTRIKE': 'Three-Line Strike',
    'CDL3OUTSIDE': 'Three Outside Up/Down',
    'CDL3STARSINSOUTH': 'Three Stars In The South',
    'CDL3WHITESOLDIERS': 'Three Advancing White Soldiers',
    'CDLABANDONEDBABY': 'Abandoned Baby',
    'CDLADVANCEBLOCK': 'Advance Block',
    'CDLBELTHOLD': 'Belt-hold',
    'CDLBREAKAWAY': 'Breakaway',
    'CDLCLOSINGMARUBOZU': 'Closing Marubozu',
    'CDLCONCEALBABYSWALL': 'Concealing Baby Swallow',
    'CDLCOUNTERATTACK': 'Counterattack',
    'CDLDARKCLOUDCOVER': 'Dark Cloud Cover',
    'CDLDOJI': 'Doji',
    'CDLDOJISTAR': 'Doji Star',
    'CDLDRAGONFLYDOJI': 'Dragonfly Doji',
    'CDLENGULFING': 'Engulfing Pattern',
    'CDLEVENINGDOJISTAR': 'Evening Doji Star',
    'CDLEVENINGSTAR': 'Evening Star',
    'CDLGAPSIDESIDEWHITE': 'Up/Down-gap side-by-side white lines',
    'CDLGRAVESTONEDOJI': 'Gravestone Doji',
    'CDLHAMMER': 'Hammer',
    'CDLHANGINGMAN': 'Hanging Man',
    'CDLHARAMI': 'Harami Pattern',
    'CDLHARAMICROSS': 'Harami Cross Pattern',
    'CDLHIGHWAVE': 'High-Wave Candle',
    'CDLHIKKAKE': 'Hikkake Pattern',
    'CDLHIKKAKEMOD': 'Modified Hikkake Pattern',
    'CDLHOMINGPIGEON': 'Homing Pigeon',
    'CDLIDENTICAL3CROWS': 'Identical Three Crows',
    'CDLINNECK': 'In-Neck Pattern',
    'CDLINVERTEDHAMMER': 'Inverted Hammer',
    'CDLKICKING': 'Kicking',
    'CDLKICKINGBYLENGTH': 'Kicking - bull/bear determined by the longer marubozu',
    'CDLLADDERBOTTOM': 'Ladder Bottom',
    'CDLLONGLEGGEDDOJI': 'Long Legged Doji',
    'CDLLONGLINE': 'Long Line Candle',
    'CDLMARUBOZU': 'Marubozu',
    'CDLMATCHINGLOW': 'Matching Low',
    'CDLMATHOLD': 'Mat Hold',
    'CDLMORNINGDOJISTAR': 'Morning Doji Star',
    'CDLMORNINGSTAR': 'Morning Star',
    'CDLONNECK': 'On-Neck Pattern',
    'CDLPIERCING': 'Piercing Pattern',
    'CDLRICKSHAWMAN': 'Rickshaw Man',
    'CDLRISEFALL3METHODS': 'Rising/Falling Three Methods',
    'CDLSEPARATINGLINES': 'Separating Lines',
    'CDLSHOOTINGSTAR': 'Shooting Star',
    'CDLSHORTLINE': 'Short Line Candle',
    'CDLSPINNINGTOP': 'Spinning Top',
    'CDLSTALLEDPATTERN': 'Stalled Pattern',
    'CDLSTICKSANDWICH': 'Stick Sandwich',
    'CDLTAKURI': 'Takuri (Dragonfly Doji with very long lower shadow)',
    'CDLTASUKIGAP': 'Tasuki Gap',
    'CDLTHRUSTING': 'Thrusting Pattern',
    'CDLTRISTAR': 'Tristar Pattern',
    'CDLUNIQUE3RIVER': 'Unique 3 River',
    'CDLUPSIDEGAP2CROWS': 'Upside Gap Two Crows',
    'CDLXSIDEGAP3METHODS': 'Upside/Downside Gap Three Methods'

}


def candlesticks():
    print('\nRetrieveing NIFTY OHLC Data...')
    # df = yf.download('^NSEI', start=start_day_str,
    #                  end=today_str, progress=False)
    df = get_history(symbol="NIFTY", start=start_day, end=today, index=True)
    close = df['Close'].tail(1).values[0]
    ldate = df['Close'].tail(1).index[0]
    print(f'NIFTY closed at {close} on {ldate}')
    bullish = []
    bearish = []
    for key, value in candlestick_patterns.items():
        # Testing patterns
        pattern_func = getattr(talib, key)
        res = pattern_func(df['Open'], df['High'], df['Low'], df['Close'])
        latest = res.tail(1).values[0]
        if latest > 0:
            bullish.append(value)
        if latest < 0:
            bearish.append(value)
    # Returning bullish and bearish patterns with day's close.
    return {'bullish': bullish, 'bearish': bearish}, close


# candlesticks()
