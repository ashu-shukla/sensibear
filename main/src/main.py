from resources import calculate_change_from_ytd as ytd
from resources import get_fii_future_cash as fii_fut_cash
from resources import get_participant_wise_fao as participant_fao
from resources import get_day_cash_data as day_cash
from resources import get_candlestick_pattern_of_the_day as cs
from resources import add_data_to_db as add_to_db
from datetime import datetime, date

# Main runs only on CRON settings and a pre-check code to see all the details and conditions are met.
# Code the pre-check here============>


# Different date formats.
today = datetime.today()
date_format_for_FAO_participant_wise_oi = today.strftime('%d%m%Y')  # DDMMYYYY
date_format_for_day_cash_and_fii_stats = today.strftime(
    '%d-%b-%Y')  # DD-MMM-YYYY
date_format_for_DB = today.strftime('%Y-%m-%d')  # YYYY-MM-DD


def get_data():
    final = {}

    # To get candlestick pattern of the day and close price.
    final['candlesticks'], final['close'] = cs.candlesticks()

    print("\nAdded Date")
    final['date'] = date_format_for_DB

    # To get Cash Data of FII and DII
    final['cash'] = day_cash.get_fii_dii_eqt(
        date_format_for_day_cash_and_fii_stats)
    print("\nAdded Cash info")

    # To get OI of all paticipants.
    final['oi'] = participant_fao.get_fao_oi(
        date_format_for_FAO_participant_wise_oi)
    print("\nAdded OI data")

    # To get Future Buy and Sell of FII in Crores.
    final['fii_future_crores'] = fii_fut_cash.get_fii_stats(
        date_format_for_day_cash_and_fii_stats)
    print("\nAdded FUT Data")

    # print(final)

    # Calculating change from previos day.
    done = ytd.previous_data_compare(final)
    # print(done)

    # Adding data to DB
    add_to_db.add_daily_data(done)


def prelims():
    if date.today().weekday() == 6 or date.today().weekday() == 5:
        print('Saturday and Sunday nothing to do!')
        quit()
    else:
        get_data()


prelims()
