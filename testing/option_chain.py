from nsepy.derivatives import get_expiry_date
from datetime import date
from nsepy import get_history
option_chain_url = 'https://www.nseindia.com/api/option-chain-indices?symbol=NIFTY'
expiry = get_expiry_date(year=2022, month=3)
exp = None
for e in expiry:
    exp = e
nifty_opt = get_history(symbol="NIFTY",
                        start=date(2022, 1, 7),
                        end=date.today(),
                        index=True,
                        option_type='PE',
                        strike_price=16400,
                        expiry_date=date(2022, 2, 24))
print(nifty_opt)

# 122-07
# 127-10
# 118-11
# 127-12
# 113-13
# 91-14
# 100-17
# 98-18
# 89-19
# 59-20
# 175-21
# 114-24
# 208-25
