import psycopg2
from config import config


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """DROP TABLE IF EXISTS daily_cash CASCADE""",
        """DROP TABLE IF EXISTS retail_future_open_interest""",
        """DROP TABLE IF EXISTS retail_call_open_interest""",
        """DROP TABLE IF EXISTS retail_put_open_interest""",
        """DROP TABLE IF EXISTS fii_future_open_interest""",
        """DROP TABLE IF EXISTS fii_call_open_interest""",
        """DROP TABLE IF EXISTS fii_put_open_interest""",
        """DROP TABLE IF EXISTS dii_future_open_interest""",
        """DROP TABLE IF EXISTS dii_call_open_interest""",
        """DROP TABLE IF EXISTS dii_put_open_interest""",
        """DROP TABLE IF EXISTS proprietary_future_open_interest""",
        """DROP TABLE IF EXISTS proprietary_call_open_interest""",
        """DROP TABLE IF EXISTS proprietary_put_open_interest""",
        """DROP TABLE IF EXISTS fii_future_crores""",
        """DROP TABLE IF EXISTS nifty_candlesticks""",
        """
        CREATE TABLE IF NOT EXISTS daily_cash(
            day_id INTEGER PRIMARY KEY,
            date DATE NOT NULL,
            nifty_close NUMERIC NOT NULL,
            dii_cash_buy NUMERIC NOT NULL,
            dii_cash_sell NUMERIC NOT NULL,
            dii_cash_net NUMERIC NOT NULL,
            fii_cash_buy NUMERIC NOT NULL,
            fii_cash_sell NUMERIC NOT NULL,
            fii_cash_net NUMERIC NOT NULL
        )
         """,
        """ CREATE TABLE IF NOT EXISTS retail_future_open_interest(
                day_id INTEGER,
                long_oi NUMERIC NOT NULL,
                short_oi NUMERIC NOT NULL,
                net_oi NUMERIC NOT NULL,
                interday_change_in_long_oi NUMERIC NOT NULL,
                interday_change_in_short_oi NUMERIC NOT NULL,
                interday_change_in_net_oi NUMERIC NOT NULL,
                interday_percentage_change_in_long_oi NUMERIC NOT NULL,
                interday_percentage_change_in_short_oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS retail_call_open_interest(
                day_id INTEGER,
                long_oi NUMERIC NOT NULL,
                short_oi NUMERIC NOT NULL,
                net_oi NUMERIC NOT NULL,
                interday_change_in_long_oi NUMERIC NOT NULL,
                interday_change_in_short_oi NUMERIC NOT NULL,
                interday_change_in_net_oi NUMERIC NOT NULL,
                interday_percentage_change_in_long_oi NUMERIC NOT NULL,
                interday_percentage_change_in_short_oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS retail_put_open_interest(
                day_id INTEGER,
                long_oi NUMERIC NOT NULL,
                short_oi NUMERIC NOT NULL,
                net_oi NUMERIC NOT NULL,
                interday_change_in_long_oi NUMERIC NOT NULL,
                interday_change_in_short_oi NUMERIC NOT NULL,
                interday_change_in_net_oi NUMERIC NOT NULL,
                interday_percentage_change_in_long_oi NUMERIC NOT NULL,
                interday_percentage_change_in_short_oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS fii_future_open_interest(
                day_id INTEGER,
                long_oi NUMERIC NOT NULL,
                short_oi NUMERIC NOT NULL,
                net_oi NUMERIC NOT NULL,
                interday_change_in_long_oi NUMERIC NOT NULL,
                interday_change_in_short_oi NUMERIC NOT NULL,
                interday_change_in_net_oi NUMERIC NOT NULL,
                interday_percentage_change_in_long_oi NUMERIC NOT NULL,
                interday_percentage_change_in_short_oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS fii_call_open_interest(
                day_id INTEGER,
                long_oi NUMERIC NOT NULL,
                short_oi NUMERIC NOT NULL,
                net_oi NUMERIC NOT NULL,
                interday_change_in_long_oi NUMERIC NOT NULL,
                interday_change_in_short_oi NUMERIC NOT NULL,
                interday_change_in_net_oi NUMERIC NOT NULL,
                interday_percentage_change_in_long_oi NUMERIC NOT NULL,
                interday_percentage_change_in_short_oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS fii_put_open_interest(
                day_id INTEGER,
                long_oi NUMERIC NOT NULL,
                short_oi NUMERIC NOT NULL,
                net_oi NUMERIC NOT NULL,
                interday_change_in_long_oi NUMERIC NOT NULL,
                interday_change_in_short_oi NUMERIC NOT NULL,
                interday_change_in_net_oi NUMERIC NOT NULL,
                interday_percentage_change_in_long_oi NUMERIC NOT NULL,
                interday_percentage_change_in_short_oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS dii_future_open_interest(
                day_id INTEGER,
                long_oi NUMERIC NOT NULL,
                short_oi NUMERIC NOT NULL,
                net_oi NUMERIC NOT NULL,
                interday_change_in_long_oi NUMERIC NOT NULL,
                interday_change_in_short_oi NUMERIC NOT NULL,
                interday_change_in_net_oi NUMERIC NOT NULL,
                interday_percentage_change_in_long_oi NUMERIC NOT NULL,
                interday_percentage_change_in_short_oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS dii_call_open_interest(
                day_id INTEGER,
                long_oi NUMERIC NOT NULL,
                short_oi NUMERIC NOT NULL,
                net_oi NUMERIC NOT NULL,
                interday_change_in_long_oi NUMERIC NOT NULL,
                interday_change_in_short_oi NUMERIC NOT NULL,
                interday_change_in_net_oi NUMERIC NOT NULL,
                interday_percentage_change_in_long_oi NUMERIC NOT NULL,
                interday_percentage_change_in_short_oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS dii_put_open_interest(
                day_id INTEGER,
                long_oi NUMERIC NOT NULL,
                short_oi NUMERIC NOT NULL,
                net_oi NUMERIC NOT NULL,
                interday_change_in_long_oi NUMERIC NOT NULL,
                interday_change_in_short_oi NUMERIC NOT NULL,
                interday_change_in_net_oi NUMERIC NOT NULL,
                interday_percentage_change_in_long_oi NUMERIC NOT NULL,
                interday_percentage_change_in_short_oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS  proprietary_future_open_interest(
                day_id INTEGER,
                long_oi NUMERIC NOT NULL,
                short_oi NUMERIC NOT NULL,
                net_oi NUMERIC NOT NULL,
                interday_change_in_long_oi NUMERIC NOT NULL,
                interday_change_in_short_oi NUMERIC NOT NULL,
                interday_change_in_net_oi NUMERIC NOT NULL,
                interday_percentage_change_in_long_oi NUMERIC NOT NULL,
                interday_percentage_change_in_short_oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS  proprietary_call_open_interest(
                day_id INTEGER,
                long_oi NUMERIC NOT NULL,
                short_oi NUMERIC NOT NULL,
                net_oi NUMERIC NOT NULL,
                interday_change_in_long_oi NUMERIC NOT NULL,
                interday_change_in_short_oi NUMERIC NOT NULL,
                interday_change_in_net_oi NUMERIC NOT NULL,
                interday_percentage_change_in_long_oi NUMERIC NOT NULL,
                interday_percentage_change_in_short_oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS  proprietary_put_open_interest(
                day_id INTEGER,
                long_oi NUMERIC NOT NULL,
                short_oi NUMERIC NOT NULL,
                net_oi NUMERIC NOT NULL,
                interday_change_in_long_oi NUMERIC NOT NULL,
                interday_change_in_short_oi NUMERIC NOT NULL,
                interday_change_in_net_oi NUMERIC NOT NULL,
                interday_percentage_change_in_long_oi NUMERIC NOT NULL,
                interday_percentage_change_in_short_oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS  fii_future_crores(
                day_id INTEGER,
                buy NUMERIC NOT NULL,
                sell NUMERIC NOT NULL,
                net NUMERIC NOT NULL,
                oi NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """,
        """ CREATE TABLE IF NOT EXISTS  nifty_candlesticks(
                day_id INTEGER,
                date DATE NOT NULL,
                bullish text[],
                bearish text[],
                FOREIGN KEY(day_id)
                    REFERENCES daily_cash(day_id)
                    ON DELETE CASCADE
                )
        """
    )
    conn = None
    try:
        # read the connection parameters
        params = config()
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**params)
        print('Connected to DB!')
        cur = conn.cursor()
        # create table one by one
        for command in commands:
            cur.execute(command)
        # close communication with the PostgreSQL database server
        print('Commands executed to DB!')
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Closed DB Connection!')


if __name__ == '__main__':
    create_tables()
