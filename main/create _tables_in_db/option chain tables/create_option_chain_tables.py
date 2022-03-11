import psycopg2
from config import config


def create_tables():
    """ create tables in the PostgreSQL database"""
    commands = (
        """DROP TABLE IF EXISTS fno_nifty_expiry_dates CASCADE""",
        """DROP TABLE IF EXISTS current_nifty_option_chain""",
        """DROP TABLE IF EXISTS historical_nifty_option_chain""",

        """CREATE TABLE IF NOT EXISTS  fno_nifty_expiry_dates(
                day_id INTEGER PRIMARY KEY,
                expiry_date DATE NOT NULL,
                last_updated TIMESTAMP NOT NULL default current_timestamp
                )
        """,
        """CREATE OR REPLACE FUNCTION update_last_updated_column()
            RETURNS TRIGGER AS $$
            BEGIN
            NEW.last_updated = now(); 
            RETURN NEW;
            END;
            $$ language 'plpgsql'
        """,
        """ CREATE TRIGGER last_updated_changes
            BEFORE UPDATE
            ON fno_nifty_expiry_dates
            FOR EACH ROW
            EXECUTE PROCEDURE update_last_updated_column();
        """,
        """ CREATE TABLE IF NOT EXISTS  current_nifty_option_chain(
                day_id INTEGER,
                expiry_date DATE NOT NULL,
                strikePrice NUMERIC,
                last_updated TIMESTAMP NOT NULL default current_timestamp,
                ce_openInterest NUMERIC,
                ce_changeinOpenInterest NUMERIC NOT NULL,
                ce_pchangeinOpenInterest NUMERIC NOT NULL,
                ce_impliedVolatility NUMERIC NOT NULL,
                ce_lastPrice NUMERIC NOT NULL,
                ce_change NUMERIC NOT NULL,
                ce_pChange NUMERIC NOT NULL,
                ce_totalBuyQuantity NUMERIC NOT NULL,
                ce_totalSellQuantity NUMERIC NOT NULL,
                pe_openInterest NUMERIC NOT NULL,
                pe_changeinOpenInterest NUMERIC NOT NULL,
                pe_pchangeinOpenInterest NUMERIC NOT NULL,
                pe_impliedVolatility NUMERIC NOT NULL,
                pe_lastPrice NUMERIC NOT NULL,
                pe_change NUMERIC NOT NULL,
                pe_pChange NUMERIC NOT NULL,
                pe_totalBuyQuantity NUMERIC NOT NULL,
                pe_totalSellQuantity NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES fno_nifty_expiry_dates(day_id)
                    ON DELETE CASCADE
            )
        """,
        """ CREATE TABLE IF NOT EXISTS  historical_nifty_option_chain(
                day_id INTEGER,
                date DATE NOT NULL,
                expiry_date DATE NOT NULL,
                strikePrice NUMERIC,
                last_updated TIMESTAMP NOT NULL default current_timestamp,
                ce_openInterest NUMERIC,
                ce_changeinOpenInterest NUMERIC NOT NULL,
                ce_pchangeinOpenInterest NUMERIC NOT NULL,
                ce_impliedVolatility NUMERIC NOT NULL,
                ce_lastPrice NUMERIC NOT NULL,
                ce_change NUMERIC NOT NULL,
                ce_pChange NUMERIC NOT NULL,
                ce_totalBuyQuantity NUMERIC NOT NULL,
                ce_totalSellQuantity NUMERIC NOT NULL,
                pe_openInterest NUMERIC NOT NULL,
                pe_changeinOpenInterest NUMERIC NOT NULL,
                pe_pchangeinOpenInterest NUMERIC NOT NULL,
                pe_impliedVolatility NUMERIC NOT NULL,
                pe_lastPrice NUMERIC NOT NULL,
                pe_change NUMERIC NOT NULL,
                pe_pChange NUMERIC NOT NULL,
                pe_totalBuyQuantity NUMERIC NOT NULL,
                pe_totalSellQuantity NUMERIC NOT NULL,
                FOREIGN KEY(day_id)
                    REFERENCES fno_nifty_expiry_dates(day_id)
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
        print('Creating Options Table')
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
