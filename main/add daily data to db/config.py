import os
from dotenv import load_dotenv
load_dotenv()
# Import os and load env varibales


def config():
    # Config to get env variables in a dictionary form.
    config = {
        'host': 'POSTGRES_HOST',
        'database': 'POSTGRES_DATABASE',
        'user': 'POSTGRES_USER',
        'password': 'POSTGRES_PWD'
    }
    db_config = {k: os.environ.get(v) for k, v in config.items()}
    # print(db_config)
    return db_config
