import logging

import pymysql

from common import config
from datasource import db_api_client

logger = logging.getLogger()

ENGINE = "mysql"


class MysqlDBApiClient(db_api_client.DBApiClient):
    dialect = "mysql"

    def __init__(self):
        super().__init__()

    def create_connection(self):
        return pymysql.connect(database=config.CONNECTION_DATABASE, user=config.CONNECTION_USER,
                               password=config.CONNECTION_PASSWORD, host=config.CONNECTION_HOST,
                               port=config.CONNECTION_PORT)
