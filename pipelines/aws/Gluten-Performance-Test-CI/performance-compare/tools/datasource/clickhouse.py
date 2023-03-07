import logging

from clickhouse_driver import dbapi

from common import config
from datasource import db_api_client
from datasource import statement

logger = logging.getLogger()

ENGINE = "clickhouse"


class ClickhouseDBApiClient(db_api_client.DBApiClient):
    dialect = "clickhouse"

    def __init__(self):
        super().__init__()

    def create_connection(self):
        return dbapi.connect(database=config.CONNECTION_DATABASE, user=config.CONNECTION_USER,
                             password=config.CONNECTION_PASSWORD, host=config.CONNECTION_HOST,
                             port=config.CONNECTION_PORT)

    def engine_sql(self):
        return " ENGINE=MergeTree() "

    def order_by_sql(self, order_by_column):
        if order_by_column == "" or len(order_by_column) == 0:
            return " ORDER BY tuple() "

        return " ORDER BY ( " + ",".join(order_by_column) + ") "

    def trans_column_nullable(self, nullable):
        return ""

    def trans_column_type(self, origin_type):
        if origin_type == statement.ColumnType.STRING:
            return "String"

        return origin_type.name
