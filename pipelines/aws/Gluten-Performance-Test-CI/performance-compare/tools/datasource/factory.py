import logging
import sys

from datasource import clickhouse
from datasource import gluten
from datasource import hive
from datasource import mysql
from datasource import starrocks
from datasource import doris

def get_client(engine):
    client = str.lower(engine)
    if client == str.lower(clickhouse.ENGINE):
        return clickhouse.ClickhouseDBApiClient()
    elif client == str.lower(hive.ENGINE):
        return hive.HiveDBApiClient()
    elif client == str.lower(mysql.ENGINE):
        return mysql.MysqlDBApiClient()
    elif client == str.lower(gluten.ENGINE):
        return gluten.GlutenDBApiClient()
    elif client == str.lower(starrocks.ENGINE):
        return starrocks.StarrocksDBApiClient()
    elif client == str.lower(doris.ENGINE):
        return doris.DorisDBApiClient()
    else:
        logging.getLogger("Engine").error("Engine {} not support", client)
        sys.exit(10)
