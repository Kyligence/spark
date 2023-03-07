import os
import sys
from abc import abstractmethod

from common import config
from datasource import statement

log = config.log


class Query:
    statement = ""
    result = None
    name = ""
    order = False


def init_stmt(dialect):
    stmts = {}

    if dialect == "":
        return stmts

    stmt_path = os.path.join(config.SQL_PATH, dialect)
    if not os.path.exists(stmt_path):
        return stmts

    for sql_file_name in os.listdir(stmt_path):
        file_full_path = os.path.join(stmt_path, sql_file_name)
        if os.path.isfile(file_full_path) and sql_file_name.endswith(".sql"):
            sql_name = sql_file_name[:-len(".sql")]
            query = Query()
            query.name = sql_name

            with open(file_full_path, "r") as file:
                query.statement = file.read()

            query.result = get_stmt_result(os.path.join(os.path.join(stmt_path, "result"), sql_name + ".tsv"))
            query.order = os.path.exists(os.path.join(os.path.join(stmt_path, "result"), sql_name + ".order"))
            stmts[sql_name] = query

    return stmts


def get_stmt_result(result_file_path):
    content = None

    if os.path.exists(result_file_path) and os.path.isfile(result_file_path):
        content = []
        with open(result_file_path, "r") as file:
            line = file.readline().strip('\n')
            while line:
                content.append(line.split('\t'))
                line = file.readline().strip('\n')

    return content


class DBApiClient(object):
    dialect = "ansi"

    def __init__(self):
        self.create_table_sql = statement.Tpch().create_table_sql(self)
        try:
            self.connection = self.create_connection()
            self.create_table()
        except Exception as e:
            log.error(e)
            sys.exit(-1)

    def get_stmt(self):
        stmt = {}
        if not config.ONLY_CREATE_TABLE:
            stmt = init_stmt(config.DEFAULT_DIALECT)
            if config.DEFAULT_DIALECT != self.dialect:
                dia = init_stmt(self.dialect)
                for sql_name in dia:
                    if sql_name in stmt:
                        if dia[sql_name].statement.startswith("skip"):
                            stmt.pop(sql_name, "")
                        else:
                            stmt[sql_name].statement = dia[sql_name].statement
                            if dia[sql_name].result is not None:
                                stmt[sql_name].result = dia[sql_name].result
        return stmt

    @abstractmethod
    def create_connection(self):
        pass

    def execute(self, stmt):
        cursor = self.connection.cursor()
        cursor.execute(stmt)
        cursor.close()
        return []

    def execute_and_fetchall(self, stmt):
        cursor = self.connection.cursor()
        cursor.execute(stmt)
        result = cursor.fetchall()
        cursor.close()
        return result

    def create_table(self):
        for create_sql in self.create_table_sql:
            try:
                self.execute(create_sql)
            except Exception as e:
                log.error(e)

    def trans_column_type(self, origin_type):
        return origin_type.name

    def trans_column_nullable(self, nullable):
        if nullable:
            return " NOT "
        else:
            return " NOT NULL "

    def trans_column_default_value(self, default_value):
        if default_value == "":
            return ""
        else:
            return " default {} ".format(default_value)

    def random_column(self):
        return ""

    def engine_sql(self):
        return ""

    # @abstractmethod
    def order_by_sql(self, order_by_column):
        return ""

    def shard_by_sql(self, shard_by_column):
        return ""

    def location_sql(self, location_uri):
        return ""

    def other_sql(self, table):
        return ""

    def pre_create_table(self):
        return []
