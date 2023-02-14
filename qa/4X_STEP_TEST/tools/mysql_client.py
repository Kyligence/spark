import pymysql

from tools.config import Online


class MysqlClient:
    def __init__(self, config):
        self._host = config.mysql_host
        self._port = config.mysql_port
        self._database = config.mysql_database
        self._user = config.mysql_username
        self._password = config.mysql_password
        self._charset = config.mysql_charset
        self._autocommit = config.mysql_autocommit
        self.client = pymysql.Connect(host=self._host,
                                      port=self._port,
                                      user=self._user,
                                      password=self._password,
                                      database=self._database,
                                      charset=self._charset,
                                      autocommit=self._autocommit
                                      )

        self.cursor = self.client.cursor(pymysql.cursors.DictCursor)

    # # 调用with方法的入口
    # def __enter__(self):
    #     return self
    #
    # # 调用with方法结束时启动
    # def __exit__(self, exc_type, exc_val, exc_tb):
    #     self.client.close()

    def close(self):
        self.cursor.close()
        self.client.close()

    def commit(self):
        self.client.commit()

    def rollback(self):
        self.client.rollback()

    def query_many(self, sql):
        """查询多条数据"""
        try:

            count = self.cursor.execute(sql)
            result = list()
            if count > 0:
                result = self.cursor.fetchall()
            return result
        except Exception as e:
            format_err = f"ERROR - {self._host} query failed: {e} - {sql}" + "\n"
            raise Exception(format_err)

    def query_one(self, sql):
        """查询一条数据"""
        try:

            count = self.cursor.execute(sql)
            result = {}
            if count > 0:
                result = self.cursor.fetchone()
            return result
        except Exception as e:

            format_err = f"ERROR - {self._host} query failed: {e} - {sql}" + "\n"
            raise Exception(format_err)

    def change_one(self, sql: str):
        """执行单条的dml"""
        try:
            rows = self.cursor.execute(sql)
            return rows
        except Exception as err:
            self.client.rollback()
            format_err = f"ERROR - {self._host} change failed: {err} - {sql}" + "\n"
            raise Exception(format_err)

    def change_many(self, sql, values: list):
        try:
            rows = self.cursor.executemany(sql, values)
            return rows
        except Exception as err:
            self.client.rollback()
            format_err = f"ERROR - {self._host} change_many failed: {err} - {sql}" + "\n"
            raise Exception(format_err)


if __name__ == '__main__':
    client=MysqlClient(Online)
    data=client.query_one(sql='select * from params where length(value)=4 and id=34109')
