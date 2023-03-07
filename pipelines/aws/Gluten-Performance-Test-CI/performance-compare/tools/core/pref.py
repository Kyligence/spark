import math
import time

from common import config
from datasource import factory

log = config.log


class Executor(object):
    result = {}

    def __init__(self, engine, conf):
        self.client = factory.get_client(engine)
        self.conf = conf
        self.stmt = self.client.get_stmt()
        self.stmt_sort_by_name = list(self.stmt.keys())
        self.stmt_sort_by_name.sort()
        for name in self.stmt_sort_by_name:
            self.result[name] = []

    def do_execute_query(self):
        for name in self.stmt_sort_by_name:
            print("Run iterations {}".format(config.RUN_ITERATION))
            print("Run sql {}".format(name))
            self.result[name].append(self.query(self.stmt[name]))

    def query(self, query):
        query_id = str(time.time())
        request_meta = {
            "query_id": query_id,
            "start_time": time.time(),
            "exception": None,
            "profile": {
                'client_time': 0, 'server_time': 0, 'memory_usage': 0
            }
        }

        start_perf_counter = time.perf_counter()
        res = []
        try:
            if config.COMPARE_RESULT:
                res = self.client.execute_and_fetchall(query.statement)
            else:
                self.client.execute(query.statement)
        except Exception as e:
            log.error(e)
            request_meta["exception"] = Exception('query_name: {}, query_id {}:'.format(query.name, query_id), e)

        client_time = int((time.perf_counter() - start_perf_counter) * 1000)  # ms

        if config.COMPARE_RESULT and query.result is not None:
            try:
                result_without_none = []
                for row in res:
                    column_without_none = []
                    for column in row:
                        if column is None:
                            column = "NULL"
                        column_without_none.append(column)
                    result_without_none.append(column_without_none)

                compare_result(query.result, result_without_none, query.order)
            except Exception as e:
                request_meta["exception"] = Exception('query_name: {}, query_id {}:'.format(query.name, query_id), e)

        request_meta["profile"]["client_time"] = client_time
        request_meta["profile"]["server_time"] = client_time
        return request_meta


def compare_result(expected_answer, sql_answer, order=False):
    expected_len = len(expected_answer)
    sql_len = len(sql_answer)
    if expected_len != sql_len:
        raise Exception('Expected row count {} but return {}'.format(expected_len, sql_len))

    if expected_len == 0:
        return

    expected_column_len = len(expected_answer[0])
    sql_column_len = len(sql_answer[0])

    if expected_column_len != sql_column_len:
        raise Exception('Expected column count {} but return {}'.format(expected_column_len, sql_column_len))

    if order:
        expected_answer.sort()
        sql_answer.sort()

    for row in range(0, expected_len):
        for column in range(0, expected_column_len):
            expected_column = expected_answer[row][column]
            sql_column = sql_answer[row][column]

            if not check_eval(expected_column, sql_column):
                raise Exception(
                    'Expected row {} column {} value {} but return {}'.format(row, column,
                                                                              expected_answer[row][column],
                                                                              sql_answer[row][column]))


def check_eval(e1, e2):
    if is_null(e1) and is_null(e2):
        return True

    if e1.startswith("'") and e1.endswith("'"):
        e1 = e1[1:-1]

    if e1 == e2:
        return True

    if str(e1) == str(e2):
        return True

    try:
        return eval(e1) == eval(str(e2)) or math.fabs(eval(e1) - eval(str(e2))) / eval(e1) < 0.00001
    except Exception as e:
        return False


def is_null(v):
    if v is None:
        return True

    if v.startswith("'") and v.endswith("'"):
        v = v[1:-1]

    return v == 'NULL' or v == 'null'
