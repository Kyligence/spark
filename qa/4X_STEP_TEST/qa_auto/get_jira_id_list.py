import re
from tools.config import Online
from tools.mysql_client import MysqlClient


def get_jira_id_list(case_list):
    all_jira_set = set()
    for case in case_list:
        name = case.get("name")
        results = re.findall("\d{4,16}", name)
        for item in results:
            all_jira_set.add(item)
    return all_jira_set


def test_plan_jira_case_num(version_id):
    jira = JiraIdList(config=Online)
    case_all_list = jira.get_all_case_by_version_id(version_id)
    all_jira_id_list = get_jira_id_list(case_all_list)
    case_online_list = jira.get_online_case_by_version_id(version_id)
    online_jira_id_list = get_jira_id_list(case_online_list)
    return len(all_jira_id_list),len(online_jira_id_list)


class JiraIdList:
    def __init__(self, config):
        self.mysql_client = MysqlClient(config=config)

    def get_all_case_by_version_id(self, version_id):
        sql = f"""
            select id, name
            from `case`
            where delete_status = 0
              and id in (select case_id from case_relation where product_line_version_id = {version_id});
        """
        # delte 删除标志0 未删除，use 0禁用
        return self.mysql_client.query_many(sql=sql)

    def get_online_case_by_version_id(self, version_id):
        sql = f"""
            select id, name
            from `case`
            where delete_status = 0
              and use_status = 1
              and id in (select case_id from case_relation where product_line_version_id = {version_id});
        """
        # delte 删除标志0 未删除，use 0禁用
        return self.mysql_client.query_many(sql=sql)


if __name__ == '__main__':
    pass
    print(test_plan_jira_case_num(92))