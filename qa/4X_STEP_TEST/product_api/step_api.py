import json
import time

import requests
from tools import config

def get_token(step_username,step_password):
    url = config.step_ip + '/api/v1/users/sign-in'
    data = {"username": step_username, "password": step_password}
    res = requests.post(url, json.dumps(data),
                        headers={"Accept": "applinamecation/json, text/plain, */*", "Connection": "keep-alive",
                                 'Content-Type': 'application/json'})
    return res.cookies.values()[0]

def get_header(step_username,step_password):
    headers = {"Accept": "applinamecation/json, text/plain, */*", "Connection": "keep-alive",
               'Content-Type': 'application/json', "token": get_token(step_username,step_password)}
    return headers

def product_line_id_get(product_line_name,headers):
    url = config.step_ip + '/api/v1/product_lines?limit=999999&offset=0'
    res = requests.get(url, headers=headers)
    product_line_lists = json.loads(res.text)['data']['list']
    for product_line_list in product_line_lists:
        if product_line_list['name'] == product_line_name:
            return product_line_list['id']
        else:
            continue


def note_get(note_id,headers):
    url = config.step_ip + '/api/v1/case_record_notes/' + str(note_id)
    res = requests.get(url, headers=headers)
    return json.loads(res.text)['data']


def get_suite_list(project_id,headers):
    url = config.step_ip + '/api/v1/projects/' + str(project_id)
    res = requests.get(url, headers=headers)
    return json.loads(res.text)['data']['suite_list']


def get_case_list(suite_id,headers):
    url = config.step_ip + '/api/v1/suites/' + str(suite_id)
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        print(res.text.encode('utf-8').decode('unicode_escape'))
    return json.loads(res.text)['data']['case_list']


def case_delete(case_id,headers):
    url = config.step_ip + '/api/v1/cases/' + str(case_id)
    res = requests.delete(url, headers=headers)
    return json.loads(res.text)['msg']


def suite_delete(suite_id,headers):
    url = config.step_ip + '/api/v1/suites/' + str(suite_id)
    res = requests.delete(url, headers=headers)
    return json.loads(res.text)['msg']


def project_delete(project_id,headers):
    url = config.step_ip + '/api/v1/projects/' + str(project_id)
    res = requests.delete(url, headers=headers)
    return json.loads(res.text)['msg']


def add_suite(p_id, s_name,headers):
    url = config.step_ip + '/api/v1/suites'
    data = {"project_id": str(p_id), "name": s_name, "run_order": 1}
    res = requests.post(url, data=json.dumps(data), headers=headers)
    if res.status_code != 200:
        print(res.text.encode('utf-8').decode('unicode_escape'))
    else:
        return json.loads(res.text)['data']['id']


def create_case(case_name, suite_id,headers):
    data = {"suite_id": suite_id, "name": case_name, "run_order": 1, "priority": 1, "is_safe": 1}
    url = config.step_ip + '/api/v1/cases'
    res = requests.post(url, data=json.dumps(data), headers=headers)
    if res.status_code != 200:
        print(res.text.encode('utf-8').decode('unicode_escape'))
    return json.loads(res.text)['data']['id']


def create_query_step(case_id,headers, step_name="查询"):
    data = {"case_id": str(case_id), "type": 0, "name": step_name, "run_order": 1, "template_id": 15140}
    url = config.step_ip + '/api/v1/steps'
    res = requests.post(url, data=json.dumps(data), headers=headers)
    if res.status_code != 200:
        print(res.text.encode('utf-8').decode('unicode_escape'))
    return json.loads(res.text)['data']['id']


def create_compare_step(case_id, template_id,headers, step_name="数据处理"):
    data = {"unit_id": str(case_id), "type": 4, "name": step_name, "run_order": 1, "template_id": template_id,"scene":"case"}
    url = config.step_ip + '/api/v1/steps'
    res = requests.post(url, data=json.dumps(data), headers=headers)
    if res.status_code != 200:
        print(res.text.encode('utf-8').decode('unicode_escape'))
    return json.loads(res.text)['data']['id']


def delete_assert(case_id, assert_id,headers):
    url = config.step_ip + '/api/v1/asserts/' + str(assert_id) + '?case_id=' + str(case_id)
    res = requests.delete(url, headers=headers)
    if res.status_code != 200:
        print(res.text.encode('utf-8').decode('unicode_escape'))


def add_assert_for_step(case_id, step_id, a_type, actual_value,headers, expected_value=""):
    data = {"step_id": str(step_id), "unit_id": case_id, "scene": 0, "type": a_type, "actual_value": actual_value,
            "expected_value": expected_value,"unit_scene":"case"}
    url = config.step_ip + '/api/v1/asserts'
    res = requests.post(url, data=json.dumps(data), headers=headers)
    if res.status_code != 200:
        print("add_assert_for_step",res.text.encode('utf-8').decode('unicode_escape'))


def get_suit_id(p_id, s_name,headers):
    url = config.step_ip + '/api/v1/projects/' + str(p_id)
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        print(res.text.encode('utf-8').decode('unicode_escape'))
        return False
    else:
        suite_list = json.loads(res.text)['data']['suite_list']
        suite_id = None
        for suite in suite_list:
            if s_name == suite['name']:
                suite_id = suite['id']
        return suite_id


def get_case_id(s_id, c_name,headers):
    url = config.step_ip + '/api/v1/suites/' + str(s_id)
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        print(res.text.encode('utf-8').decode('unicode_escape'))
        return False
    else:
        case_list = json.loads(res.text)['data']['case_list']
        case_id = None
        for case in case_list:
            if c_name == case['name']:
                case_id = case['id']
        return case_id


def step_delete(case_id, step_id,headers):
    url = config.step_ip + '/api/v1/steps/' + str(step_id) + '?case_id=' + str(case_id)
    res = requests.delete(url, headers=headers)
    if res.status_code != 200:
        print(res.text.encode('utf-8').decode('unicode_escape'))


def case_run(platform_id, case_id,headers):
    url = config.step_ip + '/api/v1/cases/' + str(case_id) + '/run'
    data = {"platform_id": platform_id, "continue_run": "false"}
    res = requests.post(url, data=json.dumps(data), headers=headers)
    if res.status_code != 200:
        print('case_run', res.text.encode('utf-8').decode('unicode_escape'))


def case_run_status(platform_id, case_id,headers):
    url = config.step_ip + '/api/v1/cases/' + str(case_id) + '?platform_id='+str(platform_id)
    res = requests.get(url, headers=headers)
    if res.status_code != 200:
        print('case_run', res.text.encode('utf-8').decode('unicode_escape'))
    else:
        return json.loads(res.text)['data']


def platform_forbid(platform_id,headers,forbid_flag=0):
    url = config.step_ip + '/api/v1/platforms/'+ str(platform_id)
    data = {"id":platform_id,"use_status":forbid_flag}
    res = requests.patch(url,json.dumps(data), headers=headers)
    if res.status_code != 200:
        print('platform_forbid', res.text.encode('utf-8').decode('unicode_escape'))
    else:
        return json.loads(res.text)['data']


def test_records_get(plan_id, run_status,headers):
    url = config.step_ip + '/api/v1/test_plan_batches/' + str(
        plan_id) + '/case_records?offset=0&limit=99999&run_status=' + str(run_status)
    data = {"offset": 0, "limit": 99999, "run_status": run_status}
    res = requests.get(url, json.dumps(data), headers=headers)
    if res.status_code != 200:
        print(res.text.encode('utf-8').decode('unicode_escape'))
    return json.loads(res.text)['data']['list']


def test_report_get(report_url,headers):
    report_url = report_url.replace('en/auspicious/test-result',
                                    'report-api/v1/reports') + '/cases?offset=0&limit=10000'
    res = requests.get(report_url, headers=headers)
    case_failure_list = json.loads(res.text)["data"]["list"]
    return case_failure_list


def get_user_name_from_id(user_id,headers):
    url = config.step_ip + '/api/v1/manager/users?limit=999999&offset=0'
    res = requests.get(url, headers=headers)
    users_list = json.loads(res.text)['data']['list']
    for user_list in users_list:
        if user_id == user_list['id']:
            return user_list['username']
        else:
            continue


def case_param_delete(param_id,headers):
    url = config.step_ip + '/api/v1/params/'+str(param_id)
    res = requests.delete(url, headers=headers)
    if res.status_code != 200:
        print('case_param_delete', res.text.encode('utf-8').decode('unicode_escape'))


def test_plan_batches_get(product_line_id,plan_name,headers, plan_execute_no=''):
    url = config.step_ip + '/api/v1/test_plan_batches?offset=0&limit=100&product_line_id=' + str(
        product_line_id) + '&test_plan_name=' + plan_name
    res = requests.get(url, headers=headers)
    test_plan_batches = json.loads(res.text)['data']['list']
    if plan_execute_no:
        for test_plan_batch in test_plan_batches:
            if test_plan_batch['plan_execute_no'] == str(plan_execute_no):
                return [test_plan_batch]
    else:
        return test_plan_batches


def test_plan_run(plan_id,headers):
    url = config.step_ip + '/api/v1/test_plans/' + str(plan_id) + '/run'
    data = {"is_reset_env":0,"is_clear_data":0}
    res = requests.post(url, data=json.dumps(data), headers=headers)
    if res.status_code != 200:
        print('test_plan_run', res.text.encode('utf-8').decode('unicode_escape'))
    else:
        return json.loads(res.text)['data']['plan_execute_no']


def test_result_get(product_line_id,plan_name,headers,plan_execute_no=''):
    url = config.step_report_ip + '/api/v1/reports?offset=0&limit=100&product_line_id=' + str(
        product_line_id) + '&test_plan_name=' + plan_name
    res = requests.get(url, headers=headers)
    test_results = json.loads(res.text)['data']['list']
    if plan_execute_no:
        for test_result in test_results:
            if test_result['plan_execute_no'] == str(plan_execute_no):
                return [test_result]
    else:
        return test_results


def test_plan_errot_retry(plan_batch_id,headers):
    url = config.step_ip + '/api/v1/test_plan_batches/' + str(plan_batch_id) + '/error_retry'
    data = {"is_reset_env": 0, "is_clear_data": 0}
    res = requests.post(url, data=json.dumps(data), headers=headers)
    if res.status_code != 200:
        print('test_plan_run', res.text.encode('utf-8').decode('unicode_escape'))
    else:
        return json.loads(res.text)['data']['plan_execute_no']


class StepAPI:
    def __init__(self, product_name,headers,version_id='',platform_id = ''):
        self.product_name =product_name
        self.headers = headers
        self.product_line_id = str(product_line_id_get(product_name,headers))
        self.version_id = str(version_id)
        self.platform_id = str(platform_id)


    def version_id_get(self, version_name,headers):
        url = config.step_ip + '/api/v1/versions?limit=999999&offset=0&product_line_id=' + self.product_line_id
        res = requests.get(url, headers=headers)
        versions_lists = json.loads(res.text)['data']['list']
        for versions_list in versions_lists:
            if versions_list['name'] == version_name:
                self.version_id = versions_list['id']
                break
            else:
                continue

    def get_version_info(self, version_id,headers):
        url = config.step_ip + '/api/v1/product_lines/' + self.product_line_id+'?version_id='+str(version_id)
        res = requests.get(url, headers=headers)
        return json.loads(res.text)['data']

    def platforms_id_get(self, platform_name,headers):
        url = config.step_ip + '/api/v1/platforms?limit=999999&offset=0&product_line_id=' + self.product_line_id
        res = requests.get(url, headers=headers)
        platforms_lists = json.loads(res.text)['data']['list']
        for platform_list in platforms_lists:
            if platform_list['name'] == platform_name:
                self.platform_id = platform_list['id']
                break
            else:
                continue

    def platform_info_get(self, platform_id,headers):
        url = config.step_ip + '/api/v1/platforms?limit=999999&offset=0&product_line_id=' + self.product_line_id
        res = requests.get(url, headers=headers)
        platforms_lists = json.loads(res.text)['data']['list']
        for platform_list in platforms_lists:
            if platform_list['id'] == platform_id:
                return platform_list

    def get_project_list(self,headers):
        url = config.step_ip + '/api/v1/versions/' + str(self.version_id)
        res = requests.get(url, headers=headers)
        return json.loads(res.text)['data']['project_list']

    def case_steps_get(self, case_id,headers):
        url = config.step_ip + '/api/v1/cases/' + str(case_id) + '?platform_id=' + str(self.platform_id)
        res = requests.get(url, headers=headers)
        # print(res.text)
        return json.loads(res.text)['data']['case_steps']

    def copy_case(self, case_id, suite_id, case_name,headers):
        url = config.step_ip + '/api/v1/cases/' + str(case_id) + '/copy'
        data = {"product_line_id": self.product_line_id, "platform_id": self.platform_id, "name": case_name,
                "version_id": self.version_id,
                "suite_id": suite_id}
        res = requests.post(url, data=json.dumps(data), headers=headers)
        if res.status_code != 200:
            print('copy_case', res.text.encode('utf-8').decode('unicode_escape'))
        return json.loads(res.text)['data']['id']

    def case_run(self, case_id,headers):
        url = config.step_ip + '/api/v1/cases/' + str(case_id) + '/run'
        data = {"platform_id": self.platform_id, "continue_run": "false"}
        res = requests.post(url, data=json.dumps(data), headers=headers)
        if res.status_code != 200:
            print('case_run', res.text.encode('utf-8').decode('unicode_escape'))

    def add_project(self, p_name,headers):
        url = config.step_ip + '/api/v1/projects'
        data = {"product_line_id": self.product_line_id, "version_id": self.version_id, "name": p_name, "run_order": 1}
        res = requests.post(url, data=json.dumps(data), headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))
        return json.loads(res.text)['data']['id']

    def step_run(self, case_id, step_id,headers):
        data = {"unit_id": str(case_id), "platform_id": self.platform_id,"scene":"case"}
        url = config.step_ip + '/api/v1/steps/' + str(step_id) + '/run'
        res = requests.post(url, data=json.dumps(data), headers=headers)
        if res.status_code != 200:
            print("step_run",url,res.text.encode('utf-8').decode('unicode_escape'))
        url = config.step_ip + '/api/v1/steps/' + str(step_id) + '?scene=case&platform_id='+self.platform_id+'&unit_id='+str(case_id)
        while True:
            time.sleep(30)
            res_status = requests.get(url,  headers=headers)
            if json.loads(res_status.text)['data']['run_status']!=1:
                break
        return json.loads(res.text)['data']['id']

    def add_params_for_step(self, case_id, step_id, param_name,headers):
        data = {"product_line_id": self.product_line_id, "version_id": self.version_id, "platform_id": self.platform_id,
                "type": 2,"id":'',
                "unit_id": case_id,
                "key": param_name, "extract_unit_id": case_id, "extract_step_id": str(step_id),
                "extract_expression": "$",
                "param_type": "extract"}
        url = config.step_ip + '/api/v1/params'
        res = requests.post(url, data=json.dumps(data), headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))
        return json.loads(res.text)['data']['value']

    def get_step_param_value(self,case_id ,step_id,headers):
        url = config.step_ip + '/api/v1/params?version_id=' + str(self.version_id) + '&platform_id=' + str(
            self.platform_id)+ '&case_id=' + str(case_id) + '&step_id=' + str(step_id)
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))
        return json.loads(res.text)['data']['list'][0]['value']

    def param_get_list(self, case_id,headers):
        url = config.step_ip + '/api/v1/params?version_id=' + str(self.version_id) + '&platform_id=' + str(
            self.platform_id) + '&type=2&is_show_all=0&limit=10&offset=0&unit_id=' + str(case_id)+'&case_id=' + str(case_id)
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))
        return json.loads(res.text)['data']['list']

    def add_param_for_case(self, case_id, param_name, value,headers):
        data = {"platform_id": self.platform_id, "product_line_id": self.product_line_id, "id": "",
                "version_id": self.version_id,
                "type": 2, "unit_id": str(case_id),
                "param_type": "normal", "key": param_name, "value": value}
        url = config.step_ip + '/api/v1/params'
        res = requests.post(url, data=json.dumps(data), headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))
        return json.loads(res.text)['data']['value']

    def param_edit_for_case(self, case_id, param_id, value,headers):
        data = {"platform_id": self.platform_id, "product_line_id": self.product_line_id, "version_id": self.version_id,
                "type": 2, "unit_id": str(case_id),
                "param_type": "normal", "key": "exp_body", "value": value}
        url = config.step_ip + '/api/v1/params/' + str(param_id)
        res = requests.patch(url, data=json.dumps(data), headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))
        return json.loads(res.text)['data']['value']

    def edit_query_step(self, client, res_data, step_id, case_id,headers):
        headers_xml = {"Accept": "*/*", "Connection": "Keep-Alive",
                       "SOAPAction": "urn:schemas-microsoft-com:xml-analysis:Execute",
                       "User-Agent": client, "Content-Type": "text/xml", "Authorization": "Basic {admin_auth}",
                       "Accept-Encoding": "gzip, deflate, br"}
        data = {"case_id": case_id, "platform_id": self.platform_id, "address": '',
                "headers": json.dumps(headers_xml),
                "response_type": 1,
                "xml": res_data,
                "url": ' /mdx/xmla/AdventureWorks',
                "url_params": "",
                "response": ""}
        url = config.step_ip + '/api/v1/steps/' + str(step_id)
        res = requests.patch(url, data=json.dumps(data), headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))

    def step_edit_url(self, case_id, step_id,headers, url=''):
        data = {"case_id": case_id, "platform_id": self.platform_id, "address": '',
                "url": url, "url_params": ""}
        url = config.step_ip + '/api/v1/steps/' + str(step_id)
        res = requests.patch(url, data=json.dumps(data), headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))
        return res.text

    def copy_step(self, case_id, step_id,headers):
        data = {"product_line_id": self.product_line_id, "current_version_id": self.version_id,
                "platform_id": self.platform_id,
                "name": "查询历史_cluster获取", "version_id": self.version_id, "unit_id": case_id, "scene": "case"}
        url = config.step_ip + '/api/v1/steps/' + str(step_id) + '/copy'
        res = requests.post(url, data=json.dumps(data), headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))
        return json.loads(res.text)['data']['id']

    def edit_json_step(self, case_id, step_id, name, user,headers,config_type="mdx"):
        headers_json = {"Accept": "application/json, text/plain, */*", "Connection": "Keep-Alive",
                        "User-Agent": "Chrome/102.0.0.0", "Content-Type": "application/json",
                        "Authorization": "Basic {" + user + "_auth}"}
        data = {"unit_id": case_id, "platform_id": self.platform_id, "name": name, "platform_config_type": config_type,
                "headers": json.dumps(headers_json), "scene": "case"}
        url = config.step_ip + '/api/v1/steps/' + str(step_id)
        res = requests.patch(url, data=json.dumps(data), headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))

    def get_project_id(self, p_name,headers):
        url = config.step_ip + '/api/v1/product_lines/' + str(self.product_line_id) + '?version_id=' + str(
            self.version_id)
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))
            return None
        else:
            project_list = json.loads(res.text.encode('utf-8').decode('unicode_escape'))['data']['project_list']
            project_id = None
            for project in project_list:
                if p_name == project['name']:
                    project_id = project['id']
                    break
            return project_id

    def get_step_detail(self, case_id, step_id,headers):
        url = config.step_ip + '/api/v1/steps/' + str(step_id) + '?case_id=' + str(case_id) + '&platform_id=' + str(
            self.platform_id)
        res = requests.get(url, headers=headers)
        return json.loads(res.text)['data']

    def case_edit(self, case_id, case_name,headers):
        url = config.step_ip + '/api/v1/cases/' + str(case_id)
        data = {"platform_id": self.platform_id, "name": case_name, "priority": 0, "use_status": 1}
        res = requests.patch(url, json.dumps(data), headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))

    def test_result_get(self, plan_name,headers):
        url = config.step_report_ip + '/api/v1/reports?offset=0&limit=100&product_line_id=' + str(
            self.product_line_id) + '&test_plan_name=' + plan_name
        res = requests.get(url, headers=headers)
        test_results_o = json.loads(res.text)['data']['list']
        test_results = []
        for test_result in test_results_o:
            if test_result['test_plan_name'] == plan_name and test_result['case_total'] != 0:
                plan_execute_no = test_result['plan_execute_no']
                test_plan_batch = self.test_plan_batches_get(plan_name, plan_execute_no)
                test_result['completed_num'] = test_plan_batch[0]['completed_num']
                test_result['report'] = "http://10.1.3.18:5000/en/auspicious/test-result/" + str(test_result['id'])
                test_result['report_detail'] = test_report_get(test_result['report'])
                case_records_url = "http://10.1.3.18:5000/api/v1/test_plan_batches/" + str(
                    test_plan_batch[0]['id']) + '/case_records?offset=0&limit=10000'
                exec_time = 0
                exec_count = 0
                res = requests.get(case_records_url, headers=headers)
                for exex_case in json.loads(res.text)['data']['list']:
                    exec_time = exec_time + exex_case['run_duration']
                    exec_count = exec_count + 1
                test_result['exec_time'] = exec_time
                test_result['exec_count'] = exec_count
                test_results.append(test_result)
        return test_results

    def test_plan_get(self, limit,headers, plan_name=''):
        url = config.step_ip + '/api/v1/test_plans?offset=0&limit=' + str(limit) + '&product_line_id=' + str(
            self.product_line_id) + "&name=" + plan_name
        res = requests.get(url, headers=headers)
        if res.status_code != 200:
            print(res.text.encode('utf-8').decode('unicode_escape'))
        plans_info = json.loads(res.text)['data']['list']
        return plans_info

    def test_plan_batches_get(self, plan_name,headers, plan_execute_no=''):
        url = config.step_ip + '/api/v1/test_plan_batches?offset=0&limit=100&product_line_id=' + str(
            self.product_line_id) + '&test_plan_name=' + plan_name
        res = requests.get(url, headers=headers)
        test_plan_batches = json.loads(res.text)['data']['list']
        if plan_execute_no:
            for test_plan_batch in test_plan_batches:
                if test_plan_batch['plan_execute_no'] == str(plan_execute_no):
                    return [test_plan_batch]
        else:
            return test_plan_batches


if __name__ == "__main__":
    # sa = StepAPI("KE4X")
    # print(sa.step_password)
    # reports =test_report_get("http://10.1.3.18:5000/en/auspicious/test-result/712")
    # print(reports)
    # url = 'http://10.1.3.18:5000/api/v1/params?version_id=96&platform_id=214&case_id=44862&step_id=178093 '
    # res = requests.get(url, headers=headers)
    # print(res.text)
    # add_assert_for_step('case_id', 'step_id', 'a_type', 'actual_value')
    pass
    # test_result_get("S_SP_4.6.0.1_20221024_AZURE_发布测试")
    # # print(get_project_id('查询测试_MicroStrategy'))
    # # case_id = create_case('function_4', 1810)
    # # step_id = create_query_step(case_id)
    # # query_step_run(case_id, step_id)
    # # add_params_for_step(case_id, step_id)
    # # time.sleep(5)
    # # res_body = get_step_param_value(step_id)
    # # print(res_body)
    # # add_param_for_case(case_id, res_body)
    # # edit_query_step('111', 'xml', step_id, case_id)
    # # step_id = create_compare_step(case_id)
    # # query_step_run(case_id, step_id)
