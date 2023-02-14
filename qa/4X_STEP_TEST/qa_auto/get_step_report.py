from product_api import feishu_api, step_api, aws_api
import time
from decimal import Decimal
import get_jira_id_list

appid = 'bascn2Orx8HaoQjw0Wae9DVIgKf'
table_id_1 = 'tblSsN7wnco17Z9i'
table_id_2 = 'tbl8HnjdnWfkw4b0'
table_id_3 = "tblPsqk5Q38q32rD"
time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
min_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 21 * 24 * 3600))  # ###获取最近21天内创建的测试计划
fail_case_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time() - 7 * 24 * 3600))  # ###获取最近7天内的失败case
plan_execute_nos = []
plan_names_records = {}
fail_case_uuids = {}


def all_feishu_records_get():
    # ###获取飞书记录中的执行明细记录
    records = feishu_api.bitable_get_records(appid, table_id_1)
    try:
        for record in records:
            plan_execute_no = record['fields']['plan_execute_no']
            plan_execute_nos.append(plan_execute_no)
    except TypeError:
        print("no test result records in feishu ")
    except KeyError:
        print("empty test result record in feishu ")
    # ###获取飞书记录中的测试计划汇总记录
    records = feishu_api.bitable_get_records(appid, table_id_2)
    try:
        for record in records:
            plan_name = record['fields']['plan_name']
            record_id = record['record_id']
            plan_names_records[plan_name] = record_id
    except TypeError:
        print("no test plan records in feishu ")
    except KeyError:
        print("empty test plan record in feishu ")
    # ###获取飞书记录中的测试失败case记录
    records = feishu_api.bitable_get_records(appid, table_id_3)
    try:
        for record in records:
            fail_case_id = record['fields']['id']
            record_id = record['record_id']
            fail_case_uuids[fail_case_id] = record_id
    except TypeError:
        print("no failure case records in feishu ")
    except KeyError:
        print("empty failure case record in feishu ")


def plan_filter(plans_info):
    plans_running = []
    for plan in plans_info:
        plan_name = plan['name']
        plan_info = plan_name.split('_')
        if len(plan_info) != 6:
            print(plan_name + "---name format ERROR ,please correct your plan name")
            continue
        if plan_info[1].upper() not in ("GA", "SP", "DAILY", "LTS", "HOTFIX", "AL"):
            print(plan_name + "---name not in (GA DAILY SP LTS HOTFIX AL) ,please correct your plan name")
            continue
        if plan_info[-1] not in ("发布测试", "日报测试", "日常测试"):
            print(plan_name + "---name format ERROR ,please correct your plan name")
            continue
        if plan['create_time'] >= min_time:
            #  ###按时间顺序
            plans_running.insert(0, plan)
    return plans_running


def plan_execute_detail_to_feishu(sa, plan):
    plan_name = plan['name']
    plan_info = plan_name.split('_')
    if len(plan_info) <= 2:
        plan_info = [plan_name, plan_name, plan_name]
    plan_results = sa.test_result_get(plan_name)
    update_flag = 0
    plan_time = 0
    exec_count = 0  ##case运行总次数
    exec_time = 0  # ##case运行时间总和
    failure_reports = {"failure_type_0": 0, "failure_type_1": 0, "failure_type_2": 0, "failure_type_3": 0,
                       "failure_type_4": 0, "failure_type_5": 0, "failure_type_6": 0}
    for result in plan_results:
        failure_report = result["report_detail"]
        for report in failure_report:
            failure_reports["failure_type_" + str(report["case_failure_type"])] = failure_reports["failure_type_" + str(
                report["case_failure_type"])] + 1
        plan_time = plan_time + result['total_time']  ###计算总耗时
        exec_count = exec_count + result['exec_count']  ###计算case运行总次数
        exec_time = exec_time + result['exec_time']  ###计算case运行时间总和
        for key in result:
            result[key] = str(result[key])  ### 为飞书记录接口进行类型转换
        if result['plan_execute_no'] not in plan_execute_nos:
            feishu_api.bitable_add_record(appid, table_id_1, result)  # ###判断测试执行明细记录不存在则录入飞书文档
            update_flag = 1
        if plan_name not in plan_names_records:  # ###测试汇总记录不存在则需要更新
            update_flag = 1
    # ###根据update_flag判断是否更新汇总记录
    if update_flag != 1:
        return False
    else:
        return plan_time, exec_count, exec_time, plan_info, plan_results, failure_reports


def plan_failure_case_to_feishu(sa, plan):
    failure_type_expressions = ["被测服务环境问题","被测服务自身缺陷","被测服务资源问题","脚本问题","雷神缺陷","需求变更","其他","未分类"]
    plan_name = plan['name']
    user_name = step_api.get_user_name_from_id(plan["creator_id"])
    if sa.get_version_info(plan['version_id']):
        version_name = sa.get_version_info(plan['version_id'])['version_info']['name']
        test_plan_batches = sa.test_plan_batches_get(plan_name)
        for batch in test_plan_batches:
            failure_cases = step_api.test_records_get(batch["id"], 3)
            for case in failure_cases:
                if case["complete_time"] <= fail_case_time:
                    continue
                if case['note_id']:
                    note_info = step_api.note_get(case['note_id'])
                else:
                    note_info = {"failure_desc": "None", "failure_type": "-1", "fix_date": "None", "id": "None",
                                 "solution": "None"}
                case_id = case["case_id"]
                case["case_id"] = {"text": str(case_id),
                                   "link": "http://10.1.3.18:5000/en/auspicious/test-info/tests/case/" + str(case_id)}
                case["case_version"] = version_name
                case["plan_name"] = plan_name
                case["failure_desc"] = note_info["failure_desc"]
                case["failure_type"] = failure_type_expressions[int(note_info["failure_type"])]
                case["fix_date"] = note_info["fix_date"]
                case["solution"] = note_info["solution"]
                case["update_time"] = time_now
                case["user_name"] = user_name
                case["product"] =sa.product_name
                for key in case:
                    if key != "case_id":
                        case[key] = str(case[key])
                fail_case_id = case["id"]
                if fail_case_id not in fail_case_uuids:
                    feishu_api.bitable_add_record(appid, table_id_3, case)
                    print(case['name'] + "---find new case , add to feishu  ")
                else:
                    feishu_api.bitable_update_record(appid, table_id_3, fail_case_uuids[fail_case_id], case)
                    print(case['name'] + "---case already exists , update to feishu  ")


def plan_report_to_feishu(sa, plan):
    plan_name = plan['name']
    # ###获取测试计划的jira用例数量信息
    jira_case_total, jira_case_online = get_jira_id_list.test_plan_jira_case_num(plan["version_id"])
    # ###逐个获取测试计划的结果报告、执行报告、合并记录
    try:
        # ###构造测试计划汇总结果字典
        plan_time, exec_count, exec_time, plan_info, plan_results, failure_reports = plan_execute_detail_to_feishu(
            sa, plan)
        version_type = plan_info[-1]
        version_env = plan_info[-2]
        case_unuse = int(plan["num_of_associated"]) - int(plan_results[-1]['case_total'])
        if case_unuse < 0:
            case_unuse = 0
        version_info = {"product": sa.product_name, "failure_type_0": failure_reports["failure_type_0"],
                        "failure_type_1": failure_reports["failure_type_1"],
                        "failure_type_2": failure_reports["failure_type_2"],
                        "failure_type_3": failure_reports["failure_type_3"],
                        "failure_type_4": failure_reports["failure_type_4"],
                        "failure_type_5": failure_reports["failure_type_5"],
                        "failure_type_6": failure_reports["failure_type_6"],
                        "jira_case_total": jira_case_total, "jira_case_online": jira_case_online,
                        "plan_name": plan_name, "version_name": plan_info[2], "version_type": version_type,
                        "version_sort": plan_info[1], "version_env": version_env, "update_time": time_now,
                        "num_of_associated": plan["num_of_associated"],
                        "case_total": plan_results[-1]['case_total'],
                        "completed_num": plan_results[-1]['completed_num'], "case_unuse": case_unuse,
                        "case_failure": int(
                            float(plan_results[0]['failure_rate']) * int(plan_results[0]['case_total'])),
                        "pass_rate": plan_results[-1]['pass_rate'],
                        "first_time_use": Decimal(float(plan_results[-1]['total_time']) / 3600000).quantize(
                            Decimal('0.00')), 'total_time': Decimal(plan_time / 3600000).quantize(Decimal('0.00')),
                        "exec_time": Decimal(exec_time / 3600000).quantize(Decimal('0.00')),
                        "exec_count": exec_count,
                        'report_link': plan_results[0]['report'], "create_time": plan_results[-1]['start_time'],
                        'user_name': step_api.get_user_name_from_id(plan['creator_id'])}
        for key in version_info:
            version_info[key] = str(version_info[key])
        # ###判断测试计划是否存在，存在则更新，不存在则新建
        try:
            record_id = plan_names_records[plan_name]
            print(plan_name + "---find test plan record in feishu ,update plan record ")
            feishu_api.bitable_update_record(appid, table_id_2, record_id, version_info)
        except KeyError:
            print(plan_name + "---no test plan record in feishu ,add new plan record ")
            feishu_api.bitable_add_record(appid, table_id_2, version_info)
    except TypeError:
        print(plan_name + "---no new execute record ,skip update plan record ")


def feishu_to_csv_to_s3(local_path, s3_path, table_id):
    records = feishu_api.bitable_get_records(appid, table_id)
    f = open(local_path, "w", encoding="utf-8")
    first_line = ''
    for key in records[0]["fields"]:
        if first_line != '':
            first_line = first_line + ',' + key.replace(',','|')
        else:
            first_line = first_line + key.replace(',','|')
    f.writelines(first_line.replace('\n','') + '\n')
    for record in records:
        if records.index(record) != 0:
            line = ''
            for key in records[0]["fields"]:
                try:
                    line_content = str(record["fields"][key])
                    if line_content == "None":
                        line_content = '0'
                    if key == 'case_id':
                        line_content = str(record["fields"]['case_id']['text'])
                except KeyError:
                    if key == "note":
                        line_content = "null"
                    else:
                        line_content = '0'
                if line != '':
                    line = line + ',' + line_content.replace(',','|')
                else:
                    line = line + line_content.replace(',','|')
            f.writelines(line.replace('\n','') + '\n')
    f.close()
    aws_api.upload_single_file(local_path, 'QA/' + s3_path + '/' + local_path)


def feishu_report(product_name):
    all_feishu_records_get()
    sa = step_api.StepAPI(product_name)
    plans_info = sa.test_plan_get(100)  # ###获取最近100条记录
    plans_running = plan_filter(plans_info)
    for plan in plans_running:
        # ###获取测试计划的失败用例
        plan_report_to_feishu(sa, plan)
        plan_failure_case_to_feishu(sa, plan)


#  ##获取step记录，写入飞书文档
feishu_report("KE4X")
feishu_report("MDX")
feishu_report("CH")
# ##生成csv上传S3
feishu_to_csv_to_s3('step_report.csv', 'STEP', table_id_2)
feishu_to_csv_to_s3('step_case_report.csv', 'STEP_CASE', table_id_3)
