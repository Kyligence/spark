import time, sys
import urllib3
from tools import common
import requests, json
from product_api import step_api
from requests.auth import HTTPBasicAuth
import jenkins
import re

# remote_ip = '10.0.0.28'
#在郭守敬本地运行需要使用外网ip
#SP包获取服务器
remote_ip = '10.0.0.32'
#GA包获取服务器
remote_ip1 = '10.0.0.28'
remote_ip2 = '10.1.8.217'
linux_user = '4xuser'
liunx_pwd = 'KyLin@QA_2022'
remote_path = '/home/4xuser/data/devops/'
des_path = '/mnt/jenkins_azure/step/'
des_path2 = '/mnt/jenkins/step/'


header = {'Accept': 'application/vnd.apache.kylin-v4-public+json', 'Accept-Language': 'en',
          'Content-Type': 'application/json;charset=utf-8'}


def get_ke_version(plan_name):
    plan_info = plan_name.split('_')
    ke_version = plan_info[1]
    return ke_version

def get_ke_tar(plan_name, tar_type,ke_version):
    plan_info = plan_name.split('_')
    commands = ['ls /home/4xuser/data/devops/*' + plan_info[2] + '-' + tar_type + '*tar.gz -ltrh']
    #SP版本去165拿包，其余版本去141拿包
    print("版本为：", ke_version)
    if ke_version == "SP":
        ke_tar_list = common.cmds_exec(commands, remote_ip, linux_user, liunx_pwd).split('\n')
    else:
        ke_tar_list = common.cmds_exec(commands, remote_ip1, linux_user, liunx_pwd).split('\n')
    try:
        ke_tar_name = ke_tar_list[-2].split('/')[-1]
    except IndexError:
        ke_tar_name = ''
    return ke_tar_name

def copy_tar_to_jenkins(ke_tar_name,ke_version):
    command = 'cp ' + remote_path + ke_tar_name + ' ' + des_path
    command2 = 'ls /mnt/jenkins_azure/step/' + ke_tar_name + ' -ltrh'
    # SP版本去165拿包，其余版本去141拿包
    if ke_version == "SP":
        common.cmds_exec([command], remote_ip, linux_user, liunx_pwd)
    else:
        common.cmds_exec([command], remote_ip1, linux_user, liunx_pwd)
    feishu_robot_card("KE安装包准备", '**复制KE安装包命令已发送：**\n' + ke_tar_name + ' 到 ' + des_path)
    # 等待CP命令完成，等待10秒
    time.sleep(10)
    check_cp_command_result = common.cmds_exec([command2], remote_ip, linux_user, liunx_pwd)
    try:
        check_cp_resault = check_cp_command_result[-2].split('/')[-1]
        feishu_robot_card("KE安装包准备", '**复制KE安装包成功：**\n' + ke_tar_name + ' 到 ' + des_path)
    except IndexError:
        check_cp_resault = ''
        feishu_robot_card("KE安装包准备", " **复制KE安装包失败，需人工介入检查", 'red')
    # command = 'cp ' + des_path + ke_tar_name + ' ' + des_path2
    # common.cmds_exec([command], remote_ip2, linux_user, liunx_pwd)
    # feishu_robot_card("KE安装包准备", '**复制KE安装包：**\n' + ke_tar_name + ' 到 ' + des_path2)
    return check_cp_resault

def ke_deploy(jenkins_job, ke_tar_name):
    server = jenkins.Jenkins('http://10.1.2.38:8080', username='quard', password='quard@2020')
    server.build_job(jenkins_job, {'ke_package': des_path2 + ke_tar_name})
    feishu_robot_card("KE部署任务", f'**获取到KE安装包：**： {ke_tar_name} \n触发Jenkins任务：{jenkins_job}')

def deploy_check(all_ke_clusters):
    cluster_foribid = []
    # time.sleep(900)
    time.sleep(10)
    ke_deploy_done = 0
    j = 0
    for platform_id in all_ke_clusters:
        ke_ip = all_ke_clusters[platform_id]
        # for i in range(j, 25):
        for i in range(j, 1):
            status_code, res_text = user_login(ke_ip)
            if status_code != 200:
                ke_deploy_done = 1
                time.sleep(60)
                # time.sleep(1)
                continue
            else:
                ke_deploy_done = 0
                feishu_robot_card("KE安装部署检查", ke_ip + ' KE状态检查通过')
                break
        if ke_deploy_done == 1:
            feishu_robot_card("KE安装部署检查", '**KE节点: **' + ke_ip + '  --安装启动超时', "red", ke_ip)
            cluster_foribid.append(platform_id)
        j = j + 1
    for cluster in cluster_foribid:
        all_ke_clusters.pop(cluster)
        step_api.platform_forbid(cluster,header)
        feishu_robot_card("STEP节点禁用", '**KE节点: **' + cluster + '  --已禁用', 'grey', cluster)

def user_login(cluster):
    url = cluster + '/kylin/api/user/authentication'
    data = {}
    data = json.dumps(data)
    try:
        res = requests.post(url, auth=HTTPBasicAuth(username="ADMIN", password="KYLIN"), headers=header, data=data)
        return res.status_code, res.text
    except requests.exceptions.ConnectionError:
        return 500, "requests.exceptions.ConnectionError"
    except urllib3.exceptions.MaxRetryError:
        return 500, "urllib3.exceptions.MaxRetryError"
    except ConnectionRefusedError:
        return 500, "ConnectionRefusedError"
    except urllib3.exceptions.NewConnectionError:
        return 500, "urllib3.exceptions.NewConnectionError"


def feishu_robot_card(title, card_content, card_template='green', card_url=''):
    # print(f"feishu_robot_card::::>> title->{title}, card_content->{card_content} ")
    # return 
    if card_url:
        card_url_content = "链接：<a>" + card_url + "</a>"
    else:
        card_url_content = card_url
    text = {
        "header": {
            "title": {
                "tag": "plain_text",
                "content": title
            },
            "template": card_template
        },
        "elements": [{
            "tag": "div",
            "text": {
                "tag": "lark_md",
                "content": card_content
            }
        },
            {
                "tag": "hr"
            },
            {
                "tag": "div",
                "text": {
                    "tag": "lark_md",
                    "content": card_url_content
                }
            }
        ]
    }
    url = "https://open.feishu.cn/open-apis/bot/v2/hook/3ffc7780-5cde-4400-b7d3-24e61205dc5d"

    payload_message = {
        "card": text,
        "msg_type": "interactive"
    }
    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.request("POST", url, headers=headers, data=json.dumps(payload_message))
    return response


def get_plan_info(sa, plan_name, header):
    plan_info = sa.test_plan_get(10,header,plan_name)  # ### 获取测试计划所在KE平台信息
    if len(plan_info) == 0:
        result = False
        return None, None, None, result
    else:
        result = True
    plan_id = plan_info[0]['id']
    product_line_id = plan_info[0]['product_line_id']
    return plan_info, plan_id, product_line_id, result


def get_setup_case(sa, plan_info,header,step_plan_type):
    case_list = []  # 存放初始化case的case_id
    version_id = plan_info[0]['version_id']
    version_info = sa.get_version_info(version_id,header)['project_list'][0]  # ###获取setup用例（默认是第一个套件下的用例）
    project_id = version_info['id']
    if step_plan_type == 'CH_HA':
        suite_info_tmp = step_api.get_suite_list(project_id, header)[10]
        suite_info = [suite_info_tmp.get('id')]
        for suite in suite_info:
            suite_id = suite
            case_info = step_api.get_case_list(suite_id, header)
            for case in case_info:
                if case['name'] == 'HA用例执行的前置操作':
                    case_list.append(case['id'])
    elif step_plan_type == 'CH_not_HA':
        suite_info_tmp = step_api.get_suite_list(project_id, header)[10]
        suite_info = [suite_info_tmp.get('id')]
        for suite in suite_info:
            suite_id = suite
            case_info = step_api.get_case_list(suite_id, header)
            for case in case_info:
                if case['name'] == '非HA用例执行的前置操作':
                    case_list.append(case['id'])
    else:
        suite_info = step_api.get_suite_list(project_id, header)
        for suite in suite_info:
            suite_id = suite['id']
            case_info = step_api.get_case_list(suite_id, header)
            for case in case_info:
                case_list.append(case['id'])

    return case_list


def get_platform(sa, plan_info,header):
    all_ke_clusters = {}
    all_platform_id = plan_info[0]['platform_id']
    platform_info = sa.platform_info_get(all_platform_id,header)
    all_platform_name = platform_info['name']
    for cluster in platform_info["children"]:  # ### 检查KE状态、版本，同步飞书消息
        cluster_ip = cluster['address']
        platform_id = cluster['id']
        if cluster['use_status'] == 1:
            all_ke_clusters[platform_id] = cluster_ip
    return all_platform_name,all_ke_clusters


def setup_ke_clusters(all_ke_clusters,case_list,header):
    """执行环境清理，并同步飞书消息"""
    cluster_status = -1
    for platform_id in all_ke_clusters:
        case_status = 0
        for case_id in case_list:
            run_status = run_setup_case(platform_id, case_id,all_ke_clusters,header)
            if run_status == 3:
                run_status = run_setup_case(platform_id, case_id,all_ke_clusters,header)
            if run_status != 2:
                step_api.platform_forbid(platform_id,header)
                feishu_robot_card("STEP节点禁用", f'**KE节点:** {all_ke_clusters[platform_id]}  已禁用', 'grey',all_ke_clusters[platform_id])
                case_status = -1
        if case_status == 0:
            cluster_status= 0
    return cluster_status


def run_setup_case(platform_id, case_id,all_ke_clusters,header):
    step_api.case_run(platform_id, case_id,header)
    run_status = -1
    for i in range(0, 40):
        time.sleep(30)
        case_run_status = step_api.case_run_status(platform_id, case_id,header)
        run_status = case_run_status['run_status']
        if run_status == 2:
            feishu_robot_card("环境初始化", all_ke_clusters[platform_id] + ' 环境清理成功', "green", all_ke_clusters[platform_id])
            break
        elif run_status == 3:
            feishu_robot_card("环境初始化", all_ke_clusters[platform_id] + ' 环境清理失败', "red", all_ke_clusters[platform_id])
            break
        elif run_status == 1:
            continue
        else:
            feishu_robot_card("环境初始化", all_ke_clusters[platform_id] + ' 环境清理运行异常，请手动执行', "red",
                              all_ke_clusters[platform_id])
            break
    if run_status == -1:
        feishu_robot_card("环境初始化", all_ke_clusters[platform_id] + ' 环境清理执行超时，请检查', "red", all_ke_clusters[platform_id])
    return run_status


def check_plan_running_status(product_line_id, plan_name, plan_excute_no, all_ke_clusters,case_list,check_time,header,step_ip="10.1.3.18"):
    while True:
        time.sleep(check_time)
        test_plan_batch = step_api.test_plan_batches_get(product_line_id, plan_name,header, plan_excute_no)[0]
        batch_status = test_plan_batch['status']
        batch_report_id = test_plan_batch['id']
        failure_num = len(step_api.test_records_get(batch_report_id, 3,header))
        batch_report_url = f"http://{step_ip}:5000/en/auspicious/test-execution/{batch_report_id}"
        batch_content = f"**总用例数目:** {test_plan_batch['case_num']} ,**已完成用例数目:** {test_plan_batch['completed_num']},**失败用例数目:** {failure_num}"
        if batch_status == 'running':
            feishu_robot_card(plan_name, batch_content + '\n**测试计划执行中**', "green", batch_report_url)
            continue
        elif batch_status == 'completed':
            feishu_robot_card(plan_name, batch_content + '\n**测试计划执行结束**', "green", batch_report_url)
            break
        else:
            feishu_robot_card(plan_name, batch_content + '\n**测试计划被取消或状态异常,请尝试重新启动jenkins任务**', 'red', batch_report_url)
            break
    test_result = step_api.test_result_get(product_line_id, plan_name,header, plan_excute_no)
    pass_rate = test_result[0]['pass_rate']
    if pass_rate == 1:
        return 'done'
    elif batch_status == 'completed':
        feishu_robot_card("环境初始化", " 初始化各个KE节点开始")
        cluster_status = setup_ke_clusters(all_ke_clusters,case_list,header)
        if cluster_status == 0:
            feishu_robot_card("环境初始化", " 初始化结束 ")
            plan_excute_status = 'continue'
        else:
            feishu_robot_card("环境初始化", " **所有KE节点均初始化失败，请检查**", 'red')
            plan_excute_status = 'retry'
        return plan_excute_status
    else:
        return 'retry'

def create_plan(plan_name, product_line_id, platform_id, estimated_execution_time, error_retry, timeout_to_stop, version_name, result_receiver_ids, headers):
    feishu_robot_card("创建计划", f"参数：{plan_name},{product_line_id},{platform_id},{estimated_execution_time},{error_retry},{timeout_to_stop},{version_name},{result_receiver_ids},{headers}")
    status_code, result = step_api.query_version_id_by_name(product_line_id, version_name, headers)
    feishu_robot_card("查询版本的结果",f"{status_code},{result}")
    if status_code != 200:
        return status_code, result

    if not result:
        feishu_robot_card("查询版本为空，请确认产品线号和版本名是否匹配", f"result：{result}")
        return 500, result

    version_id = result[0]['id']

    status_code, result = step_api.create_plan(plan_name, product_line_id, platform_id, estimated_execution_time, error_retry, timeout_to_stop, version_id, result_receiver_ids, headers)
    feishu_robot_card("创建计划的结果", f"{status_code},{result}")
    return status_code, result

def link_plan_and_cases(plan_name, plan_id, product_line_id, headers):
    if product_line_id == 146:#ke4x,关联全部用例
        status_code,msg = step_api.link_ke4x_plan_and_cases(plan_id, product_line_id, headers)
        return status_code, msg

    elif product_line_id == 149:#CH,
        #CH-HA：标签选 用例类型：HA，排除（是个bug，实际效果是包含），点查询，点按条件关联
        #CH-非HA：标签选 用例类型：非HA，排除（是个bug，实际效果是包含），点查询，点按条件关联
        if re.match('\S*非HA\S*', plan_name) is not None:
            status_code,msg = step_api.link_CH_not_HA_plan_and_cases(plan_id, product_line_id, headers)
            return status_code, msg

        if re.match('\S*HA\S*', plan_name) is not None:
            status_code,msg = step_api.link_CH_HA_plan_and_cases(plan_id, product_line_id, headers)
            return status_code, msg
        else:
            return 500, 'CH计划名称错误，需包含HA或非HA' 

    else:
        return 500, '产品线id有误，只支持KE4X和CH产品线'

    

def jenkins_job(product_line_name, version_name, plan_name, tar_type, step_username, step_password, check_time=600, step_ip="10.1.3.18"):
    feishu_robot_card(plan_name, f"**测试计划：** {plan_name}\n**类型为：** {tar_type}\n其他参数为{product_line_name}{version_name}{plan_name}{step_username}{check_time}{step_ip}")

    ''' 查找KE包，复制KE包
    ke_version = get_ke_version(plan_name)
    while True:
        ke_tar_name = get_ke_tar(plan_name, tar_type,ke_version)
        print("查询包结果为：", ke_tar_name)
        if ke_tar_name == '':
            feishu_robot_card("KE安装包准备", '**未获取到KE安装包,等待5分钟**')
            time.sleep(300)
            continue
        else:
            feishu_robot_card("KE安装包准备", f'**获取到KE安装包：**  {ke_tar_name}')
            check_cp_resault = copy_tar_to_jenkins(ke_tar_name,ke_version)
            if check_cp_resault == '':
                feishu_robot_card("KE安装包准备", " **复制KE安装包失败，任务终止**", 'red')
                exit(1)
            else:
                break
    '''

    header = step_api.get_header(step_username,step_password)
    print(f"jenkins_job header:{header}")

    if 'CH.HA' in plan_name:
        step_plan_type = 'CH_HA'
        sa = step_api.StepAPI("CH", header)
    elif 'CH.非HA' in plan_name:
        step_plan_type = 'CH_not_HA'
        sa = step_api.StepAPI("CH", header)
    else:
        step_plan_type = 'KE4X'
        sa = step_api.StepAPI("KE4X", header)

    plan_info, plan_id, product_line_id, result = get_plan_info(sa, plan_name,header)
    if not result:
        # 如果找不到计划，则新建计划
        platform_id = 0
        if step_plan_type == 'KE4X':
            platform_id = 174
            product_line_id = 146
        elif step_plan_type == 'CH_HA':
            platform_id = 181
            product_line_id = 149
        elif step_plan_type == 'CH_not_HA':
            platform_id = 158
            product_line_id = 149
        else:
            feishu_robot_card("创建计划失败", "计划不含类型信息。计划名：" + plan_name)
            return
        estimated_execution_time = 20
        error_retry = 0
        timeout_to_stop = 1
        result_receiver_ids = [44]

        create_plan_status_code, result = create_plan(plan_name, product_line_id, platform_id, estimated_execution_time, error_retry, timeout_to_stop, version_name, result_receiver_ids, header)
        if create_plan_status_code!= 200:
            feishu_robot_card("创建计划失败", result)
            return
        else:
            feishu_robot_card("创建计划成功", result)
    # 关联计划和case
    plan_info, plan_id, product_line_id, result = get_plan_info(sa, plan_name, header)
    link_result_status_code, msg = link_plan_and_cases(plan_name, plan_id, product_line_id,
                                                       header)  # KE4X:146, Ch:149
    if link_result_status_code != 200:
        feishu_robot_card("关联计划和用例失败", "**" + msg + "**")
        return
    feishu_robot_card("关联计划和用例成功", msg)
    all_platform_name,all_ke_clusters = get_platform(sa, plan_info,header)
    # 部署KE
    # ke_deploy(all_platform_name, ke_tar_name)
    # feishu_robot_card("KE部署任务", all_platform_name + " Jenkins任务执行结束，\n开始检测KE部署情况")

    deploy_check(all_ke_clusters)
    feishu_robot_card(plan_name, " **测试计划开始执行,获取初始化用例**")
    get_setup_case(sa, plan_info,header,step_plan_type)
    feishu_robot_card("环境初始化", " **初始化各个KE节点开始**")
    case_list = get_setup_case(sa, plan_info,header,step_plan_type)
    cluster_status = setup_ke_clusters(all_ke_clusters,case_list,header)
    if cluster_status==0:
        feishu_robot_card("环境初始化", f" **环境初始化结束 {plan_name} 开始执行**")
        plan_run_status_code, plan_run_result = step_api.test_plan_run(plan_id,header)
        if plan_run_status_code != 200:
            feishu_robot_card("计划运行失败", f" **计划运行失败： {plan_run_result} **")
            return
        else:
            plan_excute_no = plan_run_result
        plan_excute_status = check_plan_running_status(product_line_id, plan_name, plan_excute_no, all_ke_clusters,case_list, check_time,header,step_ip)
        for i in range(0, 3):
            if plan_excute_status == 'retry':
                break
            elif plan_excute_status == 'done':
                break
            else:
                feishu_robot_card(plan_name, '测试计划重试第' + str(i + 1) + '次')
                test_plan_batch = step_api.test_plan_batches_get(product_line_id, plan_name, plan_excute_no,header)[0]
                plan_batch_id = test_plan_batch['id']
                plan_excute_no = step_api.test_plan_errot_retry(plan_batch_id,header)
                # check_time = 900
                plan_excute_status = check_plan_running_status(product_line_id, plan_name, plan_excute_no, all_ke_clusters,case_list,check_time,header,step_ip)
    else:
        feishu_robot_card("环境初始化", " **所有KE节点均初始化失败，请检查**",'red')
        pass


def jenkins_job_continue(plan_name, plan_excute_no , step_username, step_password ,check_time=3600, step_ip="10.1.3.18"):
    feishu_robot_card(plan_name, f"**测试计划监测重启：** {plan_name}\n ")
    header = step_api.get_header(step_username, step_password)
    if 'CH.HA' in plan_name:
        step_plan_type = 'CH_HA'
        sa = step_api.StepAPI("CH", header)
    elif 'CH.非HA' in plan_name:
        step_plan_type = 'CH_not_HA'
        sa = step_api.StepAPI("CH", header)
    else:
        step_plan_type = 'KE4X'
        sa = step_api.StepAPI("KE4X", header)
    plan_info, plan_id, product_line_id, result = get_plan_info(sa, plan_name,header)
    all_platform_name,all_ke_clusters = get_platform(sa, plan_info,header)
    case_list = get_setup_case(sa,plan_info,header,step_plan_type)
    plan_excute_status = check_plan_running_status(product_line_id, plan_name, plan_excute_no, all_ke_clusters,case_list, check_time,header,step_ip)
    for i in range(0, 3):
        if plan_excute_status == 'retry':
            break
        elif plan_excute_status == 'done':
            break
        else:
            feishu_robot_card(plan_name, '测试计划重试第' + str(i + 1) + '次')
            test_plan_batch = step_api.test_plan_batches_get(product_line_id, plan_name,header, plan_excute_no)[0]
            plan_batch_id = test_plan_batch['id']
            plan_excute_no = step_api.test_plan_errot_retry(plan_batch_id,header)
            #重试的时候900秒通知一次
            check_time = 900
            plan_excute_status = check_plan_running_status(product_line_id, plan_name, plan_excute_no, all_ke_clusters,case_list,check_time,header,step_ip)

def job_run():
    try:
        plan_type = sys.argv[1]
        if plan_type=="first":
            plan_name,tar_type, product_line_name, version_name,check_time,step_username,step_password,step_ip =sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6],sys.argv[7],sys.argv[8],sys.argv[9]
            jenkins_job(product_line_name, version_name, plan_name,tar_type,step_username,step_password,int(check_time),step_ip)
        else:
            plan_name,plan_excute_no,product_line_name, version_name, check_time,step_username,step_password,step_ip  =sys.argv[2],sys.argv[3],sys.argv[4],sys.argv[5],sys.argv[6],sys.argv[7],sys.argv[8],sys.argv[9]
            jenkins_job_continue(plan_name,plan_excute_no,step_username,step_password,int(check_time),step_ip)
    except IndexError:
        feishu_robot_card("jenkins 任务", '**参数输入异常，缺少参数**')
        raise IndexError()

job_run()


#if __name__ == '__main__':
#    jenkins_job('KE4X',  'default version', 'CH.非HA_GA_4.6.6.0_20230306_AZURE_脚本测试_forCreatePlan', None,  'lianfei.qu@kyligence.io', 'xxxxxx', 600, '10.1.3.29')
#
# jenkins_job_continue('S_Daily_4.6.2.0_20221116_AZURE_日报测试','202211241443394129272875',600)
# # jenkins_job()
# jenkins_job('S_GA_4.6.4.0_0109_AZURE_脚本测试', 'QA', 600)
# jenkins_job('S_GA_4.6.4.0_0109_AZURE_脚本测试', 'QA', 'Devops_user@kyligence.io', 'xxx', 5)
# jenkins_job('CH.HA_GA_4.6.5.0_20230210_AZURE_脚本测试', 'RC', 'Devops_user@kyligence.io', 'xxx', 5)
# jenkins_job('CH.非HA_GA_4.6.5.0_20230210_AZURE_脚本测试', 'RC', 'Devops_user@kyligence.io', 'xxx', 5)