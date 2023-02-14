# -*- coding: utf-8 -*-

import base64
import json
import os,logging,sys
import paramiko
import requests
from requests.auth import HTTPBasicAuth
from tools import config
logging.basicConfig(level=logging.INFO,format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
logging.getLogger("paramiko").setLevel(logging.WARNING)
logging.getLogger("charset_normalizer").setLevel(logging.ERROR)

env_list = {'mdx_ip': '10.1.5.100', 'mdx_port': '7080', 'ke4_ip': '10.1.2.127', 'ke4_port': '7090', 'mdx_user': 'ADMIN', 'mdx_password': 'KYLIN@123','mdx_path':'MDX_14X'}


def set_value_from_argv():
    print('运行环境初始化配置开始')
    for key in env_list:
        argv_id = list(env_list.keys()).index(key)+1
        try:
            if '/' not in sys.argv[argv_id] or 'mdx' in sys.argv[argv_id]:
                config.set_value(key, sys.argv[argv_id])
                env_list[key]=sys.argv[argv_id]
                mdx_logger("根据外部参数配置："+str(key) +' ，值为：' +str(sys.argv[argv_id]))
        except IndexError:
            mdx_logger("无外部参数："+str(key))
    for key in env_list:
        if not config.get_value(key):
            config.set_value(key, env_list[key])
        else:
            env_list[key] = config.get_value(key)
    print("环境信息：", env_list)


def mdx_logger(logging_content):
    logging.info(logging_content+'\n')


def get_basic_auth_str(username, password):
    temp_str = username + ':' + password
# 转成bytes string
    bytesString = temp_str.encode(encoding="utf-8")
# base64 编码
    encodestr = base64.b64encode(bytesString)
# 解码
    decodestr = base64.b64decode(encodestr)
    return encodestr,decodestr


def url_mdx(url_str):
    ip = config.get_value('mdx_ip')
    port = config.get_value('mdx_port')
    url = 'http://'+ ip + ':'+port+url_str
    return url


def url_kc(url_str):
    ip = config.get_value('kc_ip')
    port = config.get_value('kc_port')
    url = 'http://' + ip + ':' + port + url_str
    return url


def url_ke4(url_str):
    ip = config.get_value('ke4_ip')
    port = config.get_value('ke4_port')
    url = 'http://'+ip + ':'+port+url_str
    return url


def str_contain(json_data,contain_str):
    tbj = json.dumps(json_data)
    if contain_str in tbj:
        print('target contain ' + contain_str)
        return True
    else:
        return False


def get_jsondata(json_data,keyname):
    data = json.loads(json_data)[keyname]
    # print(data)
    return data


def sftp_upload(local,remote,ip,username='root', password='hadoop'):
    sf = paramiko.Transport(ip,22)
    sf.connect(username=username,password=password)
    sftp = paramiko.SFTPClient.from_transport(sf)
    if os.path.isdir(local):  # 判断本地参数是目录还是文件
        for f in os.listdir(local):
            # 遍历本地目录
            sftp.put(os.path.join(local+f),os.path.join(remote+f))
            # 上传目录中的文件
    else:
        sftp.put(local,remote)
    # 上传文件
    sf.close()


def cmds_exec(commands,ip,username='root', password='hadoop'):
    client = paramiko.SSHClient()
    # 自动添加策略，保存服务器的主机名和密钥信息
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    # 连接服务器
    try:
        client.connect(ip, 22, username, password, compress=True,timeout=30,)
        # 执行linux命令
        newcmds = ''
        for command in commands:
            newcmds = newcmds + command + ';'
        # print(newcmds)
        stdin, stdout, stderr = client.exec_command(newcmds)
        stdout_content = ''
        for line in stdout.readlines():
            stdout_content = stdout_content + line
        # print(stdout_content)
        client.close()
    except OSError:
        print('Network is unreachable')
        stdout_content = 'Network is unreachable'
    return stdout_content


def basic_auth(user,pwd):
    return HTTPBasicAuth(user, pwd)


def session_header(user,pwd):
    url_str = '/api/login'
    url = url_mdx(url_str)
    auth = HTTPBasicAuth(user, pwd)
    res = requests.get(url, auth=auth)
    session = res.cookies.values()
    try:
        header = {"Content-Type": "application/json", "Cookie": "mdx_session=" + session[0]}
    except IndexError:
        header = {"Content-Type": "application/json"}
    return header


def get_log_content(logname):
    if logname in os.listdir('log'):
        file = open('log/'+logname, 'r')
        content = file.read()
    else:
        print('No file in log dir ')
        content = ''
    return content


def remote_scp(host_ip, remote_path, local_path, username='root', password='hadoop'):
    t = paramiko.Transport((host_ip, 22))
    t.banner_timeout=30
    t.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(t)
    src = remote_path
    des = local_path
    sftp.get(src, des)
    t.close()


def get_common_dataset():
    f = open(config.test_data_path+'datasetzip/learn_kylin_template.json', 'r', encoding='utf-8')
    common_dataset = f.read()
    f.close()
    return common_dataset