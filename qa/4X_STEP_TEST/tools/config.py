# -*- coding: utf-8 -*-
import os


class Online:
    # mysql配置
    mysql_host = "10.1.2.100"
    mysql_port = 3306
    mysql_database = "auspicious"
    mysql_username = "step"
    mysql_password = "Step!2022"
    mysql_autocommit = False
    mysql_charset = "utf8"


def global_check():
    if '_global_dict' not in globals():
        global_init()


def global_init():  #初始化
    global _global_dict
    _global_dict = {}


def set_value(key,value):
    """ 定义一个全局变量 """
    _global_dict[key] = value


def get_value(key,defValue=None):
    """ 获得一个全局变量,不存在则返回默认值 """
    try:
        return _global_dict[key]
    except KeyError:
        return defValue


def get_dict():
    return _global_dict


public_repo = '10.1.2.131'

test_pwd = "Aa123456!"
test_project = "learn_kylin"
test_data_path = os.getcwd()+'/../test_data/'

# pro
step_ip = 'http://10.1.3.18:5000'
step_report_ip ='http://10.1.2.105:5000'

# dev
# step_ip = 'http://10.1.3.29:5000'
# step_report_ip ='http://10.1.2.104:5000'

step_user = open(test_data_path+'step/step_info').read().split('||')[0]
step_pwd = open(test_data_path+'step/step_info').read().split('||')[1]
feishu_appid = open(test_data_path+'feishu/feishu_info').read().split('||')[0]
feishu_app_secret = open(test_data_path+'feishu/feishu_info').read().split('||')[1]
feishu_redirect_uri = open(test_data_path+'feishu/feishu_info').read().split('||')[2]
aws_access_key_id = open(test_data_path+'aws/aws_info').read().split('||')[0]
aws_secret_access_key=open(test_data_path+'aws/aws_info').read().split('||')[1]
bucket_name = open(test_data_path+'aws/aws_info').read().split('||')[2]
region_name= open(test_data_path+'aws/aws_info').read().split('||')[3]
global_check()
