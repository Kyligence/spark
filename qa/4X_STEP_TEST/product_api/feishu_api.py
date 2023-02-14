import requests
import json
from tools import config
import urllib
from urllib import parse

appid = config.feishu_appid
app_secret = config.feishu_app_secret
redirect_uri = urllib.parse.quote(config.feishu_redirect_uri.encode('gb2312'))


def bitable_add_record(app_id, table_id, fields):
    url = "https://open.feishu.cn/open-apis/bitable/v1/apps/" + app_id + "/tables/" + table_id + "/records"
    payload = json.dumps({
        "fields": fields
    })

    response = requests.request("POST", url, headers=header_get(), data=payload)
    try:
        record = json.loads(response.text)['data']['record']
    except KeyError:
        print(response.text)
        record = ''
    return record


def bitable_get_records(app_id, table_id):
    url = "https://open.feishu.cn/open-apis/bitable/v1/apps/" + app_id + "/tables/" + table_id + "/records"
    response = requests.request("GET", url, headers=header_get())
    try:
        records = json.loads(response.text)['data']['items']
    except KeyError:
        print(response.text)
        records = []
    return records


def bitable_delete_record(app_id, table_id, record_id):
    url = "https://open.feishu.cn/open-apis/bitable/v1/apps/" + app_id + "/tables/" + table_id + "/records/" + record_id
    response = requests.request("DELETE", url, headers=header_get())
    try:
        success = json.loads(response.text)['data']['deleted']
    except KeyError:
        print(response.text)
        success = False
    return success


def bitable_update_record(app_id, table_id, record_id, fields):
    url = "https://open.feishu.cn/open-apis/bitable/v1/apps/" + app_id + "/tables/" + table_id + "/records/" + record_id

    payload = json.dumps({
        "fields": fields
    })

    response = requests.request("PUT", url, headers=header_get(), data=payload)
    try:
        success = json.loads(response.text)['data']['record']
    except KeyError:
        print(response.text)
        success = False
    return success


def bitale_add_column(column_name):
    url = "https://open.feishu.cn/open-apis/bitable/v1/apps/fields"
    payload = json.dumps({
        "field_name": column_name,
        "type": 1
    })
    response = requests.request("POST", url, headers=header_get(), data=payload)
    print(response.text)


def tenant_access_token_get():
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal"
    payload = json.dumps({
        "app_id": appid,
        "app_secret": app_secret
    })

    headers = {
        'Content-Type': 'application/json'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    # print(json.loads(response.text)["tenant_access_token"])
    return json.loads(response.text)["tenant_access_token"]


token = tenant_access_token_get()


def header_get():
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer ' + token
    }
    return headers
# tenant_access_token_get()
