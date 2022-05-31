#!/Library/Frameworks/Python.framework/Versions/3.10/bin/python3
# -*- coding: UTF-8 -*-

#
#
#APP_ID=cli_a2df8867e238100d
#APP_SECRET=EbDHcUtx6RdxBQgshr85DdJToZzKRBLn
#VERIFICATION_TOKEN=qZrs32EFaFOV2WIRM265xedAF8Cei0sU

#创建完应用后可根据APP ID和 App Secret构造请求获取

import json
import requests
import string
import sys

def get_tenant_access_token():
  tokenurl="https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
  headers={"Content-Type":"application/json"}
  data={"app_id":"cli_a2df8867e238100d","app_secret":"EbDHcUtx6RdxBQgshr85DdJToZzKRBLn"}
  request=requests.post(url=tokenurl,headers=headers,json=data)
  response=json.loads(request.content)['tenant_access_token']
  return response

def get_open_id(tenant_access_token,user_email):
  userurl="https://open.feishu.cn/open-apis/contact/v3/users/batch_get_id"
  headers={"Authorization":"Bearer %s"%tenant_access_token}
  payload = {
    "user_id_type": "open_id",
    "emails": [user_email, "sunfuzhou@163.com"]
  }
  request=requests.post(url=userurl,headers=headers, data=payload)
  response=json.loads(request.content).get("data").get("user_list")[0].get("user_id")
  return response


###############################
def getchatid(tenant_access_token):
    #获取chatid
    chaturl="https://open.feishu.cn/open-apis/chat/v4/list?page_size=20"
    headers={"Authorization":"Bearer %s"%tenant_access_token,"Content-Type":"application/json"}
    request=requests.get(url=chaturl,headers=headers)
    response=json.loads(request.content)['data']['groups'][0]['chat_id']
    return response

############

def sendmes(user_id,chat_id,tenant_access_token):
    #向群里发送消息
    sendurl="https://open.feishu.cn/open-apis/message/v4/send/"
    headers={"Authorization":"Bearer %s"%tenant_access_token,"Content-Type":"application/json"}
    data={"chat_id":chat_id,
        "msg_type":"text",
        "content":{
            "text":"%s<at user_id=\"%s\">test</at>"%(messages,user_id)
        }
    }
    #给个人发送消息
     #data={"user_id":user_id,
     #    "msg_type":"text",
     #    "content":{
     #        "text":"%s<at user_id=\"%s\">test</at>"%(messages,user_id)
     #    }
     #}
    request=requests.post(url=sendurl,headers=headers,json=data)
    print(request.content)



if __name__ == '__main__': 

  ##传入邮箱和要发送的信息
  #messages="test-message-333" 
  #user_email="sunfuzhou@outlook.com"
  #user_email="sunfuzhou@163.com"
  #user_email="111@163.com"

  user_email = sys.argv[1]
  messages = sys.argv[2]
  

  #####
  tenant_access_token=get_tenant_access_token()
  open_id=get_open_id(tenant_access_token,user_email)
  chat_id=(getchatid(tenant_access_token))
  sendmes(open_id,chat_id,tenant_access_token)