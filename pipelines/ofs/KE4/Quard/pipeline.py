import copy
import random
import sys
import re
import enum
import os
import time
import json
import jenkins
import argparse
import requests
from requests.auth import HTTPBasicAuth
from collections import defaultdict
from filelock import FileLock
from urllib.parse import urlencode, quote_plus
from typing import Dict, Union, List, Tuple
from datetime import datetime
import curlify
from string import ascii_letters


os.environ["PYTHONHTTPSVERIFY"] = "0"


"""
Usage:


# Jenkins构建
python3 pipeline.py build \
        --server="https://cicd-ofs.kyligence.com" \
        --username="tong.zheng" \
        --token="117ce9d93aec1e60245e35890d1e2fe6c7" \
        --job-name="KE4/KE-All-CD-2.0" \
        --parameters='{"deployWay": "Manual Select", "deployType": "Regular", "version": "4.5.19.1", "envStage": "DEV", "customerPkg": "NORMAL", "docs_commitid": "latest", "scanApi": ""}' \
        --input-submit-parameters='{"commitId_back": "newten-open-core", "commitId_front": null, "overide": false, "back_remote": false, "front_remote": false, "skipNoSpark": false}'


# 单平台生成报告  
python3 pipelines/kc_package/pipeline.py report \
        --server="http://10.1.2.38:8080" \
        --username="quard" \
        --token="11d3a455b9af7fce8ad4f84265bb926295" \
        --job-name="4X_CDH5.16_ALL" \
        --job-id="977" \
        --subtype="compatibility" \
        --test-repo="quard-4.x" \
        --report-file="report.json"


# 飞书通知
python3 pipelines/kc_package/pipeline.py notify \
        --type="interactive" \
        --app-id="cli_a384621def7c100c" \
        --app-secret="pN2wI1O9fRnvUBkbkBt6Tf8iudbkVnCP" \
        --user-email="tong.zheng@kyligence.io" \
        --chat-name="冲冲冲" \
        --webhook="https://open.feishu.cn/open-apis/bot/v2/hook/77b4d804-6857-4171-9881-375d7f7d5cea" \
        --report-file="report.json" 
        # --user-code="14dm2591e6f74728b7886ade321997e4" 
        # --refresh-token-file="/Users/tong.zheng/PycharmProjects/kyligence/quard-4.x/kyQA-Quard/refresh_token_file.json"
              
              
# 飞书报告
python3 pipelines/kc_package/pipeline.py document \
        --app-id="cli_a384621def7c100c" \
        --app-secret="pN2wI1O9fRnvUBkbkBt6Tf8iudbkVnCP"

# Jira报告
python3 pipelines/kc_package/pipeline.py issue \
        --jira-server="https://olapio.atlassian.net" \
        --jira-user="tong.zheng@kyligence.io" \
        --jira-token="bMyQbU0TBpAmJL3fpaHT7189" \
        --report-file="report.json" \
        --project-id="10040" \
        --issue-type-id="10002" \
        --label="Quard" \
        --label="Regression_Bug" \
        --priority-id="4" \
        --title="[Quard][失败用例][{today}][{regtype_cn}] {test_case}"
        
"""


class FeishuNotifyTypo(enum.Enum):

    user = "user"
    group = "group"
    interactive = "interactive"


class FeishuDocumentBlockTypo(enum.Enum):

    text = {"name": "text", "type": 2, "description": "普通文本"}
    h1 = {"name": "heading1", "type": 3, "description": "一级标题"}
    h2 = {"name": "heading2", "type": 4, "description": "二级标题"}
    h3 = {"name": "heading3", "type": 5, "description": "三级标题"}
    h4 = {"name": "heading4", "type": 6, "description": "四级标题"}
    h5 = {"name": "heading5", "type": 7, "description": "五级标题"}
    h6 = {"name": "heading7", "type": 8, "description": "六级标题"}
    h7 = {"name": "heading8", "type": 9, "description": "七级标题"}
    h8 = {"name": "heading9", "type": 10, "description": "八级标题"}
    h9 = {"name": "heading9", "type": 11, "description": "九级标题"}
    sheet = {"name": "sheet", "type": 30, "description": "电子表格"}
    table = {"name": "table", "type": 31, "description": "表格"}


class JenkinsBlueOcean:

    def __init__(self, jks: "JenkinsWrapper"):
        self.jks = jks

    def get_steps_url(self, job_name: str, job_id: str, display_name: str = "Building noSpark") -> str:
        new_job_name = "pipelines/" + "/pipelines/".join(job_name.split("/"))
        request = requests.Request(
            method='GET',
            url=f"{self.jks.server}/blue/rest/organizations/jenkins/{new_job_name}/runs/{job_id}/nodes/",
        )
        response = self.jks.jenkins_request(request)
        contents = response.json()
        for item in contents:
            if item["displayName"].rfind(display_name) != -1:
                return f"{self.jks.server}{item['_links']['steps']['href']}"
        return ""

    def get_download_url(self, steps_url: str) -> str:
        request = requests.Request(method='GET', url=steps_url)
        response = self.jks.jenkins_request(request)
        contents = response.json()
        for item in contents:
            if not item["displayDescription"]: continue
            if item["displayDescription"].startswith("Download URL:"):
                return item["displayDescription"].split("Download URL:")[1].strip()
        return ""


class JenkinsWrapper(jenkins.Jenkins):

    """
    做二次封装是因为默认的方法无法正常提交构建任务.
    通过观察和参考curl的网络字节流的交互, 对 build_job_url 和 build_job 这两个方法进行了微小的调整.
    1. 对 build_job_url 方法进行了约束, 将接口范围缩小到仅支持 buildWithParameters 接口,
       因为这个接口可以返回 queue_id，通过 queue_id 可以拿到准确的 job_id.
    2. 对 build_job 方法进行了修改, 将原本的参数以path形式拼接改为以body的形式分段提交.

    Refer:
    https://www.jenkins.io/doc/book/using/remote-access-api/#RemoteaccessAPI-Submittingjobs

    Refer:
    curl --trace - -k https://cicd-ofs.kyligence.com/job/KE4/job/KE-All-CD-2.0/buildWithParameters \
         --user USER:TOKEN \
         --data deployWay="Manual Select" \
         --data deployType="Regular" \
         --data version="4.5.19.0"
    """

    def build_job_url(self, name, parameters=None, token=None, default_behavior=False):
        if default_behavior:
            return super(JenkinsWrapper, self).build_job_url(name, parameters, token)

        folder_url, short_name = self._get_job_folder(name)
        return self._build_url(jenkins.BUILD_WITH_PARAMS_JOB, locals())

    def build_job(self, name, parameters=None, token=None, default_behavior=False):
        if default_behavior:
            return super(JenkinsWrapper, self).build_job(name, parameters, token)

        request = requests.Request(
            method='POST',
            url=self.build_job_url(name),
            data=parameters
        )
        response = self.jenkins_request(request)

        if 'Location' not in response.headers:
            raise jenkins.EmptyResponseException(
                "Header 'Location' not found in "
                "response from server[%s]" % self.server)

        location = response.headers['Location']
        # location is a queue item, eg. "http://jenkins/queue/item/25/"
        if location.endswith('/'):
            location = location[:-1]
        parts = location.split('/')
        number = int(parts[-1])
        return number

    def get_stage_status(self, job_name: str, job_id: str) -> Union[str, None]:
        folder_url, short_name = self._get_job_folder(job_name)
        uri = f"%(folder_url)sjob/%(short_name)s/wfapi/runs"
        url = self._build_url(f"{self.server}{uri}", locals())
        parameters = urlencode({
            "since": int(job_id) - 1,
            "fullStages": True,
            "_": int(datetime.now().timestamp() * 1000)
        })
        response = self.jenkins_request(requests.Request("GET", f"{url}?{parameters}"))
        contents = response.json()
        for i in contents:
            if i["id"] == job_id:
                return i["status"]

    def get_input_submit_id(self, job_name: str, job_id: str):
        folder_url, short_name = self._get_job_folder(job_name)
        uri = f"%(folder_url)sjob/%(short_name)s/{job_id}/wfapi/nextPendingInputAction"
        url = self._build_url(f"{self.server}{uri}", locals())
        parameters = urlencode({"_": int(datetime.now().timestamp() * 1000)})
        response = self.jenkins_request(requests.Request("GET", f"{url}?{parameters}"))
        contents = response.json()
        return contents["id"]

    def input_submit(self, job_name: str, job_id: str, input_id: str, **parameters) -> None:
        folder_url, short_name = self._get_job_folder(job_name)
        uri = f"%(folder_url)sjob/%(short_name)s/{job_id}/wfapi/inputSubmit"
        url = self._build_url(f"{self.server}{uri}", locals())
        url_parameters = urlencode({"inputId": input_id})
        form_parameters = {"json": json.dumps({"parameter": [{"name": k, "value": v} for k, v in parameters.items()]})}
        self.jenkins_request(requests.Request("POST", f"{url}?{url_parameters}", data=form_parameters))

    def get_job_name(self, job_url: str):
        job_url = re.sub(r"/\d+[/]?$", "", job_url)
        response = self.jenkins_request(requests.Request("GET", f"{job_url}/api/json"))
        contents = response.json()
        return contents["fullName"]


class AllureParser:

    def __init__(self, jks: "JenkinsWrapper"):
        self.jks = jks

    def get_summary_info(self, job_name: str, job_id: str, subtype_en: str, subtype_cn: str) -> Dict:
        folder_url, short_name = self.jks._get_job_folder(job_name)
        uri = f"%(folder_url)sjob/%(short_name)s/{job_id}/allure/widgets/summary.json"
        url = self.jks._build_url(f"{self.jks.server}{uri}", locals())
        content = requests.get(url).json()
        total = content["statistic"]["total"]
        passed = content["statistic"]["passed"] + content["statistic"]["skipped"]
        return {"total_cases": total,
                "passed_cases": passed,
                "error_cases": content["statistic"]["broken"] + content["statistic"]["failed"],
                "error_stack": self.get_failure_cases(job_name, job_id, subtype_en, subtype_cn)}

    def get_failure_cases(self, job_name: str, job_id: str, subtype_en: str, subtype_cn: str) -> Dict:
        folder_url, short_name = self.jks._get_job_folder(job_name)
        uri = f"%(folder_url)sjob/%(short_name)s/{job_id}/allure/data/suites.json"
        url = self.jks._build_url(f"{self.jks.server}{uri}", locals())
        content = requests.get(url).json()
        failure_cases = {}
        for children in content["children"]:
            for testcase in children["children"]:
                name = testcase["name"]
                failure = any([True if i["children"][0]["status"] in ("broken", "failed") else False for i in testcase["children"]])
                case_uid = f"{testcase['children'][0]['children'][0]['uid']}"
                parent_uid = f"{testcase['children'][0]['children'][0]['parentUid']}"
                if failure:
                    failure_info = self.get_failure_info(job_name, job_id, case_uid)
                    failure_info["url"] = self.get_detail_url(job_name, job_id, case_uid, parent_uid)
                    failure_info["subtype_en"] = subtype_en
                    failure_info["subtype_cn"] = subtype_cn
                    failure_cases[name] = failure_info
        return failure_cases

    def get_failure_info(self, job_name: str, job_id: str, case_uid: str) -> Dict:
        folder_url, short_name = self.jks._get_job_folder(job_name)
        uri = f"%(folder_url)sjob/%(short_name)s/{job_id}/allure/data/test-cases/{case_uid}.json"
        url = self.jks._build_url(f"{self.jks.server}{uri}", locals())
        content = requests.get(url).json()
        seconds = f"{int((content['time']['duration'] / 1000) % 60)}".zfill(2)
        minutes = f"{int((content['time']['duration'] / (1000 * 60)) % 60)}".zfill(2)
        hours = f"{int((content['time']['duration'] / (1000 * 60 * 60)) % 24)}".zfill(2)
        return {
            "errmsg": content["statusMessage"],
            "stack": content["statusTrace"],
            "duration": f"{hours}:{minutes}:{seconds}"
        }

    def get_detail_url(self, job_name: str, job_id: str, case_uid: str, parent_uid: str):
        folder_url, short_name = self.jks._get_job_folder(job_name)
        uri = f"%(folder_url)sjob/%(short_name)s/{job_id}/allure/#suites/{parent_uid}/{case_uid}"
        return self.jks._build_url(f"{self.jks.server}{uri}", locals())


class JenkinsHelper:

    def __init__(self, jenkins_server, username, token):
        self.jenkins_server = jenkins_server
        self.username = username
        self.token = token
        self.jks = JenkinsWrapper(
            url=self.jenkins_server,
            username=self.username,
            password=self.token,
        )
        self.blue_ocean = JenkinsBlueOcean(self.jks)
        self.allure_parser = AllureParser(self.jks)
        self.input_submitted = False

    def build_job(self, job_name, parameters) -> str:
        queue_id = self.jks.build_job(name=job_name, parameters=parameters)
        url = f"{self.jenkins_server}/queue/item/{queue_id}/api/json"

        while True:
            req = requests.Request("GET", url)
            response = self.jks.jenkins_request(req)
            content = response.json()
            if content.get("executable"):
                return str(content["executable"]["number"])
            time.sleep(1)

    def wait_build_finish(
        self,
        job_name: str,
        job_id: str,
        input_submit: bool = False,
        input_submit_parameters: Union[Dict, None] = None
    ) -> str:
        while True:
            try:
                response = self.jks.get_build_info(job_name, int(job_id))

                if not self.input_submitted and input_submit and self.current_stage_is_waiting(job_name, job_id):
                    input_submit_id = self.jks.get_input_submit_id(job_name, job_id)
                    self.jks.input_submit(job_name, job_id, input_submit_id, **input_submit_parameters)
                    self.input_submitted = True

                if not response["building"]:
                    return response["result"]
            except requests.exceptions.HTTPError as e:
                print(f"requests.exceptions.HTTPError: {e}")
            except Exception as e:
                print(f"unknown exception: {e}")
            finally:
                time.sleep(1)

    def current_stage_is_waiting(self, job_name: str, job_id: str) -> bool:
        return self.jks.get_stage_status(job_name, job_id) == "PAUSED_PENDING_INPUT"

    def generate_url(self, job_name: str, job_id: str) -> str:
        folder_url, short_name = self.jks._get_job_folder(job_name)
        uri = f"%(folder_url)sjob/%(short_name)s/{job_id}"
        return self.jks._build_url(f"{self.jks.server}{uri}", locals())

    def get_job_info(self, job_url, repo: Union[str, None] = None) -> Union[None, Dict]:
        return self._get_job_info(f"{job_url}/api/json", repo)

    def get_job_info2(self, job_name, job_id: str, repo: Union[str, None] = None) -> Union[None, Dict]:
        folder_url, short_name = self.jks._get_job_folder(job_name)
        uri = f"%(folder_url)sjob/%(short_name)s/{job_id}/api/json"
        url = self.jks._build_url(f"{self.jks.server}{uri}", locals())
        return self._get_job_info(url, repo)

    def _get_job_info(self, url: str, repo: Union[str, None] = None) -> Dict:
        req = requests.Request("GET", url)
        response = self.jks.jenkins_request(req)
        content = response.json()
        duration = (time.time() * 1000) - content["timestamp"]
        seconds = f"{int((duration / 1000) % 60)}".zfill(2)
        minutes = f"{int((duration / (1000 * 60)) % 60)}".zfill(2)
        hours = f"{int((duration / (1000 * 60 * 60)) % 24)}".zfill(2)
        build_time = datetime.fromtimestamp(int(content["timestamp"] / 1000))
        has_build_data = False
        for action in content["actions"]:
            if action.get("_class") == "hudson.plugins.git.util.BuildData":
                has_build_data = True
                commit = action["lastBuiltRevision"]["branch"][0]["SHA1"]
                branch = action["lastBuiltRevision"]["branch"][0]["name"].split("/")[-1]
                result = {
                    "commit": commit,
                    "repo": action["remoteUrls"][0],
                    "branch": branch,
                    "duration": f"{hours}:{minutes}:{seconds}",
                    "build_time": build_time.strftime("%Y-%m-%d %H:%M:%S")
                }
                if repo is None:
                    return result
                elif repo and f"{repo}.git".upper() in action["remoteUrls"][0].upper():
                    return result

        if not has_build_data:
            return {"commit": None, "repo": None, "branch": None,
                    "duration": f"{hours}:{minutes}:{seconds}",
                    "build_time": build_time.strftime("%Y-%m-%d %H:%M:%S")}

    def artifact_file_exist(self, job_name: str, job_id: str, filename: str) -> bool:
        job_url = self.generate_url(job_name=job_name, job_id=job_id)
        api_url = f"{job_url}/api/json"
        req = requests.Request("GET", api_url)
        response = self.jks.jenkins_request(req)
        content = response.json()
        for i in content["artifacts"]:
            if i["fileName"].upper() == filename.upper():
                return True
        return False

    def get_artifact_url(self, job_name: str, job_id: str, filename: str) -> str:
        folder_url, short_name = self.jks._get_job_folder(job_name)
        uri = f"%(folder_url)sjob/%(short_name)s/{job_id}/artifact/{filename}"
        return self.jks._build_url(f"{self.jks.server}{uri}", locals())

    def download_artifact(self, artifact_url: str, save_to: str) -> None:
        resp = requests.get(artifact_url, stream=True)
        resp.raise_for_status()
        with open(save_to, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1024):
                f.write(chunk)

    def read_artifact_as_json(self, artifact_url: str) -> Dict:
        resp = requests.get(artifact_url)
        return resp.json()

    def get_current_job_url(self, job_name: str, job_id: str):
        folder_url, short_name = self.jks._get_job_folder(job_name)
        uri = f"%(folder_url)sjob/%(short_name)s/{job_id}/"
        return self.jks._build_url(f"{self.jks.server}{uri}", locals())


class FeishuAuthenticate:

    # TODO:
    # 1. 先创建文档, 然后再将文档移动到wiki.
    # https://open.feishu.cn/document/ukTMukTMukTM/uUDN04SN0QjL1QDN/wiki-v2/space-node/move_docs_to_wiki

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        user_access_token_code: Union[str, None] = None,
        refresh_token_file: str = os.path.expanduser("~/refresh_token_file.json")
    ):
        self.app_id = app_id
        self.app_secret = app_secret
        self._tenant_access_token = self.get_tenant_access_token()
        self._user_code = user_access_token_code
        self._refresh_token_file = refresh_token_file
        self._user_access_token = None
        self._refresh_token = None

        self.setup_user_access_token()

    @property
    def tenant_access_headers(self) -> Dict:
        return {"Authorization": f"Bearer {self.tenant_access_token}"}

    @property
    def tenant_access_token(self) -> Dict:
        return self._tenant_access_token

    @property
    def user_access_headers(self) -> Dict:
        return {"Authorization": f"Bearer {self.user_access_token}"}

    @property
    def user_access_token(self) -> Dict:
        return self._user_access_token

    def get_tenant_access_token(self) -> Dict:
        response = requests.post(
            url="https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/",
            json={
                "app_id": self.app_id,
                "app_secret": self.app_secret
            }
        )
        return json.loads(response.content)['tenant_access_token']

    @staticmethod
    def generate_redirect_url(app_id) -> str:
        redirect_uri = quote_plus("https://open.feishu.cn/")
        return f"https://open.feishu.cn/open-apis/authen/v1/index?redirect_uri={redirect_uri}&app_id={app_id}"

    def get_user_access_token(self, user_code) -> Dict:
        response = requests.post(
            url="https://open.feishu.cn/open-apis/authen/v1/access_token",
            headers=self.tenant_access_headers,
            json={
                "grant_type": "authorization_code",
                "code": user_code
            }
        )
        return json.loads(response.content)

    def refresh_user_access_token(self, refresh_token) -> Dict:
        response = requests.post(
            url="https://open.feishu.cn/open-apis/authen/v1/refresh_access_token",
            headers=self.tenant_access_headers,
            json={
                "grant_type": "refresh_token",
                "refresh_token": refresh_token
            }
        )
        return json.loads(response.content)

    def setup_user_access_token(self) -> None:
        refresh_expire = None
        history_expire = []
        if self._user_code:
            resp = self.get_user_access_token(self._user_code)
            self._user_access_token = resp["data"]["access_token"]
            self._refresh_token = resp["data"]["refresh_token"]
            refresh_expire = resp["data"]["refresh_expires_in"]
        else:
            if not os.path.exists(self._refresh_token_file):
                redirect_url = FeishuAuthenticate.generate_redirect_url(self.app_id)
                errmsg = f"尚未鉴权, 请先访问这个链接生成鉴权码: {redirect_url}"
                raise RuntimeError(errmsg)

            with open(self._refresh_token_file) as f:
                content = json.load(f)
                if content["expire"] <= int(datetime.now().timestamp()):
                    redirect_url = FeishuAuthenticate.generate_redirect_url(self.app_id)
                    errmsg = f"用户鉴权码已过期, 重新生成需要用户访问这个链接: {redirect_url}"
                    raise RuntimeError(errmsg)

                resp = self.refresh_user_access_token(refresh_token=content["refresh_token"])
                self._user_access_token = resp["data"]["access_token"]
                self._refresh_token = resp["data"]["refresh_token"]
                refresh_expire = resp["data"]["refresh_expires_in"]
                content["history_expire"].append(content["expire"])
                history_expire = content["history_expire"]

        with open(self._refresh_token_file, "w") as f:
            f.write(json.dumps({
                "refresh_token": self._refresh_token,
                "expire": int(datetime.now().timestamp() + refresh_expire - 172800),  # 172800 = 2 day
                "history_expire": history_expire
            }))


class FeishuDocument:

    def __init__(self, auth: FeishuAuthenticate):
        self.auth = auth

    def create_document(self, folder_token, title):
        response = requests.post(
            url="https://open.feishu.cn/open-apis/docx/v1/documents",
            headers=self.auth.user_access_headers,
            json={
                "folder_token": folder_token,
                "title": title
            }
        )
        return json.loads(response.content)

    def move_document_to_wiki(
        self,
        space_id: str = "6991710376987541505",                      # CTO OFFICE - 质量部
        parent_wiki_token: str = "wikcnqQ8aPGnqwIXOqtOV4Kh5Tg",     # Quard 日报
        document_type: str = "docx",
        document_token: str = "wikcnnWxKIzxHShab0rDe4YcGFe"              # 文档编号
    ):
        response = requests.post(
            url=f"https://open.feishu.cn/open-apis/wiki/v2/spaces/{space_id}/nodes/move_docs_to_wiki",
            headers=self.auth.user_access_headers,
            json={
              "parent_wiki_token": parent_wiki_token,
              "obj_type": document_type,
              "obj_token": document_token,
            }
        )
        return json.loads(response.content)

    def create_block(
        self,
        document_id: str,
        payload: dict,
        block_id: Union[str, None] = None
    ):
        document_revision_id = -1
        if block_id is None:
            block_id = document_id

        response = requests.post(
            url=f"https://open.feishu.cn/open-apis/docx/v1/documents/{document_id}/blocks/{block_id}/children?document_revision_id={document_revision_id}",
            headers=self.auth.user_access_headers,
            json=payload
        )
        return json.loads(response.content)

    def create_text_block(
        self,
        text: str,
        document_id: str,
        block_typo: FeishuDocumentBlockTypo = FeishuDocumentBlockTypo.text
    ):
        payload = {
            "index": -1,
            "children": [
                {
                    "block_type": block_typo.value["type"],
                    block_typo.value["name"]: {
                        "elements": [
                            {
                                "text_run": {
                                    "content": text,
                                    "text_element_style": {}
                                }
                            }
                        ],
                        "style": {}
                    }
                }
            ]
        }
        return self.create_block(document_id, payload)

    def create_sheet_block(self, row_size: int, column_size: int, document_id: str):
        payload = {
            "index": -1,
            "children": [
                {
                    "block_type": FeishuDocumentBlockTypo.sheet.value["type"],
                    FeishuDocumentBlockTypo.sheet.value["name"]: {
                        "row_size": row_size,
                        "column_size": column_size
                    }
                }
            ]
        }
        return self.create_block(document_id, payload)

    def append_sheet_rows(self, sheet_id: str, sheet_token: str, size: int):
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{sheet_token}/dimension_range"
        resp = requests.post(url, headers=self.auth.user_access_headers, json={
            "dimension": {
               "sheetId": sheet_id,
                "majorDimension": "ROWS",
                "length": size
             }
        })
        print(resp.content)
        return resp.json()

    def write_values_to_sheet(self, sheet_token, value_range, values):
        url = f"https://open.feishu.cn/open-apis/sheets/v2/spreadsheets/{sheet_token}/values_append"
        resp = requests.post(url, headers=self.auth.user_access_headers, json={
            "valueRange": {
                "range": value_range,
                "values": values
            }
        })
        print(resp.content)
        return resp.json()


class FeishuHelper:

    def __init__(
        self,
        app_id: str,
        app_secret: str,
        user_access_token_code: Union[str, None] = None,
        refresh_token_file: str = os.path.expanduser("~/refresh_token_file.json")
    ):
        self.app_id = app_id
        self.app_secret = app_secret
        self.auth = FeishuAuthenticate(app_id, app_secret, user_access_token_code, refresh_token_file)
        self.document = FeishuDocument(auth=self.auth)

    def get_open_id(self, user_email):
        response = requests.post(
            url="https://open.feishu.cn/open-apis/contact/v3/users/batch_get_id",
            headers=self.auth.tenant_access_headers,
            params={
                "user_id_type": "open_id"
            },
            json={
                "emails": [user_email]
            }
        )
        return json.loads(response.content).get("data").get("user_list")[0].get("user_id")

    def get_chat_id(self, chat_name: str, page_token: str = "") -> Union[str, None]:
        """
        @param chat_name:  通过chat_name(群名称)获取群ID(chat_id)
        @param page_token: 翻页查询游标.
        @return:
        """
        resp = requests.get(
            url="https://open.feishu.cn/open-apis/im/v1/chats",
            headers=self.auth.user_access_headers,
            params={"page_size": 100, "user_id_type": "user_id", "page_token": page_token},
        )

        content = resp.json()
        for i in content["data"]["items"]:
            if i["name"] == chat_name:
                return i["chat_id"]

        if content["data"]["page_token"]:
            return self.get_chat_id(chat_name, content["data"]["page_token"])

    def send_interactive_message_to(
        self,
        receive_id_type: str,
        receive_id: str,
        title: str,
        report_link: str,
        full_cases: Union[List[str], None] = None,
        compatibility_cases: Union[List[str], None] = None,
        upgrade_cases: Union[List[str], None] = None,
        azure_cases: Union[List[str], None] = None,
        aws_cases: Union[List[str], None] = None
    ):
        # 调试路径: https://open.feishu.cn/api-explorer/cli_a384621def7c100c
        content = {
            "header": {
                "title": {
                    "tag": "plain_text",
                    "content": title
                },
                "template": "red"
            },
            "elements": [
                {
                    "tag": "hr"
                },
                {
                    "tag": "div",
                    "fields": [
                        {
                            "is_short": False,
                            "text": {
                                "tag": "lark_md",  # 支持markdown功能
                                "content": report_link
                            }
                        }
                    ]
                }
            ]
        }

        case_items = [
            ("AWS", aws_cases),
            ("Azure", azure_cases),
            ("升级回滚", upgrade_cases),
            ("兼容性", compatibility_cases),
            ("全量", full_cases)
        ]

        for subtitle, case_info in case_items:
            if case_info is None:
                continue

            element = {
                "tag": "div",
                "fields": [
                    {
                        "is_short": False,
                        "text": {
                            "tag": "lark_md",  # 支持markdown功能
                            "content": "**{}:** 总数: {}; 通过: {}; 失败: {}; 通过率: {}; 耗时: {}".format(subtitle, *case_info)
                        }
                    }
                ]
            }
            content["elements"].insert(0, element)

        response = requests.post(
            url="https://open.feishu.cn/open-apis/im/v1/messages",
            headers=self.auth.tenant_access_headers,
            params={
                "receive_id_type": receive_id_type
            },
            json={
                "receive_id": receive_id,
                "msg_type": "interactive",
                "content": json.dumps(content),
            }
        )
        return response.content

    def send_text_message_to_group(
        self,
        webhook_url: str,
        title: str,
        report: str,
        full_cases: List[str],
        compatibility_cases: List[str],
        upgrade_cases: Union[List[str], None] = None,
        azure_cases: Union[List[str], None] = None,
        aws_cases: Union[List[str], None] = None
    ):
        case_items = [
            ("AWS", aws_cases),
            ("Azure", azure_cases),
            ("升级回滚", upgrade_cases),
            ("兼容性", compatibility_cases),
            ("全量", full_cases)
        ]

        message = ""
        for subtitle, case_info in case_items:
            if case_info:
                message = "{}: 总数: {}; 通过: {}; 通过率: {}\n".format(subtitle, *case_info) + message
        message += f"{'-'*42}\n{report}\n"

        payload_message = {
            "msg_type": "post",
            "content": {
                "post": {
                    "zh_cn": {
                        "title": title,
                        "content": [
                            [
                                {
                                    "tag": "text",
                                    "text": message
                                },
                                # {
                                #     "tag": "at",
                                #     "user_id": open_id,
                                # }
                            ]
                        ]
                    }
                }
            }
        }
        response = requests.request("POST", webhook_url, json=payload_message)
        return response

    def create_daily_reporter(self):
        pass

    def create_release_reporter(self):
        pass


class JiraTemplate:

    @classmethod
    def issue(
        cls,
        title: str,
        project_id: str,
        issue_type_id: str,
        priority_id: str,
        labels: List[str],
        description: Dict
    ) -> Dict:
        return {
            "fields": {
                "project": {"id": project_id},
                "issuetype": {"id": issue_type_id},
                "priority": {"id": priority_id},
                "summary": title,
                "description": description,
                "labels": labels
            },
            "update": {}
        }

    @classmethod
    def text(cls, string: str) -> Dict:
        """ 一行/一个段落 """
        return {
            "type": "paragraph",
            "content": [
                {
                    "type": "text",
                    "text": string
                }
            ]
        }

    @classmethod
    def heading(cls, level: int, string: str) -> Dict:
        return {
            "type": "heading",
            "attrs": {
                "level": level
            },
            "content": [
                {
                    "type": "text",
                    "text": string
                }
            ]
        }

    @classmethod
    def code_block(cls, code: str) -> Dict:
        return {
            "type": "codeBlock",
            "attrs": {},
            "content": [
                {
                    "type": "text",
                    "text": code
                }
            ]
        }

    @classmethod
    def description(cls, content: List) -> Dict:
        return {
            "version": 1,
            "type": "doc",
            "content": content
        }

    @classmethod
    def media(cls, media_id: str, media_collection: str) -> Dict:
        return {
            "type": "mediaGroup",
            "content": [
                {
                    "type": "media",
                    "attrs": {
                        "id": media_id,
                        "type": "file",
                        "collection": media_collection
                    }
                }
            ]
        }


class JiraHelper:

    def __init__(self, server, username, token):
        self.version = "3"
        self.server = server
        self.username = username
        self.token = token
        self.auth = HTTPBasicAuth(username, token)
        self.template = JiraTemplate()

    def create_issue(self, payload):
        url = f"{self.server}/rest/api/{self.version}/issue"
        return requests.post(url, auth=self.auth, json=payload)

    def upload_file(self, filename):
        raise NotImplementedError


class PipelineDownloadType(enum.Enum):

    none = "none"
    ke = "ke"
    ke_no_spark = "ke_no_spark"
    ke_and_nospark = "ke_and_nospark"


class ReportRegTypeEN(enum.Enum):

    ga = "ga"
    sp = "sp"
    daily = "daily"
    other = "other"


class ReportRegTypeCN(enum.Enum):

    ga = "{today} {branch} {version} GA发布"
    sp = "{today} {branch} {version} SP发布"
    daily = "{today} {branch} {version} 日报"
    other = "{today} {branch} {version} 非常规回归"


class ReportSubTypeEN(enum.Enum):

    full = "full"
    compatibility = "compatibility"
    upgrade = "upgrade"
    azure = "azure"
    aws = "aws"


class ReportSubTypeCN(enum.Enum):

    full = "全量"
    compatibility = "兼容性"
    upgrade = "升级回滚"
    azure = "azure"
    aws = "aws"


def build(
    server: str,
    username: str,
    token: str,
    job_name: str,
    parameters: Dict,
    input_submit_parameters: Union[Dict, None] = None,
    download_type: PipelineDownloadType = PipelineDownloadType.none,
    job_url: str = "",
    download_file: str = "download.txt",
    report_file: str = "report.json",
    version: Union[str, None] = None,
    regtype: Union[str, None] = None,
    repo: Union[str, None] = None
):
    """
    @param server:                      "https://cicd-ofs.kyligence.com"
    @param username:                    "tong.zheng"
    @param token:                       "117ce9d93aec1e60245e35890d1e2fe6c7"
    @param job_name:                    "KE4/KE-All-CD-2.0"
    @param parameters:                  {
                                            "deployWay": "Manual Select",
                                            "deployType": "Regular",
                                            "version": "4.5.19.1",
                                            "envStage": "DEV",
                                            "customerPkg": "NORMAL",
                                            "docs_commitid": "latest",
                                            "scanApi": ""
                                        }
    @param input_submit_parameters:     {
                                            "commitId_back": "newten-open-core",
                                            "commitId_front": None,
                                            "overide": False,
                                            "back_remote": False,
                                            "front_remote": False,
                                            "skipNoSpark": False
                                        }
    @param download_type:               ["", "ke", "ke_noSpark", "ke_and_noSpark"]
    @param job_url:                    https://cicd-ofs.kyligence.com/job/KE4/view/正式交付/job/KE-All-CD-2.0/1360/
    @param download_file:              存放包路径信息的文件
    @param platform_file:              存放平台信息的文件
    @param report_file:                存放报告信息的文件
    @param version:                    4.6.0
    @param regtype:                    ga | sp | daily | other
    @param repo:                       Kyligence
    @return:
    """
    jks = JenkinsHelper(jenkins_server=server, username=username, token=token)

    if job_url:
        job_id: str = re.findall(r"/\d+", job_url)[0].replace("/", "")
        job_name_2 = jks.jks.get_job_name(job_url)
        print(f"job_name_2: {job_name_2}")
    else:
        job_id: str = jks.build_job(job_name=job_name, parameters=parameters)
        job_url: str = jks.get_current_job_url(job_name, job_id)

        if not input_submit_parameters:
            jks.wait_build_finish(
                job_name=job_name,
                job_id=job_id,
            )
        else:
            jks.wait_build_finish(
                job_name=job_name,
                job_id=job_id,
                input_submit=True,
                input_submit_parameters=input_submit_parameters
            )

    def save_ke_url(filemode: str = "w") -> None:
        steps_url = jks.blue_ocean.get_steps_url(job_name=job_name, job_id=job_id, display_name="Building Normal")
        download_url = jks.blue_ocean.get_download_url(steps_url=steps_url)
        package_name = download_url.split("/")[-1]
        with open(download_file, filemode) as f:
            f.write(f"{package_name},{download_url}\n")

    def save_ke_no_spark_url(filemode: str = "w") -> None:
        steps_url = jks.blue_ocean.get_steps_url(job_name=job_name, job_id=job_id, display_name="Building noSpark")
        download_url = jks.blue_ocean.get_download_url(steps_url=steps_url)
        if not download_url: return
        package_name = download_url.split("/")[-1]
        with open(download_file, filemode) as f:
            f.write(f"{package_name},{download_url}\n")

    def create_report_file() -> None:
        info = jks.get_job_info(job_url, repo=repo)
        today = datetime.now().strftime("%Y-%m-%d")
        regtype_cn = ReportRegTypeCN[regtype].value.format(today=today, branch=info["branch"], version=version)
        data = {
            "title": regtype_cn,
            "report_link": "",
            "version": version,
            "summary": {},
            "regtype_en": regtype,
            "regtype_cn": regtype_cn,
            "dev_repo": repo,
            "dev_branch": info["branch"],
            "dev_commit": info["commit"]
        }
        lock = FileLock(f"{report_file}.lock")
        with lock:
            with open(report_file, "w") as f:
                f.write(json.dumps(data, indent=4))

    def save_summary() -> None:
        url = jks.get_artifact_url(job_name=job_name, job_id=job_id, filename=report_file)
        platform_report = jks.read_artifact_as_json(url)
        lock = FileLock(f"{report_file}.lock")
        with lock:

            with open(report_file, "r") as f:
                data = json.load(f)
                data["summary"][f"{job_name}:{platform_report['subtype_en']}"] = platform_report
                data = json.dumps(data, indent=4)

            with open(report_file, "w") as f:
                f.write(data)

    if download_type == PipelineDownloadType.ke:
        save_ke_url()
    elif download_type == PipelineDownloadType.ke_no_spark:
        save_ke_no_spark_url()
    elif download_type == PipelineDownloadType.ke_and_nospark:
        save_ke_url()
        save_ke_no_spark_url("a+")

    if repo and regtype:
        create_report_file()

    if jks.artifact_file_exist(job_name, job_id, report_file):
        save_summary()


def report(
    server: str,
    username: str,
    token: str,
    job_name: str,
    job_id: str,
    subtype_en: ReportSubTypeEN,
    subtype_cn: ReportSubTypeCN,
    test_repo: Union[str, None] = None,
    report_file: str = "report.json"
):
    jks = JenkinsHelper(jenkins_server=server, username=username, token=token)
    summary = jks.allure_parser.get_summary_info(
        job_name=job_name, job_id=job_id, subtype_en=subtype_en.value, subtype_cn=subtype_cn.value
    )
    job_info = jks.get_job_info2(job_name=job_name, job_id=job_id)
    job_url = jks.generate_url(job_name=job_name, job_id=job_id)
    result = {
        "server": server,
        "job_url": job_url,
        "job_id": job_id,
        "platform": job_name,
        "subtype_en": subtype_en.value,
        "subtype_cn": subtype_cn.value,
        "test_repo": test_repo if test_repo else job_info["repo"],
        "test_branch": job_info["branch"],
        "test_commit": job_info["commit"],
        "elapsed": job_info["duration"],
        "build_time": job_info["build_time"],
        "total_cases": summary["total_cases"],
        "passed_cases": summary["passed_cases"],
        "error_cases": summary["error_cases"],
        "error_stack": summary["error_stack"],
    }
    with open(report_file, "w") as f:
        f.write(json.dumps(result, indent=4))
    return result


def report_to_summary(report_file: str) -> Dict:

    # TODO: 这里转换为字典，不要使用列表
    title = None
    summary = {
        "full_cases": [0, 0, 0, "", [], []],
        "compatibility_cases": [0, 0, 0, "", [], []],
        "upgrade_cases": [0, 0, 0, "", [], []],
        "azure_cases": [0, 0, 0, "", [], []],
        "aws_cases": [0, 0, 0, "", [], []]
    }

    with open(report_file) as f:
        report_json = json.load(f)

    version = report_json["version"]
    regtype_en = report_json["regtype_en"]
    dev_branch = report_json["dev_branch"]
    today = datetime.now().strftime("%Y-%m-%d")
    for platform_subtype, info in report_json["summary"].items():
        platform, subtype_en = platform_subtype.split(":")
        key = f"{subtype_en}_cases"
        title = ReportRegTypeCN[regtype_en].value.format(today=today, branch=dev_branch, version=version)
        summary[key][0] += info["total_cases"]
        summary[key][1] += info["passed_cases"]
        summary[key][2] += info["error_cases"]
        summary[key][4].append(info["elapsed"])
        summary[key][5].append(platform)

    for i in list(summary.keys()):
        if summary[i][0:3] == [0, 0, 0]:
            summary.pop(i)

    for i in summary:
        summary[i][3] = f"{summary[i][1] / summary[i][0] * 100:.2f}%"
        sorted_elapsed = sorted(summary[i][4])
        mid_index = int((len(sorted_elapsed) - 1) / 2)
        summary[i][4] = sorted_elapsed[mid_index]

    summary["title"] = title
    summary["report_link"] = report_json["report_link"]
    return summary


def report_to_issues(report_file: str) -> Dict:

    issues = defaultdict(dict)

    with open(report_file) as f:
        report_json = json.load(f)

    for platform_subtype, info in report_json["summary"].items():
        platform, subtype_en = platform_subtype.split(":")
        for error_case, stack in info["error_stack"].items():
            stack["subtype_en"] = info["subtype_en"]
            stack["subtype_cn"] = info["subtype_cn"]
            issues[error_case][platform] = stack

    return issues


def get_last_line(filename, n = 1):
    num_newlines = 0
    with open(filename, 'rb') as f:
        try:
            f.seek(-2, os.SEEK_END)
            while num_newlines < n:
                f.seek(-2, os.SEEK_CUR)
                if f.read(1) == b'\n':
                    num_newlines += 1
        except OSError:
            f.seek(0)
        last_line = f.readline().decode()
    return last_line


def notify(
    app_id: str,
    app_secret: str,
    user_email: str,
    chat_name: str,
    report_file: str,
    notify_typo: FeishuNotifyTypo,
    refresh_token_file,
    webhook_url: str = None,
    user_code: str = None,
):
    """
    @param app_id:             "cli_a384621def7c100c"
    @param app_secret:         "pN2wI1O9fRnvUBkbkBt6Tf8iudbkVnCP"
    @param webhook_url:        "https://open.feishu.cn/open-apis/bot/v2/hook/77b4d804-6857-4171-9881-375d7f7d5cea"
    @param user_email:         "tong.zheng@kyligence.io"
    @param chat_name:          群名称
    @param report_file:        "report.json"
    @param notify_typo:        "user" or "group" or "interactive"
    @param refresh_token_file: user_access_token缓存文件
    @param user_code:          用户授权码
    @return:
    """
    feishu = FeishuHelper(
        app_id=app_id,
        app_secret=app_secret,
        user_access_token_code=user_code,
        refresh_token_file=refresh_token_file
    )

    summary = report_to_summary(report_file=report_file)
    if notify_typo == FeishuNotifyTypo.interactive:
        chat_id = feishu.get_chat_id(chat_name)
        feishu.send_interactive_message_to(receive_id_type="chat_id", receive_id=chat_id, **summary)
    elif notify_typo == FeishuNotifyTypo.user:
        open_id = feishu.get_open_id(user_email)
        feishu.send_interactive_message_to(receive_id_type="open_id", receive_id=open_id, **summary)
    elif notify_typo == FeishuNotifyTypo.group:
        feishu.send_text_message_to_group(webhook_url=webhook_url, **summary)


def document(
    app_id: str,
    app_secret: str,
    report_file: str,
    csv_file: str,
    refresh_token_file: str,
    space_id: str,
    parent_wiki_token: str,
    user_code: str = None,
) -> None:

    feishu = FeishuHelper(
        app_id=app_id,
        app_secret=app_secret,
        user_access_token_code=user_code,
        refresh_token_file=refresh_token_file
    )

    with open(report_file, "r") as f:
        report_json = json.load(f)

    if not report_json["summary"]:
        return

    # 创建文档
    resp = feishu.document.create_document(folder_token="fldcnHpRSoHKlAJKvy9LAkUBamb", title=report_json["title"])
    print(f"create document: {resp}")
    document_id = resp["data"]["document"]["document_id"]
    report_json["report_link"] = f"详细报告: https://kyligence.feishu.cn/docx/{document_id}"
    print(f"report_link: {report_json['report_link']}")
    lock = FileLock(f"{report_file}.lock")
    with lock:
        with open(report_file, "w") as f:
            f.write(json.dumps(report_json, indent=4))

    # 总览数据准备
    summary = report_to_summary(report_file)

    # s3数据准备
    values_en = [
        ["full"] + copy.deepcopy(summary.get("full_cases", [])),
        ["compatibility"] + copy.deepcopy(summary.get("compatibility_cases", [])),
        ["upgrade"] + copy.deepcopy(summary.get("upgrade_cases", [])),
        ["AWS"] + copy.deepcopy(summary.get("azure_cases", [])),
        ["Azure"] + copy.deepcopy(summary.get("aws_cases", [])),
    ]
    [values_en.pop(i) for i in reversed(range(len(values_en))) if len(values_en[i]) != 7]

    # s3模板表格
    ke_version = report_json["version"]
    ke_branch = report_json["dev_branch"]
    ke_commit = report_json["dev_commit"]
    report_link = report_json["report_link"]
    plan_type = report_json["regtype_en"]
    plan_time = datetime.now().strftime("%Y-%m-%d")
    with open(f"{csv_file}", "a+") as f:
        last_line = get_last_line(filename=csv_file)
        if last_line[-1] != "\n":
            f.write("\n")

        for i in values_en:
            dt = datetime.strptime(i[5], "%H:%M:%S")
            minutes = f"{(dt.hour * 60 + dt.minute)}"
            case_scope = f"{i[0]}"
            platforms = f"{'/'.join(i[6])}"
            exe_case_num = f"{i[1]}"
            fail_case_num = f"{i[3]}"
            items = [ke_version, ke_branch, plan_type, case_scope, platforms,
                     minutes, exe_case_num, fail_case_num, "0", plan_time, report_link]
            f.write(f"{','.join(items)}\n")

    # 总览数据准备
    values_cn = [
        ["类型", "总数", "成功", "失败", "成功率", "耗时", "平台"],
        ["全量"] + summary.get("full_cases", []),
        ["兼容性"] + summary.get("compatibility_cases", []),
        ["升级回滚"] + summary.get("upgrade_cases", []),
        ["AWS"] + summary.get("azure_cases", []),
        ["Azure"] + summary.get("aws_cases", []),
    ]
    [values_cn.pop(i) for i in reversed(range(len(values_cn))) if len(values_cn[i]) != 7]
    [values_cn[i].pop() for i in reversed(range(len(values_cn)))]

    # 创建总览表格
    feishu.document.create_text_block("总览", document_id, FeishuDocumentBlockTypo.h1)
    feishu.document.create_text_block(f"Branch: {ke_version}", document_id)
    feishu.document.create_text_block(f"Commit: {ke_commit}", document_id)
    response = feishu.document.create_sheet_block(len(values_cn), len(values_cn[0]), document_id)
    sheet_token, sheet_id = response["data"]["children"][0]["sheet"]["token"].split("_")
    feishu.document.write_values_to_sheet(
        sheet_token=sheet_token, value_range=f"{sheet_id}!A1:F{len(values_cn)}", values=values_cn
    )

    # 按平台维度做数据准备
    platforms_cn = [
        ["平台", "类型", "总数", "成功", "失败", "成功率", "耗时"],
    ]
    for platform_subtype, i in report_json["summary"].items():
        platform, subtype_en = platform_subtype.split(":")
        percent = f"{ 0 if i['passed_cases'] == 0 else i['passed_cases'] / i['total_cases'] * 100 :.2f}%"
        item = [platform, i["subtype_cn"], i["total_cases"], i["passed_cases"], i["error_cases"], percent, i["elapsed"]]
        platforms_cn.append(item)

    # 创建平台展示表格
    feishu.document.create_text_block("各平台执行结果", document_id, FeishuDocumentBlockTypo.h1)
    response = feishu.document.create_sheet_block(1, len(platforms_cn[0]), document_id)
    sheet_token, sheet_id = response["data"]["children"][0]["sheet"]["token"].split("_")
    feishu.document.append_sheet_rows(sheet_id, sheet_token, len(platforms_cn) - 1)
    print(f"sheet: values_cn: {platforms_cn}")
    feishu.document.write_values_to_sheet(
        sheet_token=sheet_token, value_range=f"{sheet_id}!A1:G{len(platforms_cn)}", values=platforms_cn
    )

    # 失败用例数据准备
    values_cn = [
        ["失败用例", "类型", "耗时", "平台"]
    ]
    for error_case, error_item in report_json["issues"].items():
        error_case = {
            "text": error_case,
            "link": error_item["jira_url"],
            "type": "url"
        }
        platforms = []
        durations = []
        subtype_cns = []
        for platform, stack in error_item["platforms"].items():
            platforms.append(platform)
            durations.append(stack["duration"])
            subtype_cns.append(stack["subtype_cn"])

        values_cn.append([error_case, subtype_cns[0], sorted(durations)[-1], "、".join(platforms)])

    # 创建失败用例表格
    feishu.document.create_text_block("失败用例", document_id, FeishuDocumentBlockTypo.h1)
    response = feishu.document.create_sheet_block(1, len(values_cn[0]), document_id)
    sheet_token, sheet_id = response["data"]["children"][0]["sheet"]["token"].split("_")
    feishu.document.append_sheet_rows(sheet_id, sheet_token, len(values_cn) - 1)
    print(f"sheet: values_cn: {values_cn}")
    feishu.document.write_values_to_sheet(sheet_token=sheet_token, value_range=f"{sheet_id}!A1:D{len(values_cn)}", values=values_cn)

    # 将文档移动至wiki
    feishu.document.move_document_to_wiki(space_id=space_id, parent_wiki_token=parent_wiki_token, document_token=document_id)


def issue(
    jira_server: str = "https://olapio.atlassian.net",
    jira_user: str = "tong.zheng@kyligence.io",
    jira_token: str = "bMyQbU0TBpAmJL3fpaHT7189",
    project_id: str = "10040",
    issue_type_id: str = "10002",
    priority_id: str = "4",
    labels: List[str] = ("Quard", "Regression_Bug"),
    title: str = "[Quard][失败用例][{regtype_cn}] {test_case}",
    report_file: str = "report.json",
):

    # add issues segment
    with open(report_file) as f:
        report_json = json.load(f)

    content = defaultdict(dict)
    for platform_subtype, info in report_json["summary"].items():
        platform, subtype_en = platform_subtype.split(":")
        for error_case, stack in info["error_stack"].items():
            if not content[error_case].get("platforms"):
                content[error_case]["platforms"] = {}
            content[error_case]["platforms"][platform] = copy.deepcopy(report_json["summary"][platform_subtype])
            content[error_case]["platforms"][platform].update(stack)

    report_json["issues"] = content

    # create jira issues
    jira = JiraHelper(jira_server, jira_user, jira_token)
    for error_case, error_items in report_json["issues"].items():

        contents = [
            jira.template.text(f"repo: {report_json['dev_repo']}"),
            jira.template.text(f"branch: {report_json['dev_branch']}"),
            jira.template.text(f"commit: {report_json['dev_commit']}"),
        ]

        for platform_k, platform_v in error_items["platforms"].items():
            contents.append(jira.template.heading(level=2, string=platform_k))
            contents.append(jira.template.text(f"allure 详情报告页面: {platform_v['url']}"))
            contents.append(jira.template.text(f"错误信息"))
            contents.append(jira.template.code_block(platform_v["errmsg"][0:1000]))
            contents.append(jira.template.text(f"错误堆栈"))
            contents.append(jira.template.code_block(platform_v["stack"][0:1000]))
            contents.append(jira.template.text(f" "))

        payload = jira.template.issue(
            title=title.format(regtype_cn=report_json["regtype_cn"], test_case=error_case),
            project_id=project_id,
            issue_type_id=issue_type_id,
            labels=labels,
            priority_id=priority_id,
            description=jira.template.description(contents)
        )

        resp = jira.create_issue(payload)
        content = resp.json()
        report_json["issues"][error_case]["jira_url"] = f"{jira_server}/browse/{content['key']}"

    with open(report_file, "w") as f:
        f.write(json.dumps(report_json, indent=4))


def main():
    parser = argparse.ArgumentParser(description=f"流水线部署工具")
    parser.add_argument("--verbose", )
    subparsers = parser.add_subparsers(dest="command", help="sub-command help")

    build_parser = subparsers.add_parser("build", help="流水线构建参数")
    build_parser.add_argument("--server", help="Jenkins Server", required=True)
    build_parser.add_argument("--username", help="Jenkins Username", required=True)
    build_parser.add_argument("--token", help="Jenkins Token", required=True)
    build_parser.add_argument("--job-name", dest="job_name", help="Jenkins Job Name", required=True)
    build_parser.add_argument("--job-url", dest="job_url", default="", help="KE Packaged Job url")
    build_parser.add_argument("--download-type", default="none", dest="download_type", help="['none', 'ke', 'ke_no_spark', 'ke_and_nospark']")
    build_parser.add_argument("--parameters", default="{}", help="Jenkins Build Parameters")
    build_parser.add_argument("--input-submit-parameters", dest="input_submit_parameters", default=None, help="Jenkins Build Input Submit Parameters for KE")
    build_parser.add_argument("--download-file", dest="download_file", default="download.txt", help="输出结果存放文件")
    build_parser.add_argument("--report-file", dest="report_file", default="report.json", help="报告文件名")
    build_parser.add_argument("--version", default=None, help="版本号")
    build_parser.add_argument("--regtype", default=None, help="回归类型")
    build_parser.add_argument("--repo", default=None, help="研发仓库")

    notify_parser = subparsers.add_parser("notify", help="飞书通知")
    notify_parser.add_argument("--type", choices=["user", "group", "interactive"], help="定向或在组内发送通知", required=True)
    notify_parser.add_argument("--app-id", dest="app_id", required=True)
    notify_parser.add_argument("--app-secret", dest="app_secret", required=True)
    notify_parser.add_argument("--user-email", dest="user_email", default="")
    notify_parser.add_argument("--chat-name", dest="chat_name", default="")
    notify_parser.add_argument("--webhook", default=None)
    notify_parser.add_argument("--report-file", dest="report_file", default="report.json", help="报告文件名")
    notify_parser.add_argument("--user-code", dest="user_code", default="")
    notify_parser.add_argument("--refresh-token-file", dest="refresh_token_file", default="../refresh_token_file.json")

    report_parser = subparsers.add_parser("report", help="单个平台生成报告")
    report_parser.add_argument("--server", help="Jenkins Server", required=True)
    report_parser.add_argument("--username", help="Jenkins Username", required=True)
    report_parser.add_argument("--token", help="Jenkins Token", required=True)
    report_parser.add_argument("--job-name", dest="job_name", help="任务名称(平台)", required=True)
    report_parser.add_argument("--job-id", dest="job_id", help="任务编号", required=True)
    report_parser.add_argument("--subtype", default="", help="回归类型: full | compatibility | upgrade")
    report_parser.add_argument("--test-repo", default="", dest="test_repo", help="测试仓库")
    report_parser.add_argument("--report-file", dest="report_file", default="report.json", help="报告文件名")

    document_parser = subparsers.add_parser("document", help="生成文档")
    document_parser.add_argument("--app-id", dest="app_id", required=True)
    document_parser.add_argument("--app-secret", dest="app_secret", required=True)
    document_parser.add_argument("--user-code", dest="user_code", default="")
    document_parser.add_argument("--refresh-token-file", dest="refresh_token_file", default="../refresh_token_file.json")
    document_parser.add_argument("--report-file", dest="report_file", default="report.json", help="报告文件名")
    document_parser.add_argument("--csv-file", dest="csv_file", default="csv_file.csv", help="报告文件名")
    document_parser.add_argument("--space-id", dest="space_id", default="6991710376987541505", help="空间ID: CTO OFFICE - 质量部")
    document_parser.add_argument("--parent-wiki-token", dest="parent_wiki_token", default="wikcnqQ8aPGnqwIXOqtOV4Kh5Tg", help="文档挂靠路径")

    issue_parser = subparsers.add_parser("issue", help="创建Issue")
    issue_parser.add_argument("--jira-server", dest="jira_server", help="服务链接")
    issue_parser.add_argument("--jira-user", dest="jira_user", help="账号")
    issue_parser.add_argument("--jira-token", dest="jira_token", help="token")
    issue_parser.add_argument("--report-file", dest="report_file", default="report.json", help="报告文件名")
    issue_parser.add_argument("--project-id", dest="project_id", default="10040", help="Jira项目ID")
    issue_parser.add_argument("--issue-type-id", dest="issue_type_id", default="10002", help="Jira Issue 类型ID")
    issue_parser.add_argument("--label", dest="label", action='append', help="Jira Issue 标签")
    issue_parser.add_argument("--priority-id", dest="priority_id", default="4", help="Jira Issue 优先级")
    issue_parser.add_argument("--title", dest="title", default="[Quard][失败用例][{regtype_cn}] {test_case}", help="Jira Issue 优先级")

    args = parser.parse_args()
    if args.command is None:
        parser.print_help()
        return

    if args.command == "build":
        build(
            server=args.server,
            username=args.username,
            token=args.token,
            job_name=args.job_name,
            job_url=args.job_url,
            parameters=json.loads(args.parameters),
            input_submit_parameters=json.loads(args.input_submit_parameters) if args.input_submit_parameters else None,
            download_type=PipelineDownloadType[args.download_type],
            version=args.version,
            regtype=args.regtype,
            repo=args.repo
        )

    if args.command == "report":
        report(
            server=args.server,
            username=args.username,
            token=args.token,
            job_name=args.job_name,
            job_id=args.job_id,
            subtype_en=ReportSubTypeEN[args.subtype],
            subtype_cn=ReportSubTypeCN[args.subtype],
            test_repo=args.test_repo,
            report_file=args.report_file
        )

    if args.command == "document":
        # 写飞书文档
        document(
            app_id=args.app_id,
            app_secret=args.app_secret,
            report_file=args.report_file,
            user_code=args.user_code,
            refresh_token_file=args.refresh_token_file,
            space_id=args.space_id,
            parent_wiki_token=args.parent_wiki_token,
            csv_file=args.csv_file
        )

    if args.command == "notify":
        # 读取report.json获取需要的字段, 发送结果.
        notify(
            app_id=args.app_id,
            app_secret=args.app_secret,
            user_email=args.user_email,
            chat_name=args.chat_name,
            report_file=args.report_file,
            notify_typo=FeishuNotifyTypo[args.type],
            webhook_url=args.webhook,
            user_code=args.user_code,
            refresh_token_file=args.refresh_token_file
        )

    if args.command == "issue":
        # 1. 创建issue(quard分支信息、ke分支信息、ke_commit信息、quard标签).
        # 2. 自动将issue编号回写到代码仓库.
        # 3. 监控issue状态, 关闭时触发任务重跑.
        issue(
            jira_server=args.jira_server,
            jira_user=args.jira_user,
            jira_token=args.jira_token,
            report_file=args.report_file,
            project_id=args.project_id,
            issue_type_id=args.issue_type_id,
            labels=args.label,
            priority_id=args.priority_id
        )


if __name__ == '__main__':
    main()
