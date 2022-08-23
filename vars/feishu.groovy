def call(ok){
 sh """
   curl https://open.feishu.cn/open-apis/bot/v2/hook/6619d514-cc57-4a96-b4c1-a2261f1d3810 \
      -H 'Content-type: application/json' \
      -d '{"msg_type":"post","content":{"post":{"zh_cn":{"title":"构建项目：${env.JOB_NAME}","content":[[{"tag":"text","text":"通知构建：${ok}\\n- 执行人：${currentBuild.buildCauses.shortDescription}\\n- 持续时间: ${currentBuild.durationString}\\n"},{"tag":"a","text":"当前构建编号：${currentBuild.displayName}，查看点击我\\n","href":"${env.BUILD_URL}"},{"tag":"at","user_id":"all"}]]}}}}'
"""
}