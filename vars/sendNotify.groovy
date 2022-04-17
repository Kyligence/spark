def call(String way,String subject,String to,String body,String from=null) {

    switch(way) {
        case 'EMAIL':
            sendEmail(subject,to,body,from)
        break
        case 'FEISHU':
            sendFeishu(subject,to,body,from)
        break
        case 'SMS':
            sendSms(subject,to,body,from)
        break
    }
}

// way == EMAIL
def sendEmail(subject,to,body,from=null) {
    emailext (
        subject: subject,
        to: to,
        from: from ?: 'devops_support@kyligence.io',
        //定义html 模版
        body: """${body}
            <br />
            <hr />
            <p>Devops Jenkins</p>
        """
    )
}

def sendFeishu(subject,to,body,from) {
    println "飞书提醒还未实现，情景期待"
}

def sendSms(subject,to,body,from) {
    println "短信提醒还未实现，情景期待"
}