'''
--------------------------------------------------------------------
项目名：rwp
模块名：opr.memail
本模块主要用于邮件发送
--------------------------------------------------------------------
python = 3.6
--------------------------------------------------------------------
'''
import sys
import smtplib
from email.mime.text import MIMEText
from email.header import Header
import json as js


# 第三方 SMTP 服务
def send_email(title,content,configpath):
    '''发送邮件

    输入参数
    -------
    title : `str`
        邮件标题
    content : `str`
        邮件内容
    configpath : `str`
        配置文件路径

    返回值
    -----
    `bool` 若发送成功则返回True，否则返回False
    '''

    receiver_name = 'debugger'
    sender_name = 'preskymonitor'

    with open(configpath) as f:
        config = js.load(f)

    mail_host=config['email']['send_host']  # 设置服务器
    mail_user=config['email']['account']    # 用户名
    mail_pass=config['email']['password']   # 密码

    sender = config['email']['account']
    receivers = [config['email']['receive_address']]

    message = MIMEText(content, 'plain', 'utf-8')
    message['From'] = Header(sender_name, 'utf-8')
    message['To'] =  Header(receiver_name, 'utf-8')

    subject = title
    message['Subject'] = Header(subject, 'utf-8')


    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, 25)    # 25 为 SMTP 端口号
        smtpObj.login(mail_user,mail_pass)
        smtpObj.sendmail(sender, receivers, message.as_string())
        print("邮件发送成功")
        return True

    except smtplib.SMTPException:
        print("Error: 无法发送邮件")
        return False

if __name__ == '__main__':
    title, content = sys.argv[1], sys.argv[2]
    send_email(title,content)
