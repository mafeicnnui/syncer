#!/home/hopson/apps/usr/webserver/python3.6.0/bin/python3
# -*- coding:utf-8 -*-
import sys
import traceback
import os
import configparser
import time
import datetime
import smtplib
import pymysql
from email.mime.text import MIMEText

def send_mail(p_from_user,p_from_pass,p_to_user,p_title,p_content):    
    to_user=p_to_user.split(",")   
    try:
        msg = MIMEText(p_content,'html','utf-8')
        msg["Subject"] = p_title
        msg["From"]    = p_from_user
        msg["To"]      = ",".join(to_user)       
        server = smtplib.SMTP("smtp.exmail.qq.com", 25)
        server.set_debuglevel(0)
        server.login(p_from_user, p_from_pass)
        server.sendmail(p_from_user, to_user, msg.as_string())
        server.quit() 
    except smtplib.SMTPException as e:        
        print(e)
		
def main():
   send_user    = "190343@lifeat.cn"
   send_pass    = "Hhc5HBtAuYTPGHQ8"
   acpt_user    = "608520@hopson.com.cn,190205@lifeat.cn,609479@hopson.com.cn,609717@hopson.com.cn,190343@lifeat.cn"
   mail_title   = sys.argv[1]
   mail_content = sys.argv[2]
   send_mail(send_user,send_pass,acpt_user,mail_title,mail_content)
  
main()  
