from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import subprocess


class SMTPClient:
    def __init__(self, login, password, host='smtp.beget.com', port=2525):
        self.__login = login
        self.__password = password
        self.__smtp_server = smtplib.SMTP(host, port)
        self.__smtp_server.starttls()

    def __make_msg(self, subject, message, email):
        self.__msg = MIMEMultipart()
        self.__msg['From'] = self.__login
        self.__msg['To'] = email
        self.__msg['Subject'] = subject
        self.__msg.attach(MIMEText(message, 'html'))

    def __send_message(self, subject, message, email):
        self.__make_msg(subject, message, email)
        self.__smtp_server.login(self.__login, self.__password)
        self.__smtp_server.sendmail(self.__msg['From'], self.__msg['To'], self.__msg.as_string())
        print(f'Will be sent to email on address: {email}')

    def send_message(self, subject, message, emails):
        try:
            for email in emails:
                self.__send_message(subject, message, email)
        finally:
            self.__smtp_server.quit()


if __name__ == '__main__':
    emails = ['borisostroumov@gmail.com']
    my_text ="<html>" \
  "<head></head>" \
  "<body>" \
  "<p><b>{0} {1} {2}</b>, Вас зарегистрировали в приложении NSClean</p>" \
    "<p>Ваши авторизационные данные:<br>" \
       "Логин: <b>{3}</b><br>" \
       "Пароль: <b>{4}</b>" \
    "</p>" \
  "</body>" \
"</html>"
    SMTPClient = SMTPClient('nsclean@neweducations.online', '290590120nN')
    SMTPClient.send_message('Успешная регистрация в приложении NSClean', my_text.format('Остроумов', 'Борис', 'Артемович', 'lokasan@inbox.ru', 'sdsdoOIH883'), emails)
