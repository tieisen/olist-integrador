
import os
import logging
import smtplib
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Email:

    def __init__(self):
        self.email_body_path = os.getenv('BODY_HTML')
        self.smtp_server = os.getenv('SMTP_SERVER')
        self.smtp_port = os.getenv('SMTP_PORT')
        self.email = os.getenv('SENDER_MAIL')
        self.pwd = os.getenv('SENDER_PASSWORD')
        self.default_to = os.getenv('TO_DEFAULT')
        self.path_logs = os.getenv('PATH_LOGS')
        
    async def enviar(self,destinatario:str=None, corpo=None, assunto:str=None):
        """
        Envia um e-mail usando SMTP
        
        Parâmetros:
        - destinatario: str - Endereço de e-mail do destinatário
        - corpo: str - Corpo do e-mail
        """

        # Criando o objeto da mensagem
        msg = MIMEMultipart()
        msg['From'] = self.email
        msg['To'] = destinatario
        msg['Subject'] = assunto
        msg.attach(MIMEText(corpo, 'html'))

        try:
            # Conectando ao servidor SMTP com SSL
            servidor = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
            servidor.login(self.email, self.pwd)
            servidor.send_message(msg)
            servidor.quit()        
        except Exception as e:
            logger.error("Falha ao enviar e-mail: %s",e)

    async def notificar(self, destinatario:str=None, tipo:str='erro'):

        assunto = None
        if not os.path.exists(self.email_body_path):
            logger.error("Arquivo não encontrado em %s.",self.email_body_path)            
        else:
            with open(file=self.email_body_path, mode='r', encoding='utf-8') as f:
                body = f.read() 

        if not os.path.exists(self.path_logs):
            logger.error("Arquivo não encontrado em %s.",self.path_logs)            
        else:
            with open(file=self.path_logs, mode='r', encoding='utf-8') as f:
                log_data = f.readlines()

        match tipo:
            case 'erro':
                assunto = os.getenv('SUBJECT_ERROR').get('text')
                cor = os.getenv('SUBJECT_ERROR').get('color')
            case 'alerta':
                assunto = os.getenv('SUBJECT_WARN').get('text')
                cor = os.getenv('SUBJECT_WARN').get('color')
            case _:
                None
        
        if body and log_data and assunto:
            await self.enviar(destinatario=destinatario or self.default_to,
                              corpo=body.format(cor,log_data[-1]),
                              assunto=assunto)