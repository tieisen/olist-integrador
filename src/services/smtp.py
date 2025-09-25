import os
import logging
import smtplib
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from database.crud import log
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

    def formatar_corpo(self,historico:list[dict]):

        body:str=''
        
        if not historico:
            return False
        body+='<ul style="list-style: none;">'
        for h in historico:
            body+=f'<li>{h.get('hora').strftime('%H:%M')} | {h.get('contexto').capitalize()} | {h.get('de').capitalize()} > {h.get('para').capitalize()}</li>'
            if not h.get('falhas'):
                body+='<ul><li>Detalhamento não encontrado</li></ul>'
            else:
                body+='<ul>'
                for f in h.get('falhas'):                    
                    body+=f'<li>{f.get('obs')}</li>' if not f.get('sucesso') else ''
                body+='</ul>'
        body+='</ul>'
        return body

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
            print(f"E-mail enviado com sucesso para {destinatario}!")
            return True
        except Exception as e:
            msg = f"Falha ao enviar e-mail: {e}"
            logger.error(msg)
            print(msg)
            return False

    async def notificar(self, empresa_id:int, destinatario:str=None):

        historico:list[dict]=[]
        corpo:str=''

        historico = await log.listar_falhas(empresa_id=empresa_id)
        corpo = self.formatar_corpo(historico=historico)
        if not corpo:
            return False
        
        assunto = None
        if not os.path.exists(self.email_body_path):
            logger.error("Arquivo não encontrado em %s.",self.email_body_path)            
        else:
            with open(file=self.email_body_path, mode='r', encoding='utf-8') as f:
                estrutura = f.read()

        if not estrutura:
            return False
        
        assunto = '[TESTE] '+os.getenv('MAIL_SUBJECT')
        cor = os.getenv('MAIL_COLOR')
        
        ack = await self.enviar(destinatario=destinatario or self.default_to,
                                corpo=estrutura.format(cor,corpo),
                                assunto=assunto)
        return ack