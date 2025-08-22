import os
import logging
import smtplib
from dotenv import load_dotenv
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from database.crud import log, log_estoque, log_pedido, log_produto
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

    def buscar_historico(self):
        historico = log.buscar_falhas()

        body = ''

        body+='<ul style="list-style: none;">'
        for h in historico:
            body+=f'<li>{h.dh_execucao.strftime('%H:%M')} | {h.contexto.capitalize()} | {h.de.capitalize()} > {h.para.capitalize()}</li>'
            match h.contexto:
                case 'estoque':            
                    estoque = log_estoque.buscar_id(log_id=h.id)
                    if not estoque:
                        body+='<ul><li>Detalhamento não encontrado</li></ul>'
                    else:
                        body+='<ul>'
                        for e in estoque:
                            if bool(e.status_estoque):
                                continue
                            body+=f'<li>{e.obs}</li>'
                        body+='</ul>'
                case 'pedido':
                    pedido = log_pedido.buscar_id(log_id=h.id)
                    if not pedido:
                        body+='<ul><li>Detalhamento não encontrado</li></ul>'
                    else:
                        body+='<ul>'
                        for pd in pedido:
                            if bool(pd.status):
                                continue
                            body+=f'<li>{pd.obs}</li>'
                        body+='</ul>'
                case 'produto':
                    produto = log_produto.buscar_id(log_id=h.id)
                    if not produto:
                        body+='<ul><li>Detalhamento não encontrado</li></ul>'
                    else:
                        body+='<ul>'
                        for p in produto:
                            if bool(p.sucesso):
                                continue
                            body+=f'<li>{p.obs}</li>'
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
        except Exception as e:
            logger.error("Falha ao enviar e-mail: %s",e)

    async def notificar(self, destinatario:str=None, tipo:str='erro'):

        assunto = None
        if not os.path.exists(self.email_body_path):
            logger.error("Arquivo não encontrado em %s.",self.email_body_path)            
        else:
            with open(file=self.email_body_path, mode='r', encoding='utf-8') as f:
                estrutura = f.read()

        body = self.buscar_historico()

        assunto = os.getenv('MAIL_SUBJECT')
        cor = os.getenv('MAIL_COLOR')
        
        await self.enviar(destinatario=destinatario or self.default_to,
                          corpo=estrutura.format(cor,body),
                          assunto=assunto)