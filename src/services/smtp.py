import os
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from database.crud import log
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Email:

    def __init__(self):
        self.email_body_path = os.getenv('BODY_HTML')
        self.smtp_server = os.getenv('SMTP_SERVER')
        self.smtp_port = os.getenv('SMTP_PORT')
        self.email = os.getenv('SENDER_MAIL')
        self.pwd = os.getenv('SENDER_PASSWORD')
        self.default_to = os.getenv('TO_DEFAULT')

    def formatar_corpo(self,historico:list[dict]) -> str:
        """
        Formata o corpo do e-mail
            :param historico: Histórico de falhas
        """

        body:str=''

        try:
            if not isinstance(historico,list):
                raise ValueError("historico não é uma lista")
            
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
        except Exception as e:
            logger.error("Erro ao formatar o corpo do e-mail: %s",e)
        finally:
            pass

        return body

    async def enviar(self,destinatario:str=None, corpo=None, assunto:str=None) -> bool:
        """
        Envia um e-mail usando SMTP
            :param destinatario: Endereço de e-mail do destinatário
            :param corpo: Corpo do e-mail
            :param assunto: Assunto do e-mail
        """

        ok:bool=True
        
        msg = MIMEMultipart()
        msg['From'] = self.email
        msg['To'] = destinatario
        msg['Subject'] = assunto
        msg.attach(MIMEText(corpo, 'html'))

        try:
            servidor = smtplib.SMTP_SSL(self.smtp_server, self.smtp_port)
            servidor.login(self.email, self.pwd)
            servidor.send_message(msg)
            servidor.quit()
            logger.info(f"E-mail enviado com sucesso para {destinatario}!")            
        except Exception as e:
            logger.error(f"Falha ao enviar e-mail: {e}")
            ok = False
        finally:
            pass
        
        return ok


    async def notificar(self, empresa_id:int, destinatario:str=None):

        historico:list[dict]=[]
        corpo:str=''
        ok:bool=True

        try:
            assunto = os.getenv('MAIL_SUBJECT')
            cor = os.getenv('MAIL_COLOR')
            tempo = os.getenv('TEMPO_HISTORICO_MINUTOS')
        
            if not all([assunto,cor,tempo]):
                raise ValueError("Assunto, cor ou tempo de histórico não encontrado na variáveis de ambiente.")

            historico = await log.listar_falhas(empresa_id=empresa_id)
            if not historico:
                return ok
        
            corpo = self.formatar_corpo(historico=historico)
            if not corpo:
                raise Exception("Falha ao formatar corpo do e-mail.")
            
            if not os.path.exists(self.email_body_path):
                raise FileNotFoundError(f"Arquivo não encontrado em {self.email_body_path}.") 
            
            with open(file=self.email_body_path, mode='r', encoding='utf-8') as f:
                estrutura = f.read()
            if not estrutura:
                raise Exception(f"Arquivo do corpo do e-mail está vazio em {self.email_body_path}.")
            
            ok = await self.enviar(destinatario=destinatario or self.default_to,
                                   corpo=estrutura.format(cor,corpo,tempo),
                                   assunto=assunto)
        except Exception as e:
            ok = False
            logger.error(f"Erro ao notificar falhas:{e}")
        finally:
            pass
        return ok
