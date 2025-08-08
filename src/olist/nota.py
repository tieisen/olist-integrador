import os
import logging
import requests
from dotenv import load_dotenv
from src.olist.connect import Connect
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Nota:

    def __init__(self):  
        self.con = Connect()
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_NOTAS')

    async def buscar(self, id:int=None, id_ecommerce:str=None) -> bool:

        if not any([id, id_ecommerce]):
            logger.error("Nota não informada.")
            print("Nota não informada.")
            return False
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False

        if id:
            url_ = self.endpoint+f"/{id}"
            id_ecommerce = None
        
        if id_ecommerce:
            url_ = self.endpoint+f"/?numeroPedidoEcommerce={id_ecommerce}"
            id = None             

        res = requests.get(
            url = url_,
            headers = {
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        nota = None
        if res.status_code != 200:
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        if res.status_code == 200 and id and res.json().get('itens'):
            nota = res.json()
        
        if res.status_code == 200 and id_ecommerce and res.json().get('itens'):
            url_id = self.endpoint+f"/{res.json().get('itens')[0].get('id')}"
            res = requests.get(
                url = url_id,
                headers = {
                    "Authorization":f"Bearer {token}",
                    "Content-Type":"application/json",
                    "Accept":"application/json"
                }
            )
            if res.status_code == 200 and url_id:
                nota = res.json()
                
        if nota:
            return nota
        else:
            print("Nota cancelada")
            logger.error("Nota cancelada")
            return False

    async def emitir(self, id:int=None):

        if not id:
            logger.error("ID não informado.")
            print("ID não informado.")
            return False
        
        url = self.endpoint+f"/{id}/emitir"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False        
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False
        
        res = requests.post(
            url = url,
            headers = {
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            json={"enviarEmail":False}
        )

        if res.status_code != 200:
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        if res.status_code == 200:
            return res.json()        

    async def buscar_financeiro(self, serie:str=None, numero:str=None) -> bool:

        if not all([serie, numero]):
            logger.error("Nota não informada.")
            print("Nota não informada.")
            return False
        
        url = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_FINANCEIRO')+f"?numeroDocumento={serie}{numero}/01"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False                
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False             

        res = requests.get(
            url = url,            
            headers = {
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code != 200:
            logger.error("Erro %s: %s fin %s", res.status_code, res.text, f"{serie}{numero}/01")
            print(f"Erro {res.status_code}: {res.text} fin {serie}{numero}/01")
            return False
        
        if res.status_code == 200:
            return res.json().get('itens')[0]
            # print(res.json())
            # url = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_FINANCEIRO')+f"/{res.json().get('itens')[0].get('id')}"
            # res_ = requests.get(
            #     url = url,            
            #     headers = {
            #         "Authorization":f"Bearer {token}",
            #         "Content-Type":"application/json",
            #         "Accept":"application/json"
            #     }
            # )
            # if res.status_code != 200:
            #     logger.error("Erro %s: %s fin %s", res.status_code, res.text, f"{serie}{numero}/01")
            #     return res_.json().get('itens')[0]
            # else:
            #     return res_.json()

    async def baixar_financeiro(self, id:int=None, valor:float=None) -> bool:

        if not all([id, valor]):
            logger.error("Dados do financeiro e da nota não informados.")
            print("Dados do financeiro e da nota não informados.")
            return False

        url = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_FINANCEIRO')+f"/{id}/baixar"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False        
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False             

        payload = {
            "contaDestino": {
                "id": 334742438
            },
            "data": None,
            "categoria": {
                "id": 347787528
            },
            "historico": None,
            "taxa": None,
            "juros": None,
            "desconto": None,
            "valorPago": valor,
            "acrescimo": None
        }

        res = requests.post(
            url = url,            
            headers = {
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            json=payload
        )

        if res.status_code == 409:
            logger.error("Financeiro da nota já foi baixado")
            print(f"Financeiro da nota já foi baixado")
            return None
        
        if res.status_code == 204:
            logger.info("Financeiro da nota baixado com sucesso")
            print(f"Financeiro da nota baixado com sucesso")
            return True
        
        if res.status_code not in (409,204):
            logger.error("Erro %s: %s fin %s", res.status_code, res.text, id)            
            print(f"Erro {res.status_code}: {res.text}")
            return False