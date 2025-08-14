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

class Separacao:

    def __init__(self):  
        self.con = Connect()
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_SEPARACAO')

    def extrair_lista(self, res:dict) -> list:

        if not isinstance(res, dict):
            logger.error("Retorno da API não informado.")
            print("Retorno da API não informado.")
            return False

        if not res.get('itens'):
            logger.error("Retorno da API não possui itens.")
            print("Retorno da API não possui itens.")
            return False

        lista = []

        try:
            for item in res.get('itens'):
                lista.append({
                    "id_pedido" : item['venda'].get('id'),
                    "id_separacao" : item.get('id')
                })
            return lista
        except Exception as e:
            logger.error("Erro ao extrair lista. %s",e)
            print("Erro ao extrair lista.")
            return False

    async def listar(self) -> list:
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False

        url = [ self.endpoint+"/?situacao=1",  # Aguardando Separacao
                self.endpoint+"/?situacao=2",  # Separada
                self.endpoint+"/?situacao=4" ] # Em Separacao
        
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False        

        status = True
        lista = []
        for u in url:
            res = requests.get(
                url = u,
                headers = {
                    "Authorization":f"Bearer {token}",
                    "Content-Type":"application/json",
                    "Accept":"application/json"
                }
            )

            if res.status_code != 200:
                status == False
                logger.error("Erro %s: %s", res.status_code, res.text)
                print(f"Erro {res.status_code}: {res.text}")
                continue

            if res.status_code == 200 and not res.json().get('itens'):
                continue

            lista+=self.extrair_lista(res.json())

        return lista if status else status        

    async def buscar(self, id:int=None) -> bool:

        if not id:
            logger.error("ID da separação não informado.")
            print("ID da separação não informado.")
            return False
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False

        url = self.endpoint+f"/{id}"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
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
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        return res.json()

    async def separar(self, id:int=None) -> bool:

        if not id:
            logger.error("ID da separação não informado.")
            print("ID da separação não informado.")
            return False
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False

        url = self.endpoint+f"/{id}/situacao"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False 

        res = requests.put(
            url = url,
            headers = {
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            json={
                "situacao": 2 # Separada
            }
        )

        if res.status_code != 204:
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        return True

    async def concluir(self, id:int=None) -> bool:

        if not id:
            logger.error("ID da separação não informado.")
            print("ID da separação não informado.")
            return False
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False

        url = self.endpoint+f"/{id}/situacao"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False 

        res = requests.put(
            url = url,
            headers = {
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            json={
                "situacao": 3 # Embalada
            }
        )

        if res.status_code != 204:
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        return True