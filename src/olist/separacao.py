import os
import logging
import requests
from dotenv import load_dotenv

#from src.utils.decorador.olist import token_olist
from src.utils.decorador import token_olist
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Separacao:

    def __init__(self, codemp:int=None, empresa_id:int=None):  
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.token = None
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_SEPARACAO')

    @token_olist
    async def listar(self) -> list:
        
        url = [ self.endpoint+"/?situacao=1",  # Aguardando Separacao
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
                    "Authorization":f"Bearer {self.token}",
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
            
            lista+=res.json().get('itens',[])

        return lista if status else status        

    @token_olist
    async def buscar(
            self,
            id:int
        ) -> bool:

        url = self.endpoint+f"/{id}"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False 

        res = requests.get(
            url = url,
            headers = {
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code != 200:
            logger.error("Erro %s: %s", res.status_code, res.text)
            print(f"Erro {res.status_code}: {res.text}")
            return False
        
        return res.json()
    
    @token_olist
    async def separar(
            self,
            id:int
        ) -> bool:

        url = self.endpoint+f"/{id}/situacao"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False 

        res = requests.put(
            url = url,
            headers = {
                "Authorization":f"Bearer {self.token}",
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

    @token_olist
    async def concluir(
            self,
            id:int
        ) -> bool:

        url = self.endpoint+f"/{id}/situacao"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False 

        res = requests.put(
            url = url,
            headers = {
                "Authorization":f"Bearer {self.token}",
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