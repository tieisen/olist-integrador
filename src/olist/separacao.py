import os
import time
import requests
from src.utils.autenticador import token_olist
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Separacao:

    def __init__(self, codemp:int=None, empresa_id:int=None):  
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.token = None
        self.req_time_sleep:float = float(os.getenv('REQ_TIME_SLEEP',1.5))
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_SEPARACAO')

    @token_olist
    async def listar(self) -> list[dict]:
        """
        Busca lista de pedidos com status Aguardando separação e Em separação.
            :return list[dict]: lista de dicionários com os dados resumidos das separações
        """
        
        url = [ self.endpoint+"/?situacao=1",  # Aguardando Separacao
                self.endpoint+"/?situacao=4" ] # Em Separacao
        
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False        

        status = True
        lista = []
        for u in url:
            time.sleep(self.req_time_sleep)
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
        ) -> dict:
        """
        Busca os dados de uma separação.
            :params id: ID da separação
            :return dict: Dicionário com os dados da separação
        """

        url = self.endpoint+f"/{id}"
        if not url:
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
            return False
        
        return res.json()
    
    @token_olist
    async def separar(
            self,
            id:int
        ) -> bool:
        """
        Atualiza o status da separação de um pedido para Separado.
            :params id: ID da separação
            :return bool: status da operação
        """        

        url = self.endpoint+f"/{id}/situacao"
        if not url:
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
            return False
        
        return True

    @token_olist
    async def concluir(
            self,
            id:int
        ) -> bool:
        """
        Atualiza o status da separação de um pedido para Embalada (checkout).
            :params id: ID da separação
            :return bool: status da operação
        """        

        url = self.endpoint+f"/{id}/situacao"
        if not url:
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
            return False
        
        return True