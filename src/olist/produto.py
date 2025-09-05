import os
import time
import json
import logging
import requests
from datetime import datetime
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

class Produto:

    def __init__(self):  
        self.con = Connect() 
        self.req_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_PRODUTOS')
        self.fornecedor_padrao = int(os.getenv('OLIST_ID_FORN_PADRAO',753053887))

    async def buscar(self,id:int=None,sku:int=None) -> bool:

        if not any([id, sku]):
            logger.error("Produto não informado.")
            print("Produto não informado.")
            return False
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False
        
        if id:
            url = self.endpoint+f"/{id}"
        if sku:
            url = self.endpoint+f"/?codigo={sku}"
        
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False  

        res = requests.get(
            url=url,
            headers={
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )
        if res.status_code == 200:
            if sku:
                try:
                    id = res.json()['itens'][0].get('id')
                    url = self.endpoint+f"/{id}"
                    res = requests.get(
                        url=url,
                        headers={
                            "Authorization":f"Bearer {token}",
                            "Content-Type":"application/json",
                            "Accept":"application/json"
                        }
                    )
                except:
                    return False                
            return res.json()
        else:
            if id and not sku:
                print(f"Erro {res.status_code}: {res.json().get('mensagem','Erro desconhecido')} cod {id}")
                logger.error("Erro %s: %s cod %s", res.status_code, res.json().get("mensagem","Erro desconhecido"), id)
            if sku:
                print(f"Erro {res.status_code}: {res.json().get('mensagem','Erro desconhecido')} sku {sku}")
                logger.error("Erro %s: %s sku %s", res.status_code, res.json().get("mensagem","Erro desconhecido"), sku)
            return False

    async def incluir(self,data:dict=None) -> tuple[bool,dict]:

        if not data:
            logger.error("Dados do produto não informados.")
            print("Dados do produto não informados.")
            return False, {}
        
        if not isinstance(data, dict):
            logger.error("Dados do produto em formato inválido.")
            print("Dados do produto em formato inválido.")
            return False, {}

        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False
        
        url = self.endpoint
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False, {}

        try:
            res = requests.post(url=url,
                                headers={"Authorization":f"Bearer {token}",
                                        "Content-Type":"application/json",
                                        "Accept":"application/json"},
                                json=data)
        except Exception as e:
            logger.error("Erro relacionado à requisição. %s",e)
            return False, {}
        
        if res.status_code in [200,201]:                    
            return True, res.json()
        else:
            logger.error("Erro %s: %s cod %s", res.status_code, res.json(), res["sku"])
            print(f"Erro {res.status_code}: {res.json()} cod {data.get('sku')}")
            return False, {}

    async def atualizar(self,id:int=None,data:dict=None) -> bool:

        if not all([id, data]):
            logger.error("Dados do produto e ID não informados.")
            print("Dados do produto e ID não informados.")
            return False

        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False  

        url = self.endpoint+f"/{id}"

        if data.get('produtoPai'): # se for variacao
            url = self.endpoint+f"/{data.get('produtoPai').get('id')}/variacoes/{id}"

        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False 
        
        res = requests.put(url=url,
                        headers={
                            "Authorization":f"Bearer {token}",
                            "Content-Type":"application/json",
                            "Accept":"application/json"
                        },
                        json=data)
        
        if res.status_code == 204:                    
            return True
        if res.status_code in [404,409]:
            logger.warning("Erro %s: %s ID %s", res.status_code, res.json(), id)
            print(f"Erro {res.status_code}: {res.json()} ID {id}")
            logger.info(json.dumps(data))
            return True, 0        
        else:
            print(f"Erro {res.status_code}: {res.json()} ID {id}")
            logger.error("Erro %s: %s ID %s", res.status_code, res.json(), id)
            logger.info(json.dumps(data))
            return False

    async def buscar_todos(self) -> list:

        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False  

        status = 200
        paginacao = {}
        itens = []
        while status == 200:
            if paginacao:        
                if paginacao["limit"] + paginacao["offset"] < paginacao ["total"]:
                    offset = paginacao["limit"] + paginacao["offset"]
                    url = self.endpoint+f"?offset={offset}"
                else:
                    url = None
            else:
                url = self.endpoint
            if url:
                res = requests.get(url=url,
                                   headers={"Authorization":f"Bearer {token}",
                                            "Content-Type":"application/json",
                                            "Accept":"application/json"})
                status = res.status_code
                itens += res.json()["itens"]
                paginacao = res.json()["paginacao"]
                time.sleep(self.req_sleep)
            else:
                status=0

        return itens
    
    async def buscar_alteracoes(self,todo_historico:bool=False) -> list:
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False  
        
        data_alteracao = datetime.today().strftime("%Y-%m-%d 00:00:00")

        status = 200
        paginacao = {}
        itens = []
        while status == 200:
            if paginacao:        
                if paginacao["limit"] + paginacao["offset"] < paginacao ["total"]:
                    offset = paginacao["limit"] + paginacao["offset"]
                    if todo_historico:
                        url = self.endpoint+f"?offset={offset}"
                    else:
                        url = self.endpoint+f"?dataAlteracao={data_alteracao}&offset={offset}"
                else:
                    url = None
            else:
                if todo_historico:
                    url = self.endpoint
                else:
                    url = self.endpoint+f"?dataAlteracao={data_alteracao}"
            if url:
                res = requests.get(url=url,
                                   headers={"Authorization":f"Bearer {token}",
                                            "Content-Type":"application/json",
                                            "Accept":"application/json"})
                status = res.status_code
                itens += res.json()["itens"]
                paginacao = res.json()["paginacao"]
                time.sleep(self.req_sleep)
            else:
                status=0
      
        if itens:
            itens.sort(key=lambda i: i['dataAlteracao'])
            lista_itens = []
            for i in itens:
                if i["tipo"] == "S":
                    lista_itens.append({"id":i.get("id"),"sku":i.get("sku"),"dh_alter":i.get("dataAlteracao"),"situacao":i.get("situacao")})
            return lista_itens
        else:
            return []    