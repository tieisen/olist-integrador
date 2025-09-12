import os
import time
import logging
import requests
from dotenv import load_dotenv

from src.utils.decorador.olist import ensure_token
from src.parser.estoque import Estoque as ParserEstoque
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Estoque:

    def __init__(self, codemp:int=None, empresa_id:int=None):  
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.token = None
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_ESTOQUES')
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))
        
    @ensure_token
    async def buscar(
            self,
            id:int=None,
            lista_produtos:list=None
        ) -> bool:
        
        if not any([id,lista_produtos]):
            print("Produto não informado.")
            logger.error("Produto não informado.")
            return False

        result = []

        if id:
            url = self.endpoint+f"/{id}"
            res = requests.get(
                url=url,
                headers={
                    "Authorization":f"Bearer {self.token}",
                    "Content-Type":"application/json",
                    "Accept":"application/json"
                }
            )

            if res.status_code == 200:
                result.append(res.json())                
            else:                      
                print(f"Erro {res.status_code}: {res.json().get('mensagem','Erro desconhecido')} cod {id}")
                logger.error("Erro %s: %s cod %s", res.status_code, res.json().get("mensagem","Erro desconhecido"), id)
                return result            

        if lista_produtos:
            for produto in lista_produtos:
                url = self.endpoint+f"/{produto}"
                time.sleep(self.req_time_sleep)
                res = requests.get(
                    url=url,
                    headers={
                        "Authorization":f"Bearer {self.token}",
                        "Content-Type":"application/json",
                        "Accept":"application/json"
                    }
                )

                if res.status_code == 200:
                    result.append(res.json())
                else: 
                    print(f"Erro {res.status_code}: {res.json().get('mensagem','Erro desconhecido')} cod {id}")                     
                    logger.error("Erro %s: %s cod %s", res.status_code, res.json().get("mensagem","Erro desconhecido"), id)
                    return result
        
        return result
    
    @ensure_token
    async def enviar_saldo(
            self,
            id:int=None,
            data:dict=None,
            lista_dados:list=None
        ) -> list:
        
        if not all([id,data]) and not lista_dados:
            print("Dados não informados.")
            logger.error("Dados não informados.")
            return False

        url = None

        if id and data:
            url = self.endpoint+f"/{id}"
            res = requests.post(url=url,
                                headers={"Authorization":f"Bearer {self.token}",
                                         "Content-Type":"application/json",
                                         "Accept":"application/json"},
                                json=data)
            if res.status_code == 200:
                data['sucesso'] = True
            else:
                print(f"Erro {res.status_code}: {res.json()} cod {id}")
                logger.error("Erro %s: %s cod %s", res.status_code, res.json(), id)
                data['sucesso'] = False
            return data
        
        if lista_dados:
            parser = ParserEstoque()
            for dados in lista_dados:
                if dados.get('variacao') == 0:
                    dados['sucesso'] = True
                    continue

                id_produto, payload = parser.to_olist(dados.get('ajuste_estoque'))
                if not all([id_produto, payload]):
                    dados['sucesso'] = False
                    print(f"Erro ao preparar dados para o produto cod {dados['ajuste_estoque'].get('id')}")
                    logger.error("Erro ao preparar dados para o produto cod %s", dados['ajuste_estoque'].get('id'))
                    continue

                url = self.endpoint+f"/{id_produto}"
                time.sleep(self.req_time_sleep)
                res = requests.post(url=url,
                                    headers={
                                        "Authorization":f"Bearer {self.token}",
                                        "Content-Type":"application/json",
                                        "Accept":"application/json"
                                    },
                                    json=payload)                
                if res.status_code == 200:
                    dados['sucesso'] = True
                else:
                    dados['sucesso'] = False
                    print(f"Erro {res.status_code}: cod {id_produto}")
                    logger.error("Erro %s: cod %s", res.status_code, id_produto)
            return lista_dados
