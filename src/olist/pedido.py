import os
import time
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from src.olist.connect import Connect
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Pedido:

    def __init__(self):  
        self.con = Connect() 
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_PEDIDOS')
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))        

    async def buscar(self,id:int=None,codigo:str=None) -> bool:

        if not any([id, codigo]):
            logger.error("Pedido não informado.")
            print("Pedido não informado.")
            return False

        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False  

        if id:
            url = self.endpoint+f"/{id}"
        if codigo:
            url = self.endpoint+f"/?numeroPedidoEcommerce={codigo}"

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
            if codigo:
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
                if res.json().get('situacao') == 8:
                    print(f"Pedido {res.json().get('numeroPedido')} dados incompletos")
                    logger.warning("Pedido %s dados incompletos", res.json().get('numeroPedido'))
                    return False
            return res.json()
        else:
            if id:
                print(f"Erro {res.status_code}: {res.json().get('mensagem','Erro desconhecido')} pedido {id}")
                logger.error("Erro %s: %s pedido %s", res.status_code, res.json().get("mensagem","Erro desconhecido"), id)
            if codigo:
                print(f"Erro {res.status_code}: {res.json().get('mensagem','Erro desconhecido')} pedido {codigo}")
                logger.error("Erro %s: %s pedido %s", res.status_code, res.json().get("mensagem","Erro desconhecido"), codigo)
            return False

    async def validar_kit(self,id:int=None,item_no_pedido:dict=None) -> tuple[bool,dict]:
        
        if not all([id, item_no_pedido]):
            logger.error("Produto e item do pedido não informados.")
            print("Produto e item do pedido não informados.")
            return False, {}
        
        if isinstance(item_no_pedido, list):
            item_no_pedido = item_no_pedido[0]
        
        if not isinstance(item_no_pedido, dict):
            logger.error("Item do pedido precisa ser um dicionário.")
            print("Item do pedido precisa ser um dicionário.")
            return False, {}
        
        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False, {} 
        
        url = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_PRODUTOS')+f"/{id}"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False, {}
             
        res = requests.get(
            url=url,
            headers={
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code == 200:
            if res.json().get('tipo') == 'K':
                qtd_kit = item_no_pedido["quantidade"]
                vlt_kit = item_no_pedido["valorUnitario"]
                res_item = []
                for k in res.json().get('kit'):
                    kit_item = {
                        "produto": {
                            "id": k['produto'].get('id'),
                            "sku": k['produto'].get('sku'),
                            "descricao": k['produto'].get('descricao')
                        },
                        "quantidade": k.get('quantidade') * qtd_kit,
                        "valorUnitario": round(vlt_kit / len(res.json().get('kit')),4),
                        "infoAdicional": ""                                    
                    }
                    res_item.append(kit_item)                            
                return True, res_item
            else:
                return False, {}
        else:                      
            print(f"Erro {res.status_code}: {res.json().get('mensagem','Erro desconhecido')} cod {id}")
            logger.error("Erro %s: %s cod %s", res.status_code, res.json().get("mensagem","Erro desconhecido"), id)
            return False, {}
        
    async def buscar_novos(self, atual:bool = True) -> tuple[bool, list]:
        """ Busca pedidos do Olist a partir de uma data inicial.
        Args:
            atual (bool): Se True, busca pedidos a partir da última data registrada. Se False, busca pedidos a partir de uma data fixa.
        Returns:
            tuple: (bool, list) - Retorna True e a lista de IDs de pedidos encontrados ou False e uma lista vazia em caso de erro.
        """

        dataInicial = None

        # Busca a data da última importação de pedidos
        # Se não houver, define a data inicial como 3 dias atrás
        if atual:
            dataInicial = (datetime.today()-timedelta(days=3)).strftime('%Y-%m-%d')
        else:
            dataInicial = '2025-07-20'  # Data fixa para buscar pedidos
            
        if not dataInicial:
            print("Erro ao buscar a data da última venda.")
            logger.error("Erro ao buscar a data da última venda.")
            return False, []

        try:
            token = self.con.get_token()
        except Exception as e:
            print(f"Erro relacionado ao token de acesso. {e}")
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False, []

        status = 200
        paginacao = {}
        itens = []            
        while status == 200:
            # Verifica se há paginação
            if paginacao:        
                if paginacao["limit"] + paginacao["offset"] < paginacao ["total"]:
                    offset = paginacao["limit"] + paginacao["offset"]
                    url = self.endpoint+f"?dataInicial={dataInicial}&offset={offset}"
                else:
                    url = None
            else:
                url = self.endpoint+f"?dataInicial={dataInicial}"
            if url:
                res = requests.get(url=url,
                                   headers={
                                       "Authorization":f"Bearer {token}",
                                       "Content-Type":"application/json",
                                       "Accept":"application/json"
                                   })
                status=res.status_code
                itens += res.json()["itens"]
                paginacao = res.json()["paginacao"]
                time.sleep(self.req_time_sleep)
            else:
                status = 0

        if not itens:
            print("Nenhum pedido encontrado.")
            logger.info("Nenhum pedido encontrado.")
            return True, []

        # Ordena os itens por ID        
        itens.sort(key=lambda i: i['id'])
        return True, [p["id"] for p in itens]    