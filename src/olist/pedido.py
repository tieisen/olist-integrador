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

    async def buscar(self,id:int=None,codigo:str=None,numero:int=None,cancelados:bool=False):

        if not cancelados and not any([id, codigo, numero]):
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
        if numero:
            url = self.endpoint+f"/?numero={numero}"
        if cancelados:
            url = self.endpoint+f"/?situacao=2&dataInicial={(datetime.today()-timedelta(days=7)).strftime('%Y-%m-%d')}"            

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
            if id:
                return res.json()
            if codigo or numero:
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
                    return res.json()
                except:
                    return False
            if not cancelados and res.json().get('situacao') == 8:
                print(f"Pedido {res.json().get('numeroPedido')} dados incompletos")
                logger.warning("Pedido %s dados incompletos", res.json().get('numeroPedido'))
                return False
            if cancelados:
                if res.json().get('itens'):
                    return [i.get('id') for i in res.json().get('itens')]
                else:
                    return []            
        else:
            if id:
                print(f"Erro {res.status_code}: {res.text} pedido {id}")
                logger.error("Erro %s: %s pedido %s", res.status_code, res.text, id)
            if codigo:
                print(f"Erro {res.status_code}: {res.text} pedido {codigo}")
                logger.error("Erro %s: %s pedido %s", res.status_code, res.text, codigo)
            return False

    async def atualizar_nunota(self,id:int=None,nunota:int=None,observacao:str=None):

        if not id:
            logger.error("Pedido não informado.")
            print("Pedido não informado.")
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

        if not observacao:
            res_get = requests.get(
                url=url,
                headers={
                    "Authorization":f"Bearer {token}",
                    "Content-Type":"application/json",
                    "Accept":"application/json"
                }
            )
            if res_get.status_code != 200:
                print(f"Erro {res_get.status_code}: {res_get.text} pedido {id}")
                logger.error("Erro %s: %s pedido %s", res_get.status_code, res_get.text, id)
                return False
            observacao = res_get.json().get('observacao')

        payload = {
            "dataPrevista": None,
            "dataEnvio": None,
            "observacoes": observacao + f' | Nº do pedido no Sankhya: {nunota}',
            "observacoesInternas": None,
            "pagamento": {
                "parcelas": []
            }
        }

        res_put = requests.put(
            url=url,
            headers={
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            },
            json=payload
        )

        if res_put.status_code != 204:
            print(f"Erro {res_put.status_code}: {res_put.text} pedido {id}")
            logger.error("Erro %s: %s pedido %s", res_put.status_code, res_put.text, id)
            return False
        
        return True

    async def gerar_nf(self,id:int=None):

        if not id:
            logger.error("Pedido não informado.")
            print("Pedido não informado.")
            return False

        try:
            token = self.con.get_token()
        except Exception as e:
            logger.error("Erro relacionado ao token de acesso. %s",e)
            return False 
        
        url = self.endpoint+f"/{id}/gerar-nota-fiscal"

        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False 

        res = requests.post(
            url=url,
            headers={
                "Authorization":f"Bearer {token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code != 200:
            print(f"Erro {res.status_code}: {res.text} pedido {id}")
            logger.error("Erro %s: %s pedido %s", res.status_code, res.text, id)            
            return False

        return res.json()

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
            elif res.json().get('tipo') == 'V':
                logger.error("Produto %s ID %s é uma variação. Ajuste o pedido no Olist", res.json().get('descricao'), id)
                print(f"Produto {res.json().get('descricao')} ID {id} é uma variação. Ajuste o pedido no Olist")
                return False, {}
            else:
                logger.error("Produto %s ID %s não é kit nem variação.", res.json().get('descricao'), id)
                print(f"Produto {res.json().get('descricao')} ID {id} não é kit nem variação.")
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
            #dataInicial = '2025-07-20'  # Data fixa para buscar pedidos
            dataInicial = '2025-08-08'  # Data fixa para buscar pedidos
            
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