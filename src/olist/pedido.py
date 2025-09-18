import os
import time
import logging
import requests
from dotenv import load_dotenv
from datetime import datetime, timedelta
from src.utils.decorador.olist import ensure_token
from src.utils.decorador.empresa import ensure_dados_empresa
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Pedido:

    def __init__(self, codemp:int=None, empresa_id:int=None):  
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.token = None
        self.dados_empresa = None
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_PEDIDOS')
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))        

    @ensure_token
    async def buscar(
            self,
            id:int=None,
            codigo:str=None,
            numero:int=None,
            cancelados:bool=False
        ):

        if not cancelados and not any([id, codigo, numero]):
            logger.error("Pedido não informado.")
            print("Pedido não informado.")
            return False

        if id:
            url = self.endpoint+f"/{id}"
        if codigo:
            url = self.endpoint+f"/?numeroPedidoEcommerce={codigo}"
        if numero:
            url = self.endpoint+f"/?numero={numero}"
        if cancelados:
            url = self.endpoint+f"/?situacao=2&dataInicial={(datetime.today()-timedelta(days=4)).strftime('%Y-%m-%d')}"            

        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False 

        res = requests.get(
            url=url,
            headers={
                "Authorization":f"Bearer {self.token}",
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
                            "Authorization":f"Bearer {self.token}",
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

    @ensure_token
    async def atualizar_nunota(
            self,
            id:int,
            nunota:int,
            observacao:str=None
        ):
        
        url = self.endpoint+f"/{id}"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False

        payload = {
            "dataPrevista": None,
            "dataEnvio": None,
            "observacoes": observacao + f" | Pedido ERP: {nunota}",
            "observacoesInternas": None,
            "pagamento": {
                "parcelas": []
            }
        }

        res_put = requests.put(
            url=url,
            headers={
                "Authorization":f"Bearer {self.token}",
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

    @ensure_token
    async def remover_nunota(
            self,
            id:int
        ):

        import re
        
        url = self.endpoint+f"/{id}"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False 

        res_get = requests.get(
            url=url,
            headers={
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )
        if res_get.status_code != 200:
            print(f"Erro {res_get.status_code}: {res_get.text} pedido {id}")
            logger.error("Erro %s: %s pedido %s", res_get.status_code, res_get.text, id)
            return False
        observacao = res_get.json().get('observacoes')
        regex = r"[|].+"
        nova_observacao = re.sub(regex, '', observacao)

        payload = {
            "dataPrevista": None,
            "dataEnvio": None,
            "observacoes": nova_observacao,
            "observacoesInternas": None,
            "pagamento": {
                "parcelas": []
            }
        }

        res_put = requests.put(
            url=url,
            headers={
                "Authorization":f"Bearer {self.token}",
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

    @ensure_token
    async def gerar_nf(
            self,
            id:int
        ):
        
        url = self.endpoint+f"/{id}/gerar-nota-fiscal"

        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False 

        res = requests.post(
            url=url,
            headers={
                "Authorization":f"Bearer {self.token}",
                "Content-Type":"application/json",
                "Accept":"application/json"
            }
        )

        if res.status_code != 200:
            print(f"Erro {res.status_code}: {res.text} pedido {id}")
            logger.error("Erro %s: %s pedido %s", res.status_code, res.text, id)            
            return False

        return res.json()

    @ensure_token
    async def validar_kit(
            self,
            id:int,
            item_no_pedido
        ) -> tuple[bool,dict]:
        
        if isinstance(item_no_pedido, list):
            item_no_pedido = item_no_pedido[0]
        
        if not isinstance(item_no_pedido, dict):
            logger.error("Item do pedido precisa ser um dicionário.")
            print("Item do pedido precisa ser um dicionário.")
            return False, {}
        
        url = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_PRODUTOS')+f"/{id}"
        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False, {}
             
        res = requests.get(
            url=url,
            headers={
                "Authorization":f"Bearer {self.token}",
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
        
    @ensure_token
    @ensure_dados_empresa
    async def buscar_novos(
            self,
            atual:bool = True
        ) -> tuple[bool, list]:
        """ Busca pedidos do Olist a partir de uma data inicial.
        Args:
            atual (bool): Se True, busca pedidos a partir da última data registrada. Se False, busca pedidos a partir de uma data fixa.
        Returns:
            tuple: (bool, list) - Retorna True e a lista de IDs de pedidos encontrados ou False e uma lista vazia em caso de erro.
        """

        dataInicial = None
        dias_busca = self.dados_empresa.get('olist_dias_busca_pedidos')
        situacao_buscar = self.dados_empresa.get('olist_situacao_busca_pedidos')

        # Busca a data da última importação de pedidos
        # Se não houver, define a data inicial como 3 dias atrás
        if situacao_buscar:
            url = self.endpoint+f"?situacao={situacao_buscar}"
        else:
            if atual:
                dataInicial = (datetime.today()-timedelta(days=dias_busca)).strftime('%Y-%m-%d')
            else:                
                dataInicial = '2025-08-08'  # Data fixa para buscar pedidos            
            if not dataInicial:
                print("Erro ao definir data inicial para busca de pedidos.")
                logger.error("Erro ao definir data inicial para busca de pedidos.")
                return False, []            
            url = self.endpoint+f"?dataInicial={dataInicial}"            

        if not url:
            print(f"Erro relacionado à url. {url}")
            logger.error("Erro relacionado à url. %s",url)
            return False, []

        status = 200
        paginacao = {}
        itens = []  

        while status == 200:
            # Verifica se há paginação
            if paginacao:        
                if paginacao["limit"] + paginacao["offset"] < paginacao ["total"]:
                    offset = paginacao["limit"] + paginacao["offset"]
                    url+=f"&offset={offset}"
                else:
                    url = None

            if url:
                res = requests.get(url=url,
                                   headers={
                                       "Authorization":f"Bearer {self.token}",
                                       "Content-Type":"application/json",
                                       "Accept":"application/json"
                                   })
                status=res.status_code
                itens += res.json().get("itens",[])
                paginacao = res.json().get("paginacao",{})
                time.sleep(self.req_time_sleep)
            else:
                status = 0

        if not itens:
            print("Nenhum pedido encontrado.")
            logger.info("Nenhum pedido encontrado.")
            return True, []

        # Ordena os itens por ID        
        self.ordena_por_id(itens)
        #return True, [p["id"] for p in itens]
        return True, itens
    
    def ordena_por_id(self,lista_itens):
        lista_itens.sort(key=lambda i: i['id'])
        return True