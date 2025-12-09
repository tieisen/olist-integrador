import os
import time
import requests
from src.olist.produto import Produto
from datetime import datetime, timedelta
from src.utils.decorador import carrega_dados_empresa
from src.utils.autenticador import token_olist
from src.utils.log import set_logger
from src.utils.load_env import load_env
from src.utils.busca_paginada import busca_paginada
load_env()
logger = set_logger(__name__)

class Pedido:

    def __init__(self, codemp:int=None, empresa_id:int=None):  
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.token = None
        self.dados_empresa = None
        self.endpoint = os.getenv('OLIST_API_URL')+os.getenv('OLIST_ENDPOINT_PEDIDOS')
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))        

    @token_olist
    async def buscar(self,id:int=None,codigo:str=None,numero:int=None,cancelados:bool=False) -> dict | list[dict]:
        """
        Busca os dados do pedido.
            :param id: ID do pedido (Olist)
            :param codigo: Código do pedido (E-commerce)
            :param numero: Número do pedido (Olist)
            :param cancelado: se a busca é dos pedidos dos últimos 5 dias com status Cancelado
            :return dict: dicionário com os dados do pedido
            :return list[dict]: lista de dicionários com os dados dos pedidos cancelados
        """        

        if not cancelados and not any([id, codigo, numero]):
            logger.error("Pedido não informado.")
            return False

        if id:
            url = self.endpoint+f"/{id}"
        if codigo:
            url = self.endpoint+f"/?numeroPedidoEcommerce={codigo}"
        if numero:
            url = self.endpoint+f"/?numero={numero}"
        if cancelados:
            url = self.endpoint+f"/?situacao=2&dataInicial={(datetime.today()-timedelta(days=5)).strftime('%Y-%m-%d')}"            

        if not url:
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
                logger.warning("Pedido %s dados incompletos", res.json().get('numeroPedido'))
                return False
            if cancelados:
                return res.json().get('itens')
        else:
            if id:
                logger.error("Erro %s: %s pedido %s", res.status_code, res.text, id)
            if codigo:
                logger.error("Erro %s: %s pedido %s", res.status_code, res.text, codigo)
            return False

    @token_olist
    async def atualizar_nunota(self,id:int,nunota:int,observacao:str=None) -> bool:
        """
        Envia número único do pedido de venda do Sankhya para os pedidos no Olist.
            :param id: ID do pedido (Olist)
            :param nunota: número único do pedido de venda (Sankhya)
            :param observacao: observação atualizada do pedido
            :return bool: status da operação
        """
        
        url = self.endpoint+f"/{id}"
        if not url:
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
            logger.error("Erro %s: %s pedido %s", res_put.status_code, res_put.text, id)
            return False        
        return True

    @token_olist
    async def remover_nunota(self,id:int,nunota:int=None) -> bool:
        """
        Remove número único do pedido de venda do Sankhya dos pedidos no Olist.
            :param id: ID do pedido (Olist)
            :param nunota: número único do pedido de venda (Sankhya)
            :return bool: status da operação
        """        

        import re
        
        url = self.endpoint+f"/{id}"
        if not url:
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
            logger.error("Erro %s: %s pedido %s", res_get.status_code, res_get.text, id)
            return False
        
        observacao:str = res_get.json().get('observacoes')
        nova_observacao:str = ''
        if nunota:
            nova_observacao = str.replace(observacao,f' | Pedido ERP: {nunota}', '')
        else:
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
            logger.error("Erro %s: %s pedido %s", res_put.status_code, res_put.text, id)
            return False
        
        return True

    @token_olist
    async def gerar_nf(self,id:int) -> dict:
        """
        Gera a NF do pedido de venda.
            :param id: ID do pedido (Olist)
            :return dict: dicionário com os dados de confirmação da geração da NF
        """        
        
        url = self.endpoint+f"/{id}/gerar-nota-fiscal"

        if not url:
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

        if res.status_code not in [200,409]:
            logger.error("Erro %s: %s pedido %s", res.status_code, res.text, id)            
            return False

        return res.json()

    @token_olist
    async def validar_kit(self,id:int,item_no_pedido:dict) -> tuple[bool,dict]:
        """
        Valida se o item do pedido é um kit ou um SKU e faz o desmembramento.
            :param id: ID do item
            :param item_no_pedido: dicionário com os dados do item no pedido
            :return bool: status da operação
            :return dict: dicionário com os dados do item ou kit desmembrado
        """          

        if isinstance(item_no_pedido, list):
            item_no_pedido = item_no_pedido[0]
        
        if not isinstance(item_no_pedido, dict):
            logger.error("Item do pedido precisa ser um dicionário.")
            return False, {}
        
        produto = Produto(codemp=self.codemp, empresa_id=self.empresa_id)

        dados_kit = await produto.buscar(id=id)

        if dados_kit.get('tipo') == 'K':
            qtd_kit = item_no_pedido["quantidade"]
            vlt_kit = item_no_pedido["valorUnitario"]
            res_item = []
            for k in dados_kit.get('kit'):

                dados_produto = await produto.buscar(id=k['produto'].get('id'))
                if not dados_produto:
                    logger.error("Produto %s ID %s não encontrado.", dados_kit.get('descricao'), id)
                    return False, {}

                kit_item = {
                    "produto": {
                        "id": k['produto'].get('id'),
                        "sku": k['produto'].get('sku'),
                        "descricao": k['produto'].get('descricao')
                    },
                    "unidade": dados_produto.get('unidade'),
                    "quantidade": k.get('quantidade') * qtd_kit,
                    "valorUnitario": round(vlt_kit / len(dados_kit.get('kit')),4),
                    "infoAdicional": ""                                    
                }

                res_item.append(kit_item)                            
            return True, res_item
        elif dados_kit.get('tipo') == 'V':
            logger.error("Produto %s ID %s é uma variação. Ajuste o pedido no Olist", dados_kit.get('descricao'), id)
            return False, {}
        else:
            logger.error("Produto %s ID %s não é kit nem variação.", dados_kit.get('descricao'), id)
            return False, {}
        
    @token_olist
    @carrega_dados_empresa
    async def buscar_novos(self,atual:bool = True) -> tuple[bool, list[dict]]:
        """
        Busca pedidos novos.
            :param atual: Se True, busca pedidos a partir da última data registrada. Se False, busca pedidos a partir de uma data fixa.
            :return bool: status da operação
            :return list[dict]: lista de dicionários com as dados resumidos dos pedidos
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
                logger.error("Erro ao definir data inicial para busca de pedidos.")
                return False, []            
            url = self.endpoint+f"?dataInicial={dataInicial}"            

        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False, []
        
        try:
            lista_novos_pedidos = await busca_paginada(token=self.token,url=url)
            return True, lista_novos_pedidos
        except Exception as e:
            logger.error(f"{e}")
            return False, lista_novos_pedidos