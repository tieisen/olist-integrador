import os
import time
import logging
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv
from src.olist.connect import Connect
from database.crud import venda as crud
from src.olist.pedido import Pedido as OlistPedido
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Venda:

    def __init__(self):
        """ Inicializa a classe Venda com a conexão ao Olist e o endpoint de pedidos. """
        self.con = Connect()
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP',1.5))
        self.endpoint = os.getenv('OLIST_API_URL') + os.getenv('OLIST_ENDPOINT_PEDIDOS')
        self.atual = None

    async def buscar_pedidos(self, atual:bool = True) -> tuple[bool, list]:
        """ Busca pedidos do Olist a partir de uma data inicial.
        Args:
            atual (bool): Se True, busca pedidos a partir da última data registrada. Se False, busca pedidos a partir de uma data fixa.
        Returns:
            tuple: (bool, list) - Retorna True e a lista de IDs de pedidos encontrados ou False e uma lista vazia em caso de erro.
        """

        dataInicial = None

        self.atual = atual if not self.atual else self.atual

        # Busca a data da última importação de pedidos
        # Se não houver, define a data inicial como dois dias atrás
        if self.atual:
            dataInicial = crud.read_last_venda_dt().strftime('%Y-%m-%d') if crud.read_last_venda_dt() else (datetime.today()-timedelta(days=2)).strftime('%Y-%m-%d')
        else:
            dataInicial = '2025-07-09'  # Data fixa para buscar pedidos
            
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
    
    def valida_pedidos_existentes(self, lista_pedidos: list) -> list:
        """ Valida se os pedidos já existem na base de dados.
        Args:
            lista_pedidos (list): Lista de IDs de pedidos a serem validados.
        Returns:
            list: Lista de IDs de pedidos que não existem na base de dados.
        """
        if not lista_pedidos:
            print("Nenhum pedido encontrado.")
            logger.info("Nenhum pedido encontrado.")
            return []
        
        existentes = crud.read_by_list_idpedido(lista_pedidos)
        existentes = [p.id_pedido for p in existentes if p.id_pedido in lista_pedidos]
        return [pedido for pedido in lista_pedidos if pedido not in existentes]
    
    async def recebe_pedidos(self, lista_pedidos:list=None, atual:bool=True) -> bool:
        """ Recebe pedidos do Olist e os adiciona à base de dados.
        Args:
            lista_pedidos (list, optional): Lista de IDs de pedidos a serem recebidos. Se None, busca todos os pedidos novos.
            atual (bool, optional): Se True, busca pedidos a partir da última data registrada. Se False, busca pedidos a partir de uma data fixa.
        Returns:
            bool: True se os pedidos foram recebidos com sucesso, False caso contrário.
        """

        self.atual = atual

        if not lista_pedidos:
            ack, lista = await self.buscar_pedidos()
            if not ack:
                print("Nenhum pedido encontrado.")
                logger.info("Nenhum pedido encontrado.")
                return False
            print(f"Pedidos encontrados: {len(lista)}")
            lista_pedidos = self.valida_pedidos_existentes(lista)
            
            if not lista_pedidos:
                print("Todos os pedidos já existem na base de dados.")
                logger.info("Todos os pedidos já existem na base de dados.")
                return True
        
        print(f"Pedidos a serem recebidos: {len(lista_pedidos)}")
        pedido_olist = OlistPedido()
        first = True
        for pedido in lista_pedidos:
            dados_pedido = await pedido_olist.buscar(id=pedido)
            crud.create(id_loja=dados_pedido['ecommerce'].get('id'),
                        id_pedido=dados_pedido.get('id'),
                        cod_pedido=dados_pedido['ecommerce'].get('numeroPedidoEcommerce'),
                        num_pedido=dados_pedido.get('numeroPedido'))
            print(f"Pedido {dados_pedido.get('numeroPedido')} adicionado à base de dados.")
            if not first:
                time.sleep(self.req_time_sleep)  # Evita rate limit
            first = False
        return True