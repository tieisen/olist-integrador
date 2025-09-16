import os
import time
import logging
from dotenv import load_dotenv

from database.crud                 import pedido     as crudPedido
from database.crud                 import log        as crudLog
from database.crud                 import log_pedido as crudLogPedido
from src.olist.separacao           import Separacao  as SeparacaoOlist
from src.utils.log                 import Log
from src.utils.decorador.contexto  import contexto
from src.utils.decorador.ecommerce import ensure_dados_ecommerce
from src.utils.decorador.log       import log_execucao

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Separacao:

    def __init__(self, id_loja:int):
        self.id_loja = id_loja
        self.dados_ecommerce = None
        self.contexto = 'separacao'        
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))

    @contexto
    @log_execucao
    @ensure_dados_ecommerce
    async def receber(self,**kwargs) -> bool:
        log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                     de='olist',
                                     para='base',
                                     contexto=kwargs.get('_contexto'))        
        # Busca lista de pedidos em separação
        print("-> Buscando lista de pedidos em separação...")
        separacao = SeparacaoOlist(id_loja=self.id_loja)
        lista_separacoes = await separacao.listar()
        if not lista_separacoes:
            print("Nenhum pedido em separação encontrado")
            await crudLog.atualizar(id=log_id,
                                    sucesso=True)
            return True        
        print(f"{len(lista_separacoes)} pedidos em separação encontrados.")
        for item in lista_separacoes:
            time.sleep(self.req_time_sleep)  # Evita rate limit
            # Valida existencia do pedido
            print(f"Validando existencia do pedido {item.get('id_pedido')}...")
            pedido = await crudPedido.buscar(id_pedido=item.get('id_pedido'))
            # Pedido não encontrado
            if not pedido:
                obs = f"Pedido ID {item.get('id_pedido')} não encontrado na base."
                logger.warning(obs)
                print(obs)
                await crudLogPedido.criar(log_id=log_id,
                                          pedido_id=item.get('id_pedido'),
                                          evento='R',
                                          sucesso=False,
                                          obs=obs)
                continue            
            # Separação já vinculada ao pedido
            if pedido.get('id_separacao') == item.get('id_separacao'):
                obs = "Separação já vinculada ao pedido."
                print(obs)
                continue
            # Vincula separação
            print("Vinculando separação ao pedido...")
            ack = await crudPedido.atualizar(id_pedido=item.get('id_pedido'),
                                             id_separacao=item.get('id_separacao'))
            if not ack:
                obs = f"Erro ao vincular separação {item.get('id_separacao')} ao pedido ID {item.get('id_pedido')}."
                logger.error(obs)
                print(obs)
                await crudLogPedido.criar(log_id=log_id,
                                          pedido_id=item.get('id_pedido'),
                                          evento='R',
                                          sucesso=False,
                                          obs=obs)
                continue
            await crudLogPedido.criar(log_id=log_id,
                                      pedido_id=item.get('id_pedido'),
                                      evento='R')
            print("Pedido atualizado com sucesso!")
        status_log = False if await crudLogPedido.buscar_falhas(log_id) else True
        await crudLog.atualizar(id=log_id,sucesso=status_log)
        print("--> RECEBIMENTO DE SEPARAÇÕES CONCLUÍDA!")
        return True

    @contexto
    @log_execucao    
    @ensure_dados_ecommerce
    async def checkout(self,**kwargs) -> bool:
        log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                     de='base',
                                     para='olist',
                                     contexto=kwargs.get('_contexto'))        
        # Busca lista de pedidos para checkout
        print("-> Buscando lista de pedidos para checkout...")
        lista_checkout = await crudPedido.buscar_checkout(ecommerce_id=self.dados_ecommerce.get('id'))
        if not lista_checkout:
            print("Nenhum pedido para checkout encontrado")
            await crudLog.atualizar(id=log_id,
                                    sucesso=True)
            return True        
        print(f"{len(lista_checkout)} pedidos para checkout encontrados.")
        separacao = SeparacaoOlist(id_loja=self.id_loja)
        for item in lista_checkout:
            time.sleep(self.req_time_sleep)  # Evita rate limit
            # Altera status da separação para Embalada
            print(f"Alterando status da separação do pedido {item.get('num_pedido')} para Embalada...")
            ack = await separacao.concluir(id=item.get('id_separacao'))
            if not ack:
                obs = f"Erro ao concluir checkout do pedido ID {item.get('id_pedido')}."
                logger.error(obs)
                print(obs)
                await crudLogPedido.criar(log_id=log_id,
                                          pedido_id=item.get('id_pedido'),
                                          evento='F',
                                          sucesso=False,
                                          obs=obs)
                continue  
            await crudLogPedido.criar(log_id=log_id,
                                      pedido_id=item.get('id_pedido'),
                                      evento='F')
            print("Checkout realizado com sucesso!")        
        status_log = False if await crudLogPedido.buscar_falhas(log_id) else True
        await crudLog.atualizar(id=log_id,sucesso=status_log)
        print("--> CHECKOUT DOS PEDIDOS CONCLUÍDO!")
        return True

    @contexto
    @log_execucao
    @ensure_dados_ecommerce
    async def separar(self,**kwargs) -> bool:
        log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                     de='base',
                                     para='olist',
                                     contexto=kwargs.get('_contexto'))        
        # Busca lista de pedidos para separar
        print("-> Buscando lista de pedidos para separar...")
        lista_checkout = await crudPedido.buscar_checkout(ecommerce_id=self.dados_ecommerce.get('id'))
        if not lista_checkout:
            print("Nenhum pedido para separar encontrado")
            await crudLog.atualizar(id=log_id,
                                    sucesso=True)
            return True        
        print(f"{len(lista_checkout)} pedidos para separar encontrados.")
        separacao = SeparacaoOlist(id_loja=self.id_loja)
        for item in lista_checkout:
            time.sleep(self.req_time_sleep)  # Evita rate limit
            # Altera status da separação para Separada
            print(f"Alterando status da separação do pedido {item.get('num_pedido')} para Separada...")
            ack = await separacao.separar(id=item.get('id_separacao'))
            if not ack:
                obs = f"Erro ao separar pedido {item.get('num_pedido')}."
                logger.error(obs)
                print(obs)
                await crudLogPedido.criar(log_id=log_id,
                                          pedido_id=item.get('id_pedido'),
                                          evento='F',
                                          sucesso=False,
                                          obs=obs)
                continue  
            await crudLogPedido.criar(log_id=log_id,
                                      pedido_id=item.get('id_pedido'),
                                      evento='F')
            print("Separação realizada com sucesso!")        
        status_log = False if await crudLogPedido.buscar_falhas(log_id) else True
        await crudLog.atualizar(id=log_id,sucesso=status_log)
        print("--> SEPARAÇÃO DOS PEDIDOS CONCLUÍDA!")
        return True

