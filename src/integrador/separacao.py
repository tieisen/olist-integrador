import os
import time
from database.crud import pedido as crudPedido
from database.crud import log as crudLog
from database.crud import log_pedido as crudLogPedido
from src.olist.separacao import Separacao  as SeparacaoOlist
from src.utils.decorador import contexto, carrega_dados_ecommerce, log_execucao, interno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Separacao:

    def __init__(self, id_loja:int):
        self.id_loja = id_loja
        self.dados_ecommerce = None
        self.contexto = 'separacao'        
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))

    @interno
    async def valida_separacoes_registradas(
            self,
            lista_pedidos:list[dict]
        ) -> list[dict]:
        lista_ids = [p['venda'].get('id') for p in lista_pedidos]
        pedidos_existentes = await crudPedido.buscar(lista=lista_ids)
        lista_pedidos_com_separacao = [p.get('id_pedido') for p in pedidos_existentes if p.get('id_separacao')]        
        pedidos_pendentes_separacao = [p for p in lista_pedidos if p['venda'].get('id') not in lista_pedidos_com_separacao]
        return pedidos_pendentes_separacao

    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def receber(self,**kwargs) -> bool:
        log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                     de='olist',
                                     para='base',
                                     contexto=kwargs.get('_contexto'))        
        # Busca lista de pedidos em separação
        print("-> Buscando lista de pedidos em separação...")
        separacao = SeparacaoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
        lista_separacoes = await separacao.listar()
        if not lista_separacoes:
            print("Nenhum pedido em separação encontrado")
            await crudLog.atualizar(id=log_id,
                                    sucesso=True)
            return True
        lista_separacoes = await self.valida_separacoes_registradas(lista_separacoes)
        if not lista_separacoes:
            print("Nenhuma separação pendente encontrada")
            await crudLog.atualizar(id=log_id,
                                    sucesso=True)
            return True
        print(f"{len(lista_separacoes)} pedidos em separação encontrados.")
        for i, item in enumerate(lista_separacoes):
            pedido:dict={}
            try:
                time.sleep(self.req_time_sleep)  # Evita rate limit
                print(f"-> Pedido {i + 1}/{len(lista_separacoes)}: {item['venda'].get('numero')}")            
                # Valida existencia do pedido
                print(f"Validando existencia do pedido...")
                res_pedido = await crudPedido.buscar(id_pedido=item['venda'].get('id'))
                # Pedido não encontrado
                if not res_pedido:
                    msg = f"Pedido {item['venda'].get('numero')} não encontrado na base."
                    raise Exception(msg)
                pedido = res_pedido[0]
                # Separação já vinculada ao pedido
                if pedido.get('id_separacao') == item.get('id'):
                    continue
                # Vincula separação
                print("Vinculando separação ao pedido...")
                ack = await crudPedido.atualizar(id_pedido=item['venda'].get('id'),
                                                 id_separacao=item.get('id'))
                if not ack:
                    msg = f"Erro ao vincular separação {item.get('id')} ao pedido {pedido.get('num_pedido')}."
                    raise Exception(msg)
                # Registra sucesso no log
                await crudLogPedido.criar(log_id=log_id,
                                          pedido_id=pedido.get('id'),
                                          evento='R')
                print("Pedido atualizado com sucesso!")
            except Exception as e:
                logger.error(str(e))
                print(str(e))
                await crudLogPedido.criar(log_id=log_id,
                                          pedido_id=pedido.get('id'),
                                          evento='R',
                                          sucesso=False,
                                          obs=str(e))
                continue             
        status_log = False if await crudLogPedido.buscar_falhas(log_id) else True
        await crudLog.atualizar(id=log_id,sucesso=status_log)
        return status_log

    @contexto
    @log_execucao    
    @carrega_dados_ecommerce
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
        separacao = SeparacaoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
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
                                          pedido_id=item.get('id'),
                                          evento='F',
                                          sucesso=False,
                                          obs=obs)
                continue  
            await crudLogPedido.criar(log_id=log_id,
                                      pedido_id=item.get('id'),
                                      evento='F')
            print("Checkout realizado com sucesso!")        
        status_log = False if await crudLogPedido.buscar_falhas(log_id) else True
        await crudLog.atualizar(id=log_id,sucesso=status_log)
        return True

    @contexto
    @log_execucao
    @carrega_dados_ecommerce
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
        separacao = SeparacaoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
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
                                          pedido_id=item.get('id'),
                                          evento='F',
                                          sucesso=False,
                                          obs=obs)
                continue  
            await crudLogPedido.criar(log_id=log_id,
                                      pedido_id=item.get('id'),
                                      evento='F')
            print("Separação realizada com sucesso!")        
        status_log = False if await crudLogPedido.buscar_falhas(log_id) else True
        await crudLog.atualizar(id=log_id,sucesso=status_log)
        return True

