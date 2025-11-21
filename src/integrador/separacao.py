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
        """
        Verifica quais separações já foram mapeadas
            :param lista_pedidos: lista de dicionários com os dados dos pedidos em separação
            :return list[dict]: lista de dicionários com os dados dos pedidos sem separação vinculada na base
        """
        lista_ids = [p['venda'].get('id') for p in lista_pedidos]
        pedidos_existentes = await crudPedido.buscar(lista=lista_ids)
        lista_pedidos_com_separacao = [p.get('id_pedido') for p in pedidos_existentes if p.get('id_separacao')]        
        pedidos_pendentes_separacao = [p for p in lista_pedidos if p['venda'].get('id') not in lista_pedidos_com_separacao]
        return pedidos_pendentes_separacao

    @interno
    def validar_loja(
            self,
            lista_pedidos: list[dict]
        ) -> list:
        """
        Verifica quais separações pertencem ao E-commerce informado
            :param lista_pedidos: lista de dicionários com os dados dos pedidos
            :return list[dict]: lista de dicionários com os dados dos pedidos do E-commerce
        """        
        return [p for p in lista_pedidos if p['ecommerce'].get('id') == self.id_loja]

    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def receber(self,**kwargs) -> bool:
        """
        Rotina de recebimento das separações dos pedidos
            :return bool: status da operação
        """
        log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                     de='olist',
                                     para='base',
                                     contexto=kwargs.get('_contexto'))        
        # Busca lista de pedidos em separação
        separacao = SeparacaoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
        lista_separacoes = await separacao.listar()
        lista_separacoes = self.validar_loja(lista_pedidos=lista_separacoes)
        if not lista_separacoes:
            await crudLog.atualizar(id=log_id,sucesso=True)
            return True
        lista_separacoes = await self.valida_separacoes_registradas(lista_separacoes)
        if not lista_separacoes:
            await crudLog.atualizar(id=log_id,sucesso=True)
            return True
        
        for i, item in enumerate(lista_separacoes):
            pedido:dict={}
            try:
                time.sleep(self.req_time_sleep)  # Evita rate limit
                # Valida existencia do pedido
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
                ack = await crudPedido.atualizar(id_pedido=item['venda'].get('id'),
                                                 id_separacao=item.get('id'))
                if not ack:
                    msg = f"Erro ao vincular separação {item.get('id')} ao pedido {pedido.get('num_pedido')}."
                    raise Exception(msg)
                # Registra sucesso no log
                await crudLogPedido.criar(log_id=log_id,
                                          pedido_id=pedido.get('id'),
                                          evento='R')
            except Exception as e:
                logger.error(str(e))
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
        """
        Rotina de atualização dos status das separações para Embaladas
            :return bool: status da operação
        """        
        log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                     de='base',
                                     para='olist',
                                     contexto=kwargs.get('_contexto'))        
        # Busca lista de pedidos para checkout
        lista_checkout = await crudPedido.buscar_checkout(ecommerce_id=self.dados_ecommerce.get('id'))
        if not lista_checkout:
            await crudLog.atualizar(id=log_id,sucesso=True)
            return True        
        separacao = SeparacaoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
        for item in lista_checkout:
            time.sleep(self.req_time_sleep)  # Evita rate limit
            # Altera status da separação para Embalada
            ack = await separacao.concluir(id=item.get('id_separacao'))
            if not ack:
                obs = f"Erro ao concluir checkout do pedido ID {item.get('id_pedido')}."
                logger.error(obs)
                await crudLogPedido.criar(log_id=log_id,
                                          pedido_id=item.get('id'),
                                          evento='F',
                                          sucesso=False,
                                          obs=obs)
                continue  
            await crudLogPedido.criar(log_id=log_id,
                                      pedido_id=item.get('id'),
                                      evento='F')
        status_log = False if await crudLogPedido.buscar_falhas(log_id) else True
        await crudLog.atualizar(id=log_id,sucesso=status_log)
        return True

    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def separar(self,**kwargs) -> bool:
        """
        Rotina de atualização dos status das separações para Separadas
            :return bool: status da operação
        """           
        log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                     de='base',
                                     para='olist',
                                     contexto=kwargs.get('_contexto'))        
        # Busca lista de pedidos para separar
        lista_checkout = await crudPedido.buscar_checkout(ecommerce_id=self.dados_ecommerce.get('id'))
        if not lista_checkout:
            await crudLog.atualizar(id=log_id,sucesso=True)
            return True        
        separacao = SeparacaoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))

        for item in lista_checkout:
            time.sleep(self.req_time_sleep)  # Evita rate limit
            # Altera status da separação para Separada
            ack = await separacao.separar(id=item.get('id_separacao'))
            if not ack:
                obs = f"Erro ao separar pedido {item.get('num_pedido')}."
                logger.error(obs)
                await crudLogPedido.criar(log_id=log_id,
                                          pedido_id=item.get('id'),
                                          evento='F',
                                          sucesso=False,
                                          obs=obs)
                continue  
            await crudLogPedido.criar(log_id=log_id,
                                      pedido_id=item.get('id'),
                                      evento='F')
        status_log = False if await crudLogPedido.buscar_falhas(log_id) else True
        await crudLog.atualizar(id=log_id,sucesso=status_log)
        return True

