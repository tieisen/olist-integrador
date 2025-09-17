import os
import re
import time
import logging
from dotenv                            import load_dotenv
from datetime                          import datetime

from src.olist.pedido                  import Pedido      as PedidoOlist
from src.sankhya.pedido                import Pedido      as PedidoSnk
from src.parser.pedido                 import Pedido      as ParserPedido
from src.sankhya.conferencia           import Conferencia as ConferenciaSnk
from src.parser.conferencia            import Conferencia as ParserConferencia
from database.crud                     import pedido      as crudPedido
from database.crud                     import log_pedido  as crudLogPed
from database.crud                     import log         as crudLog
from src.services.viacep               import Viacep
from src.utils.log                     import Log
from src.utils.decorador.contexto      import contexto
from src.utils.decorador.ecommerce     import ensure_dados_ecommerce
from src.utils.decorador.log           import log_execucao
from src.utils.decorador.internal_only import internal_only

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Pedido:

    def __init__(self, id_loja:int):
        self.id_loja:int=id_loja
        self.log_id:int=None
        self.contexto:str='pedido'
        self.dados_ecommerce:dict=None
        self.req_time_sleep:float=float(os.getenv('REQ_TIME_SLEEP', 1.5))

    @internal_only
    async def validar_existentes(
            self,
            lista_pedidos: list
        ) -> list:
        lista_ids = [p.get('id') for p in lista_pedidos]
        pedidos_existentes = await crudPedido.buscar(lista=lista_ids)
        lista_pedidos_existentes = [p.get('id_pedido') for p in pedidos_existentes]
        pedidos_pendentes = [p for p in lista_pedidos if p.get('id') not in lista_pedidos_existentes]
        return pedidos_pendentes

    @internal_only
    def validar_situacao(
            self,
            dados_pedido:dict
        ) -> bool:
        if dados_pedido.get('situacao') in [2,8]:
            # Cancelado / Dados incompletos
            return False

    # async def validar_cancelamentos(self):
    #     print("Validando pedidos cancelados...")
    #     pedido = PedidoOlist()
    #     nota = NotaOlist()
    #     pedidos_cancelados = await pedido.buscar(cancelados=True)
    #     notas_canceladas = await nota.buscar_canceladas()
    #     if not any([pedidos_cancelados, notas_canceladas]):
    #         # Nenhum pedido ou nota cancelada encontrado
    #         print("Nenhum pedido ou nota cancelada encontrado.")
    #         return True
    #     log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO+'_cancelamentos')
    #     pedidos_pendente_cancelar_integrador = venda.validar_cancelamentos(lista_ids=pedidos_cancelados)
    #     if pedidos_pendente_cancelar_integrador:
    #         print(f"Pedidos pendentes de cancelamento no integrador: {len(pedidos_pendente_cancelar_integrador)}")
    #         for pedido in pedidos_pendente_cancelar_integrador:
    #             venda.atualizar_cancelada(id_pedido=pedido.id_pedido)                
    #         print("Pedidos atualizados no integrador com sucesso!")
    #     if notas_canceladas:
    #         print(f"Notas canceladas no Olist: {len(notas_canceladas)}")
    #         for nota_cancelada in notas_canceladas:
    #             print(nota_cancelada)
    #             venda.atualizar_cancelada(id_nota=nota_cancelada)                
    #         print("Notas atualizadas no integrador com sucesso!")
    #     print("Validação de pedidos cancelados concluída!")
    #     log.atualizar(id=log_id, sucesso=True)
    #     return True

    @contexto
    @internal_only
    @ensure_dados_ecommerce
    async def receber(
            self,
            num_pedido:int=None,
            id_pedido:int=None,
            dados_pedido:dict=None,
            **kwargs
        ) -> bool:

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='olist',
                                              para='base',
                                              contexto=kwargs.get('_contexto'))            
        try:
            if not dados_pedido:
                pedido_olist = PedidoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
                # Busca dados do pedido no Olist
                print(f"Buscando dados do pedido {num_pedido or id_pedido}...")
                if num_pedido:
                    dados_pedido = await pedido_olist.buscar(numero=num_pedido)
                if id_pedido:
                    dados_pedido = await pedido_olist.buscar(id=id_pedido)            
                if not dados_pedido:
                    msg = f"Erro ao buscar dados do pedido {num_pedido or id_pedido} no Olist"
                    raise Exception(msg)
                # Valida situação
                print("Validando situação do pedido...")         
                if not self.validar_situacao(dados_pedido):
                    msg = f"Pedido {num_pedido or id_pedido} cancelado ou com dados incompletos"
                    raise Exception(msg)            
            # Adiciona pedido na base
            print("Adicionando pedido na base...")
            id = await crudPedido.criar(id_loja=dados_pedido['ecommerce'].get('id'),
                                         id_pedido=dados_pedido.get('id'),
                                         cod_pedido=dados_pedido['ecommerce'].get('numeroPedidoEcommerce'),
                                         num_pedido=dados_pedido.get('numeroPedido'))
            if not id:
                msg = f"Erro ao adicionar pedido {dados_pedido.get('numeroPedido')} à base de dados."
                raise Exception(msg)
            print(f"Pedido {dados_pedido.get('numeroPedido')} adicionado à base de dados.")
            return {"success": True, "id": id}
        except Exception as e:
            return {"success": False, "id": None, "__exception__": str(e)}

    @internal_only
    @ensure_dados_ecommerce
    async def consultar_pedidos_novos(
            self,
            atual:bool=True
        ) -> list:
        pedido_olist = PedidoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
        # Busca pedidos novos
        print("Buscando pedidos novos...")
        ack, lista = await pedido_olist.buscar_novos(atual=atual)            
        if not ack:
            # Erro na busca
            print("Erro ao buscar pedidos novos")
            return False
        if not lista:
            # Nenhum pedido novo encontrado
            print("Nenhum pedido novo encontrado")
            return True
        # Valida pedidos já recebidos
        print("Validando pedidos já recebidos...")
        lista_pedidos = await self.validar_existentes(lista)
        if not lista_pedidos:
            # Todos os pedidos já foram recebidos
            print("Todos os pedidos já foram recebidos")
            return True
        return lista_pedidos

    @contexto
    @log_execucao
    @ensure_dados_ecommerce
    async def receber_novos(
            self,
            atual:bool=True,
            num_pedido:int=None,
            **kwargs
        ) -> bool:
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='base',
                                          contexto=kwargs.get('_contexto'))
        if num_pedido:
            # Recebe um pedido específico
            ack = await self.receber(num_pedido=num_pedido)
            # Registra sucesso no log
            await crudLogPed.criar(log_id=self.log_id,
                                   pedido_id=ack.get('id',0),
                                   evento='R',
                                   status=ack.get('success'),
                                   obs=ack.get('__exception__',None))            
        else:
            # Consulta pedidos novos
            print("-> Consultando pedidos novos...")
            pedidos_novos = await self.consultar_pedidos_novos(atual=atual)
            if isinstance(pedidos_novos, list):            
                print(f"{len(pedidos_novos)} pedidos para receber")
                for i, pedido in enumerate(pedidos_novos):
                    time.sleep(self.req_time_sleep)
                    print(f"-> Pedido {i + 1}/{len(pedidos_novos)}: {pedido.get("numeroPedido")}")            
                    ack = await self.receber(dados_pedido=pedido)
                    # Registra sucesso no log
                    await crudLogPed.criar(log_id=self.log_id,
                                           pedido_id=ack.get('id',0),
                                           evento='R',
                                           status=ack.get('success'),
                                           obs=ack.get('__exception__',None))                    
            elif isinstance(pedidos_novos,bool):
                # Retornou True ou False
                pass
        if isinstance(pedidos_novos,bool):
            status_log = pedidos_novos
        else:
            status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        print("--> RECEBIMENTO DOS PEDIDOS NOVOS CONCLUÍDO!")
        return True

    async def validar_item_desmembrar_kit(self, itens:list[dict], olist:PedidoOlist):
        itens_validados:list=[]        
        for item in itens:
            # Kits não tem vínculo por SKU, ou estão marcados com #K no final do código
            if item['produto'].get('sku') and '#K' not in item['produto'].get('sku'):
                itens_validados.append(item)
            else:
                try:
                    ack, kit_desmembrado = await olist.validar_kit(id=item['produto'].get('id'),item_no_pedido=item)
                    if ack:
                        itens_validados+=kit_desmembrado
                except Exception as e:
                    logger.error(f"Erro: {e}")
                    return False
        return itens_validados

    @contexto
    @internal_only
    @ensure_dados_ecommerce
    async def importar_unico(
            self,
            dados_pedido:dict,
            **kwargs
        ) -> bool:

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='base',
                                              para='sankhya',
                                              contexto=kwargs.get('_contexto'))

        pedido_snk = PedidoSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        # Verifica se o pedido já foi importado
        print("Verificando se o pedido já foi importado...")
        dados_snk = await pedido_snk.buscar(id_olist=dados_pedido.get('id_pedido'))
        try:
            if not dados_snk:
                pedido_olist = PedidoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
                viacep = Viacep()
                parser = ParserPedido(id_loja=self.id_loja)

                # Busca dados do pedido no Olist
                print("Buscando dados do pedido no Olist...")
                dados_pedido_olist = await pedido_olist.buscar(id=dados_pedido.get('id_pedido'))
                if not dados_pedido_olist:
                    msg = "Erro ao buscar dados do pedido no Olist"
                    raise Exception(msg)

                # Busca os dados da cidade do cliente
                print("Buscando os dados da cidade do cliente...")
                ibge = await viacep.busca_ibge_pelo_cep(dados_pedido_olist["cliente"]["endereco"].get("cep"))
                if not ibge:
                    msg = "Erro ao buscar dados da cidade do cliente no Viacep"
                    raise Exception(msg)
                dados_cidade = await pedido_snk.buscar_cidade(ibge=ibge)
                if not dados_cidade:
                    msg = "Erro ao buscar dados da cidade do cliente no Sankhya"
                    raise Exception(msg)
                
                # Valida itens e desmembra kits
                print("Validando itens e desmembrando kits...")
                itens_validados = await self.validar_item_desmembrar_kit(itens=dados_pedido_olist.get('itens'),
                                                                         olist=pedido_olist)
                if not itens_validados:
                    msg = "Erro ao validar itens/desmembrar kits"
                    raise Exception(msg)
                dados_pedido_olist['itens'] = itens_validados            
                
                # Converte para o formato da API do Sankhya
                print("Convertendo para o formato da API do Sankhya...")
                data_cabecalho, data_itens = await parser.to_sankhya(dados_olist=dados_pedido_olist,
                                                                    dados_cidade=dados_cidade)
                if not any([data_cabecalho,data_itens]):
                    msg = "Erro ao converter dados do pedido para o formato da API do Sankhya"
                    raise Exception(msg)

                # Insere os dados do pedido
                print("Inserindo o pedido no Sankhya...")
                pedido_incluido = await pedido_snk.lancar(dados_cabecalho=data_cabecalho,
                                                        dados_itens=data_itens)
                if not pedido_incluido:
                    msg = f"Erro ao inserir pedido no Sankhya."
                    raise Exception(msg)
                
                ack = await crudPedido.atualizar(id_pedido=dados_pedido_olist.get('id'),
                                                 nunota=pedido_incluido,
                                                 dh_importacao=datetime.now())
                if not ack:
                    msg = f"Erro ao atualizar situação do pedido {dados_pedido_olist.get('numeroPedido')} para importado"
                    raise Exception(msg)
                
                print(f"Pedido #{dados_pedido_olist.get('numeroPedido')} importado no código {pedido_incluido}")

                # Envia nunota para o pedido nos Olist
                print("Enviando nunota para o pedido no Olist...")                
                ack = await self.atualizar_nunota(id_pedido=dados_pedido_olist.get('id'),
                                                  nunota=pedido_incluido,
                                                  olist=pedido_olist)
                if not ack:
                    msg = f"Erro ao enviar nunota para o pedido {dados_pedido_olist.get('numeroPedido')} no Olist"
                    raise Exception(msg)
            else:
                ack = await crudPedido.atualizar(id_pedido=dados_pedido.get('id_pedido'),
                                                 nunota=dados_snk.get('nunota'),
                                                 dh_importacao=dados_snk.get('dtneg'))
                if not ack:
                    msg = f"Erro ao atualizar situação do pedido {dados_pedido.get('num_pedido')} para importado"
                    raise Exception(msg)                
            return {"success": True}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}
    
    def unificar(
            self,
            lista_pedidos:list[dict]
        ) -> tuple[list,list]:
        
        pedidos:list[dict]=None
        itens:list[dict]=None
        
        for pedido in lista_pedidos:
            status_itens:bool=True
            itens_pedido = pedido.get('itens')
            for item_pedido in itens_pedido:
                # Valida o formato do código do produto
                try:
                    codprod = re.search(r'^\d{8}', item_pedido['produto'].get('sku'))
                    codprod = codprod.group()
                except Exception as e:
                    status_itens = False
                    logger.error("Código do produto inválido: %s", item_pedido['produto'].get('sku'))
                    print(f"Código do produto inválido: {item_pedido['produto'].get('sku')}")                    
                    continue

                dados_item = {
                    'codprod': item_pedido['produto'].get('sku'),
                    'qtdneg': item_pedido.get('quantidade'),
                    'vlrunit': item_pedido.get('valorUnitario')
                }

                # Verifica se o item é novo ou soma se já estiver na lista
                aux = None                
                for item in itens:
                    if (dados_item.get('codprod') == item.get('codprod')) and (dados_item.get('vlrunit') == item.get('vlrunit')):
                        aux = item
                        break                
                if not aux:
                    itens.append(dados_item)
                    continue
                aux['qtdneg']+=dados_item.get('qtdneg')
            
            if status_itens:
                pedidos.append({
                    "numero":pedido.get('numeroPedido'),
                    "codigo":pedido['ecommerce'].get('numeroPedidoEcommerce')
                })
            else:
                msg = f"Não foi possível unificar o pedido {pedido.get('numeroPedido')}. Itens inválidos."
                logger.error(msg)
                print(msg)
                continue

        return pedidos, itens

    @contexto
    @internal_only
    @ensure_dados_ecommerce
    async def importar_agrupado(
            self,
            lista_pedidos:list[dict],
            **kwargs
        ) -> dict:
        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='base',
                                              para='sankhya',
                                              contexto=kwargs.get('_contexto'))
        
        pedido_olist = PedidoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
        pedido_snk = PedidoSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        dados_pedidos_olist:list[dict]=None

        try:
            for i, pedido in enumerate(lista_pedidos):
                time.sleep(self.req_time_sleep)
                print(f"-> Pedido {i + 1}/{len(lista_pedidos)}: {pedido.get('num_pedido')}")
                
                # Busca dados do pedido no Olist
                print("Buscando dados do pedido no Olist...")
                dados_pedido_olist = await pedido_olist.buscar(id=pedido.get('id_pedido'))
                if not dados_pedido_olist:
                    msg = f"Erro ao buscar dados do pedido {pedido.get('num_pedido')} no Olist"
                    raise Exception(msg)
                
                # Valida itens e desmembra kits
                print("Validando itens e desmembrando kits...")
                itens_validados = await self.validar_item_desmembrar_kit(itens=dados_pedido_olist.get('itens'),
                                                                        olist=pedido_olist)
                if not itens_validados:
                    msg = "Erro ao validar itens/desmembrar kits"
                    raise Exception(msg)
                dados_pedido_olist['itens'] = itens_validados
                dados_pedidos_olist.append(dados_pedido_olist)

            # Unifica os itens dos pedidos
            print("-> Unificando os pedidos...")
            pedidos_agrupados, itens_agrupados = await self.unificar(lista_pedidos=dados_pedidos_olist)
            if not all([pedidos_agrupados, itens_agrupados]):
                msg = "Erro ao unificar pedidos"
                raise Exception(msg)

            # Converte para o formato da API do Sankhya
            parser = ParserPedido(id_loja=self.id_loja)
            print("-> Convertendo para o formato da API do Sankhya...")        
            dados_cabecalho, dados_itens = parser.to_sankhya_lote(lista_pedidos=pedidos_agrupados,
                                                                lista_itens=itens_agrupados)
            if not all([dados_cabecalho, dados_itens]):
                msg = "Erro ao converter dados dos pedidos para o formato da API do Sankhya"
                raise Exception(msg)            

            # Insere os dados do pedido
            print("-> Inserindo o pedido no Sankhya...")
            pedido_incluido = await pedido_snk.lancar(dados_cabecalho=dados_cabecalho,
                                                    dados_itens=dados_itens)
            if not pedido_incluido:
                msg = f"Erro ao inserir pedido no Sankhya."
                raise Exception(msg)
            
            print(f"-> Pedidos importados no código {pedido_incluido}!")
            
            # Atualiza log
            print("-> Atualizando log...")
            for pedido in lista_pedidos:
                ack = await crudPedido.atualizar(id_pedido=pedido.get('id_pedido'),
                                                 nunota=pedido_incluido,
                                                 dh_importacao=datetime.now())
                if not ack:
                    msg = f"Erro ao atualizar situação do pedido {pedido.get('num_pedido')} para importado"
                    raise Exception(msg)

            # Envia nunota para os pedidos nos Olist
            print("-> Enviando nunota para os pedidos no Olist...")
            for pedido in lista_pedidos:                
                ack = await self.atualizar_nunota(id_pedido=pedido.get('id_pedido'),
                                                  nunota=pedido_incluido,
                                                  olist=pedido_olist)
                if not ack:
                    msg = f"Erro ao enviar nunota para o pedido {pedido.get('num_pedido')} no Olist"
                    raise Exception(msg)
            return {"success": True}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    @internal_only
    async def atualizar_nunota(
            self,
            id_pedido:int,
            nunota:int,
            olist:PedidoOlist
        ) -> bool:
        
        dados_pedido = await olist.buscar(id=id_pedido)
        if not dados_pedido:
            return False
        ack = await olist.atualizar_nunota(id=id_pedido,
                                           nunota=nunota,
                                           observacao=dados_pedido.get('observacoesInternas'))
        if not ack:
            return False         
        return True

    @contexto
    @log_execucao
    @ensure_dados_ecommerce        
    async def integrar_novos(
            self,
            **kwargs
        ) -> bool:
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='base',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))
        # Busca pedidos para importar
        print("-> Buscando pedidos para importar...")
        pedidos_importar = await crudPedido.buscar_importar(ecommerce_id=self.dados_ecommerce.get('id'))
        if not pedidos_importar:
            print("--> Nenhum pedido para importar.")
            await crudLog.atualizar(id=self.log_id)
            return True        
        print(f"{len(pedidos_importar)} pedidos para importar")
        
        # Verifica o tipo de importação do ecommerce
        if self.dados_ecommerce.get('importa_pedido_lote'):
            print("-> Importando pedidos em lote...")
            ack = await self.importar_agrupado(lista_pedidos=pedidos_importar)
            # Registra sucesso no log
            print("-> Registrando log...")
            for pedido in pedidos_importar:
                await crudLogPed.criar(log_id=self.log_id,
                                       pedido_id=pedido.get('id'),
                                       evento='I',
                                       status=ack.get('success'),
                                       obs=ack.get('__exception__',None))            
        else:
            for i, pedido in enumerate(pedidos_importar):
                time.sleep(self.req_time_sleep)
                print(f"-> Pedido {i + 1}/{len(pedidos_importar)}: {pedido.get('num_pedido')}")
                ack = await self.importar_unico(dados_pedido=pedido)
                # Registra sucesso no log
                print("Registrando log...")
                await crudLogPed.criar(log_id=self.log_id,
                                       pedido_id=pedido.get('id'),
                                       evento='I',
                                       status=ack.get('success'),
                                       obs=ack.get('__exception__',None))                  
        
        # Atualiza log
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        print("--> INTEGRAÇÃO DOS PEDIDOS CONCLUÍDA!")
        return True

    @contexto
    @ensure_dados_ecommerce
    async def confirmar(
            self,
            nunota:int,
            **kwargs
        ) -> dict:

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='sankhya',
                                              para='sankhya',
                                              contexto=kwargs.get('_contexto'))

        try:
            pedido_snk = PedidoSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
            # Verifica se o pedido já foi confirmado e só não foi atualizado na base do integrador
            print("Verificando se o pedido já foi confirmado...")
            validacao = await pedido_snk.buscar(nunota=nunota)
            if validacao.get('statusnota' == 'L'):
                print(f"Pedido {nunota} já foi confirmado.")
                ack = await crudPedido.atualizar(nunota=nunota,
                                                dh_confirmado=validacao.get('dtmov'))
                if not ack:
                    msg = f"Erro ao atualizar situação do pedido {nunota} para confirmado"
                    raise Exception(msg)            
            # Confirma pedido
            print("Confirmando pedido...")
            ack = await pedido_snk.confirmar(nunota=nunota)
            if not ack:
                msg = f"Erro ao confirmar pedido {nunota} no Sankhya"
                raise Exception(msg)            
            # Atualiza log
            print("Atualizando log...")
            ack = await crudPedido.atualizar(nunota=nunota,
                                            dh_confirmado=datetime.now())
            if not ack:
                msg = f"Erro ao atualizar situação do pedido {nunota} para confirmado"
                raise Exception(msg)            
            return {"success": True}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    @contexto
    @log_execucao
    @ensure_dados_ecommerce
    async def integrar_confirmacao(
            self,
            **kwargs
        ) -> bool:

        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='sankhya',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))
        # Busca pedidos para confirmar
        print("-> Buscando pedidos para confirmar...")
        pedidos_confirmar = await crudPedido.buscar_confirmar(ecommerce_id=self.dados_ecommerce.get('id'))
        if not pedidos_confirmar:
            print("--> Nenhum pedido para confirmar.")
            await crudLog.atualizar(id=self.log_id)            
            return True        
        print(f"{len(pedidos_confirmar)} pedidos para confirmar")
        for i, pedido in enumerate(pedidos_confirmar):
            time.sleep(self.req_time_sleep)
            print(f"-> Pedido {i + 1}/{len(pedidos_confirmar)}: {pedido.get('nunota')}")
            ack = await self.confirmar(nunota=pedido.get('nunota'))
            # Registra sucesso no log
            await crudLogPed.criar(log_id=self.log_id,
                                   pedido_id=pedido.get('id'),
                                   evento='C',
                                   status=ack.get('success'),
                                   obs=ack.get('__exception__',None))
        # Atualiza log
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        print("--> CONFIRMAÇÃO DOS PEDIDOS CONCLUÍDA!")
        return True

    @contexto
    @ensure_dados_ecommerce
    async def conferir(
            self,
            **kwargs
        ) -> dict:
        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='sankhya',
                                              para='sankhya',
                                              contexto=kwargs.get('_contexto'))

        conferencia = ConferenciaSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))

        # Busca pedidos para conferir
        print(f"Buscando pedidos da loja {self.dados_ecommerce.get('nome')} para conferir...")
        lista_pedidos = await conferencia.buscar_aguardando_conferencia(id_loja=self.id_loja)
        if not lista_pedidos:
            print("--> Nenhum pedido para conferir.")
            await crudLog.atualizar(id=self.log_id)
            return True        
        print(f"{len(lista_pedidos)} pedidos para conferir")        
        
        # parser_conferencia = ParserConferencia()
        # for i, pedido in enumerate(lista_pedidos):
        #     time.sleep(self.req_time_sleep)
        #     print("")
        #     print(f"Pedido {i+1}/{len(lista_pedidos)}: {pedido.get('nunota')}")
        #     print("Busca a nota")
        #     dados_nota = await nota_olist.buscar_legado(id_ecommerce=pedido.get('ad_mkp_codped'))
        #     if not dados_nota:
        #         obs = "Erro ao buscar nota"
        #         print(obs)             
        #         continue
        #     print("Gerando conferência do pedido")            
        #     if not await conferencia.criar(nunota=pedido.get('nunota')):
        #         obs = "Erro ao criar conferência do pedido no Sankhya"
        #         print(obs)
        #         continue
        #     # Vincula a conferencia ao pedido
        #     print("Vincula a conferencia ao pedido")
        #     if not await conferencia.vincular_pedido(nunota=pedido.get('nunota'), nuconf=conferencia.nuconf):
        #         obs = "Erro ao vincular conferência ao pedido no Sankhya"
        #         print(obs)
        #         continue
        #     # Informa os itens na conferência
        #     print("Informa os itens na conferência")
        #     itens_para_conferencia = parser_conferencia.to_sankhya_itens(nuconf=conferencia.nuconf, dados_olist=dados_nota.get('itens'))
        #     if not itens_para_conferencia:
        #         obs = "Erro ao converter itens da nota para o formato da API do Sankhya"
        #         print(obs)
        #         continue
        #     ack_insercao_itens = await conferencia.insere_itens(dados_item=itens_para_conferencia)
        #     if not ack_insercao_itens:
        #         obs = "Erro ao inserir itens na conferência no Sankhya"
        #         print(obs)
        #         continue
        #     print("Itens inseridos na conferência")            
        #     # Conclui a conferência do pedido
        #     print("Conclui a conferência do pedido")
        #     if not await conferencia.concluir(nuconf=conferencia.nuconf):
        #         obs = "Erro ao concluir conferência do pedido no Sankhya"
        #         print(obs)
        #         continue
        #     print(f"Conferência do pedido {pedido.get('nunota')} concluída!")
        # status_log = False if obs else True
        # return status_log
        return True

