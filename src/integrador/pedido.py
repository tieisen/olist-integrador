import os
import re
import time
from tqdm import tqdm
from datetime import datetime
from src.olist.pedido import Pedido as PedidoOlist
from src.sankhya.pedido import Pedido as PedidoSnk
from src.parser.pedido import Pedido as ParserPedido
from src.olist.produto import Produto as ProdutoOlist
# from src.sankhya.conferencia import Conferencia as ConferenciaSnk
from database.crud import pedido as crudPedido
from database.crud import log_pedido as crudLogPed
from database.crud import log as crudLog
from src.services.viacep import Viacep
from src.utils.decorador import contexto, carrega_dados_ecommerce, carrega_dados_empresa, log_execucao, interno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Pedido:

    def __init__(self, id_loja:int=None, codemp:int=None):
        self.id_loja:int=id_loja
        self.codemp:int=codemp
        self.empresa_id:int=None
        self.log_id:int=None
        self.contexto:str='pedido'
        self.dados_ecommerce:dict={}
        self.dados_empresa:dict={}
        self.req_time_sleep:float=float(os.getenv('REQ_TIME_SLEEP', 1.5))
        self.pedido_cancelado:int=int(os.getenv('OLIST_SIT_PEDIDO_CANCELADO'))
        self.pedido_incompleto:int=int(os.getenv('OLIST_SIT_PEDIDO_INCOMPLETO'))

    @interno
    async def validar_cancelados(
            self,
            lista_pedidos: list[dict]
        ) -> list:

        lista_ids = [p.get('id') for p in lista_pedidos]
        pedidos_nao_cancelados = await crudPedido.buscar_cancelar(lista=lista_ids)
        lista_pedidos_pendentes_cancelar = [p.get('id_pedido') for p in pedidos_nao_cancelados]
        return lista_pedidos_pendentes_cancelar
    
    @interno
    async def validar_existentes(
            self,
            lista_pedidos: list
        ) -> list:      

        lista_ids = [p.get('id') for p in lista_pedidos]
        pedidos_existentes = await crudPedido.buscar(lista=lista_ids)
        lista_pedidos_existentes = [p.get('id_pedido') for p in pedidos_existentes]
        pedidos_pendentes = [p for p in lista_pedidos if p.get('id') not in lista_pedidos_existentes]
        return pedidos_pendentes
    
    @interno
    def validar_loja(
            self,
            lista_pedidos: list
        ) -> list:        
        return [p for p in lista_pedidos if p['ecommerce'].get('id') == self.id_loja]

    @interno
    async def validar_situacao(
            self,
            dados_pedido:dict
        ) -> bool:

        if not isinstance(dados_pedido,dict):
            try:
                dados_pedido = dados_pedido[0]
            except Exception as e:
                return False
        
        if dados_pedido.get('situacao') == self.pedido_cancelado:
            # Cancelado
            await crudPedido.atualizar(id_pedido=dados_pedido.get('id'),
                                       dh_cancelamento=datetime.now())
            return False
        elif dados_pedido.get('situacao') == self.pedido_incompleto:
            # Dados incompletos
            return False
        else:
            return True

    @contexto
    @interno
    @carrega_dados_ecommerce
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
                # print(f"Buscando dados do pedido {num_pedido or id_pedido}...")
                if num_pedido:
                    dados_pedido = await pedido_olist.buscar(numero=num_pedido)
                if id_pedido:
                    dados_pedido = await pedido_olist.buscar(id=id_pedido)            
                if not dados_pedido:
                    msg = f"Erro ao buscar dados do pedido {num_pedido or id_pedido} no Olist"
                    raise Exception(msg)
                # Valida situação
                # print("Validando situação do pedido...")         
                if not await self.validar_situacao(dados_pedido):
                    msg = f"Pedido {num_pedido or id_pedido} cancelado ou com dados incompletos"
                    raise Exception(msg)

            # Valida itens e desmembra kits
            print("Validando itens e desmembrando kits...")
            itens_validados = await self.validar_item_desmembrar_kit(itens=dados_pedido.get('itens'),
                                                                     olist=pedido_olist)
            if not itens_validados:
                msg = "Erro ao validar itens/desmembrar kits"                    
                raise Exception(msg)
            
            dados_pedido['itens'] = itens_validados

            # Adiciona pedido na base
            print("Adicionando pedido na base...")
            id = await crudPedido.criar(id_loja=dados_pedido['ecommerce'].get('id'),
                                        id_pedido=dados_pedido.get('id'),
                                        cod_pedido=dados_pedido['ecommerce'].get('numeroPedidoEcommerce'),
                                        num_pedido=dados_pedido.get('numeroPedido'),
                                        dados_pedido=dados_pedido)
            if not id:
                msg = f"Erro ao adicionar pedido {dados_pedido.get('numeroPedido')} à base de dados."
                raise Exception(msg)
            print(f"Pedido {dados_pedido.get('numeroPedido')} adicionado à base de dados.")
            return {"success": True, "id": id}
        except Exception as e:
            logger.error("Erro ao receber pedido %s. %s",num_pedido or id_pedido,e)
            return {"success": False, "id": None, "__exception__": str(e)}

    @interno
    @carrega_dados_ecommerce
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

    @interno
    @carrega_dados_ecommerce
    async def consultar_cancelamentos(self) -> list:
        pedido_olist = PedidoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
        # Busca pedidos cancelados
        print("Buscando pedidos cancelados...")
        lista = await pedido_olist.buscar(cancelados=True)            
        if isinstance(lista,bool):
            if not lista:
                # Erro na busca
                print("Erro ao buscar pedidos cancelados")
                return False            
        if isinstance(lista,list):
            if not lista:
                # Nenhum pedido cancelado encontrado
                print("Nenhum pedido cancelado encontrado")
                return True
        # Valida pedidos cancelados
        print("Validando pedidos cancelados...")
        lista_pedidos = await self.validar_cancelados(lista)
        if not lista_pedidos:
            # Todos os pedidos já foram cancelados
            print("Todos os pedidos já foram cancelados")
            return True        
        # Registra cancelamentos
        print("Registrando cancelamentos...")
        ack:list=[]
        for i in lista_pedidos:
            ack.append(await self.registrar_cancelamento(id_pedido=i))
        return all(ack)

    @interno
    async def registrar_cancelamento(self, id_pedido:int) -> bool:        
        return await crudPedido.atualizar(id_pedido=id_pedido,dh_cancelamento=datetime.now())

    @contexto
    @log_execucao
    @carrega_dados_ecommerce
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
                                   pedido_id=ack.get('id'),
                                   evento='R',
                                   sucesso=ack.get('success'),
                                   obs=ack.get('__exception__',None))            
        else:
            # Consulta pedidos novos
            print("-> Consultando pedidos novos...")
            pedidos_novos = await self.consultar_pedidos_novos(atual=atual)
            if isinstance(pedidos_novos, list):
                pedidos_novos = self.validar_loja(lista_pedidos=pedidos_novos)                
                print(f"{len(pedidos_novos)} pedidos para receber")
                for i, pedido in tqdm(enumerate(pedidos_novos),desc="Processando..."):
                    time.sleep(self.req_time_sleep)
                    # print(f"-> Pedido {i + 1}/{len(pedidos_novos)}: {pedido.get("numeroPedido")}")            
                    # ack = await self.receber(dados_pedido=pedido)
                    ack = await self.receber(id_pedido=pedido.get('id'))
                    # Registra sucesso no log
                    await crudLogPed.criar(log_id=self.log_id,
                                           pedido_id=ack.get('id'),
                                           evento='R',
                                           sucesso=ack.get('success'),
                                           obs=ack.get('__exception__',None))
            elif isinstance(pedidos_novos,bool):
                # Retornou True ou False
                pass
        if isinstance(pedidos_novos,bool):
            status_log = pedidos_novos
        else:
            status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        return status_log

    @carrega_dados_empresa    
    async def validar_unidade(self, dados_item:dict):        
        produto_olist = ProdutoOlist(codemp=self.codemp)
        dados_produto:dict = await produto_olist.buscar(id=dados_item['produto'].get('id'))
        if not dados_produto:
            return False
        dados_item['unidade'] = dados_produto.get('unidade')
        return dados_item        

    async def validar_item_desmembrar_kit(self, itens:list[dict], olist:PedidoOlist):
        itens_validados:list=[]

        for item in itens:
            time.sleep(self.req_time_sleep)  # Evita rate limit
            # Kits não tem vínculo por SKU, ou estão marcados com #K no final do código
            if item['produto'].get('sku') and '#K' not in item['produto'].get('sku'):
                item = await self.validar_unidade(dados_item=item)
                itens_validados.append(item)
            else:
                try:
                    ack, kit_desmembrado = await olist.validar_kit(id=item['produto'].get('id'),item_no_pedido=item)
                    time.sleep(self.req_time_sleep)  # Evita rate limit
                    if ack:
                        itens_validados+=kit_desmembrado
                except Exception as e:
                    logger.error(f"Erro: {e}")
                    return False
        return itens_validados

    @contexto
    @interno
    @carrega_dados_ecommerce
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
                
                # Valida situação
                print("Validando situação do pedido...")
                if not await self.validar_situacao(dados_pedido_olist):
                    msg = "Pedido cancelado ou com dados incompletos"
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
        
        pedidos:list[dict]=[]
        itens:list[dict]=[]

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
                    'unidade': item_pedido.get('unidade'),
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
    @interno
    @carrega_dados_ecommerce
    async def importar_agrupado(
            self,
            lista_pedidos:list[dict],
            **kwargs
        ) -> list[dict]:
        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='base',
                                              para='sankhya',
                                              contexto=kwargs.get('_contexto'))
        
        pedido_olist = PedidoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
        pedido_snk = PedidoSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        dados_pedidos_olist:list[dict]=[]

        try:
            aux_lista_pedidos = lista_pedidos.copy()
            for i, pedido in enumerate(aux_lista_pedidos):
                time.sleep(self.req_time_sleep)
                print(f"-> Pedido {i + 1}/{len(aux_lista_pedidos)}: {pedido.get('num_pedido')}")
                
                # # Busca dados do pedido no Olist
                print("Buscando dados do pedido no Olist...")
                # dados_pedido_olist = await pedido_olist.buscar(id=pedido.get('id_pedido'))
                dados_pedido_olist = await crudPedido.buscar(id_pedido=pedido.get('id_pedido'))
                if not dados_pedido_olist:
                    msg = f"Erro ao buscar dados do pedido {pedido.get('num_pedido')}"
                    print(msg)
                    logger.error(msg)
                    lista_pedidos.pop(lista_pedidos.index(pedido))
                    continue
            
                try:
                    dados_pedido_olist = dados_pedido_olist[0].get('dados_pedido')
                except Exception as e:
                    msg = f"Erro ao buscar dados do pedido {pedido.get('num_pedido')}"
                    print(msg)
                    logger.error(msg)
                    lista_pedidos.pop(lista_pedidos.index(pedido))
                    continue

                if not dados_pedido_olist.get('itens'):
                    msg = f"Erro ao buscar itens do pedido {pedido.get('num_pedido')}"
                    print(msg)
                    logger.error(msg)
                    lista_pedidos.pop(lista_pedidos.index(pedido))
                    continue
                
                # Valida situação e remove se cancelado
                print("Validando situação do pedido...")
                if not await self.validar_situacao(dados_pedido_olist):
                    msg = f"Pedido {pedido.get('num_pedido')} cancelado ou com dados incompletos"
                    print(msg)
                    logger.error(msg)
                    lista_pedidos.pop(lista_pedidos.index(pedido))
                    continue
                
                dados_pedidos_olist.append(dados_pedido_olist)

            if not dados_pedidos_olist:
                msg = "Nenhum pedido válido para importar"
                raise Exception(msg)            
            print(f"--> {len(dados_pedidos_olist)} pedidos validados para importar")
            logger.info(f"{len(dados_pedidos_olist)} pedidos validados para importar")

            # Unifica os itens dos pedidos
            print("-> Unificando os pedidos...")
            pedidos_agrupados, itens_agrupados = self.unificar(lista_pedidos=dados_pedidos_olist)
            if not all([pedidos_agrupados, itens_agrupados]):
                msg = "Erro ao unificar pedidos"
                raise Exception(msg)

            # Converte para o formato da API do Sankhya
            parser = ParserPedido(id_loja=self.id_loja)
            print("-> Convertendo para o formato da API do Sankhya...")        
            dados_cabecalho, dados_itens = await parser.to_sankhya_lote(lista_pedidos=pedidos_agrupados,
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

            # Atualiza log / Envia nunota para os pedidos nos Olist
            print("-> Finalizando...")
            lista_retornos:list[dict]=[]
            for pedido in lista_pedidos:
                time.sleep(self.req_time_sleep)
                retorno = {
                    "id": pedido.get('id'),
                    "numero": pedido.get('num_pedido'),
                    "success": None,
                    "__exception__": None
                }
                ack = await crudPedido.atualizar(id_pedido=pedido.get('id_pedido'),
                                                 nunota=pedido_incluido,
                                                 dh_importacao=datetime.now())
                if not ack:
                    msg = f"Erro ao atualizar situação do pedido {pedido.get('num_pedido')} para importado"
                    logger.error(msg)
                    retorno['success'] = False
                    retorno['__exception__'] = msg
                    lista_retornos.append(retorno)
                    continue
                                
                ack = await self.atualizar_nunota(id_pedido=pedido.get('id_pedido'),
                                                  nunota=pedido_incluido,
                                                  olist=pedido_olist)
                if not ack:
                    msg = f"Erro ao enviar nunota para o pedido {pedido.get('num_pedido')} no Olist"
                    logger.error(msg)
                    retorno['success'] = False
                    retorno['__exception__'] = msg
                    lista_retornos.append(retorno)
                    continue

                retorno['success'] = True
                lista_retornos.append(retorno)
                
            return lista_retornos
        except Exception as e:
            logger.error("Erro ao importar pedidos em lote. %s",e)
            return [{"id": None, "numero": None, "success": False, "__exception__": str(e)}]

    @interno
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
                                           observacao=dados_pedido.get('observacoes'))
        if not ack:
            return False         
        return True

    @contexto
    @log_execucao
    @carrega_dados_ecommerce        
    async def integrar_novos(
            self,
            **kwargs
        ) -> bool:
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='base',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))
        
        # Valida cancelamentos
        print("-> Validando cancelamentos...")        
        if not await self.consultar_cancelamentos():
            print("Erro ao validar cancelamentos")

        # Busca pedidos para importar
        print("-> Buscando pedidos para importar...")
        pedidos_importar = await crudPedido.buscar_importar(ecommerce_id=self.dados_ecommerce.get('id'))
        if not pedidos_importar:
            print("--> Nenhum pedido para importar.")
            await crudLog.atualizar(id=self.log_id)
            return True        
        print(f"{len(pedidos_importar)} pedidos para importar")
        logger.info(f"{len(pedidos_importar)} pedidos para importar")
        
        # Verifica o tipo de importação do ecommerce
        if self.dados_ecommerce.get('importa_pedido_lote'):
            print("-> Importando pedidos em lote...")
            ack_importacao = await self.importar_agrupado(lista_pedidos=pedidos_importar)
            # Registra no log
            print("-> Registrando log...")
            for pedido in ack_importacao:
                if not pedido.get('success'):
                    logger.error(f"Erro ao importar pedido {pedido.get('numero')}: {pedido.get('__exception__',None)}")
                    print(f"Erro ao importar pedido {pedido.get('numero')}: {pedido.get('__exception__',None)}")
                await crudLogPed.criar(log_id=self.log_id,
                                       pedido_id=pedido.get('id'),
                                       evento='I',
                                       sucesso=pedido.get('success'),
                                       obs=pedido.get('__exception__',None))            
        else:
            for i, pedido in enumerate(pedidos_importar):
                time.sleep(self.req_time_sleep)
                print(f"-> Pedido {i + 1}/{len(pedidos_importar)}: {pedido.get('num_pedido')}")
                ack_importacao = await self.importar_unico(dados_pedido=pedido)
                # Registra sucesso no log
                print("Registrando log...")
                await crudLogPed.criar(log_id=self.log_id,
                                       pedido_id=pedido.get('id'),
                                       evento='I',
                                       sucesso=ack_importacao.get('success'),
                                       obs=ack_importacao.get('__exception__',None))                  
        
        # Atualiza log
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        return status_log

    @contexto
    @carrega_dados_ecommerce
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
                                                 dh_confirmacao=validacao.get('dtmov'))
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
                                             dh_confirmacao=datetime.now())
            if not ack:
                msg = f"Erro ao atualizar situação do pedido {nunota} para confirmado"
                raise Exception(msg)            
            return {"success": True}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    @interno
    def formata_lista_pedidos_confirmar(self, lista_pedidos_confirmar:list[dict]):
        lista_nunotas = list(set(pedido.get('nunota') for pedido in lista_pedidos_confirmar))
        lista_pedidos:list[dict] = []
        for i, nunota in enumerate(lista_nunotas):
            lista_pedidos.append({'nunota':nunota,'pedidos':[]})
            for pedido in lista_pedidos_confirmar:
                if pedido.get('nunota') == nunota:
                    lista_pedidos[i]['pedidos'].append(pedido.get('id'))        
        return lista_pedidos

    @contexto
    @log_execucao
    @carrega_dados_ecommerce
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
        pedidos_pendente_confirmar = await crudPedido.buscar_confirmar(ecommerce_id=self.dados_ecommerce.get('id'))
        if not pedidos_pendente_confirmar:
            print("--> Nenhum pedido para confirmar.")
            await crudLog.atualizar(id=self.log_id)            
            return True
        
        # Verifica o tipo de importação do ecommerce
        if self.dados_ecommerce.get('importa_pedido_lote'):
            pedidos_confirmar = self.formata_lista_pedidos_confirmar(lista_pedidos_confirmar=pedidos_pendente_confirmar)
        
        print(f"{len(pedidos_confirmar)} pedidos para confirmar")
        for i, pedido in enumerate(pedidos_confirmar):
            time.sleep(self.req_time_sleep)
            print(f"-> Pedido {i + 1}/{len(pedidos_confirmar)}: {pedido.get('nunota')}")
            ack = await self.confirmar(nunota=pedido.get('nunota'))
            
            # Registra sucesso no log
            if pedido.get('id_pedido'):
                await crudLogPed.criar(log_id=self.log_id,
                                       pedido_id=pedido.get('id'),
                                       evento='C',
                                       sucesso=ack.get('success'),
                                       obs=ack.get('__exception__',None))
            elif pedido.get('pedidos'):
                for pedido_id in pedido.get('pedidos'):
                    await crudLogPed.criar(log_id=self.log_id,
                                           pedido_id=pedido_id,
                                           evento='C',
                                           sucesso=ack.get('success'),
                                           obs=ack.get('__exception__',None))                    
        
        # Atualiza log
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        return True

    @contexto
    @log_execucao
    @carrega_dados_empresa
    async def integrar_cancelamento(
            self,
            nunota:int,
            **kwargs
        ) -> bool:

        self.log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                          de='sankhya',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))
        
        # Valida pedido
        print("-> Validando pedido...")
        pedidos_cancelar = await crudPedido.buscar(nunota=nunota)
        if not pedidos_cancelar:
            print("--> Pedido não encontrado. Exclua diretamente no Sankhya")
            await crudLog.atualizar(id=self.log_id,sucesso=False)            
            return True
        
        pedido_snk = PedidoSnk(empresa_id=self.dados_empresa.get('id'))
        pedido_olist = PedidoOlist(empresa_id=self.dados_empresa.get('id'))

        # Busca pedido Sankhya
        dados_pedido_snk = await pedido_snk.buscar(nunota=nunota)
        if not dados_pedido_snk:
            print("--> Pedido não encontrado no Sankhya.")
            await crudLog.atualizar(id=self.log_id,sucesso=False)            
            return True

        # Cancela pedido
        print("-> Cancelando pedido...")
        ack = await pedido_snk.excluir(nunota=nunota)
        if not ack:
            print("--> Erro ao cancelar pedido.")
            await crudLog.atualizar(id=self.log_id,sucesso=False)            
            return True
        
        # Remove vínculo do pedido
        print("-> Removendo vínculo do pedido...")
        ack = await crudPedido.cancelar(nunota=nunota)
        if not ack:
            print("--> Erro ao remover vínculo do pedido.")
            await crudLog.atualizar(id=self.log_id,sucesso=False)            
            return True
        
        # Remove observação
        print("-> Removendo observação no Olist...")
        for pedido in pedidos_cancelar:
            ack = await pedido_olist.remover_nunota(id=pedido.get('id_pedido'),nunota=nunota)
            if not ack:
                print(f"--> Erro ao remover observação do pedido {pedido.get('num_pedido')} no Olist.")
        
        # Atualiza log
        print("-> Atualizando log...")
        ack = await crudLog.atualizar(id=self.log_id)

        return True

    @contexto
    @log_execucao
    @carrega_dados_empresa
    async def anular_pedido_importado(
            self,
            nunota:int,
            **kwargs
        ) -> dict:
        """ Exclui pedido que ainda não foi conferido do Sankhya. """

        res:dict={}
        msg:str=None
        status:bool=None
        erro:str=None
        snk = PedidoSnk(codemp=self.codemp)
        olist = PedidoOlist(codemp=self.codemp)
        self.log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                          de='sankhya',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))

        try:
            # Validando pedido no Sankhya
            print("-> Validando pedido no Sankhya...")
            dados_snk = await snk.buscar_nota_do_pedido(nunota=nunota)        
            if isinstance(dados_snk,bool):
                msg = f"Pedido {nunota} não encontrado no Sankhya"
                raise Exception(msg)
            
            if isinstance(dados_snk,dict):
                msg = f"Pedido já foi faturado e não pode ser excluído"
                raise Exception(msg)

            # Exclui pedido no Sankhya
            print(f"-> Excluindo pedido {nunota} no Sankhya...")
            ack = await snk.excluir(nunota=nunota)
            if not ack:
                msg = f"Erro ao excluir pedido {nunota} no Sankhya"
                raise Exception(msg)

            # Busca pedidos relacionados no Olist
            lista_pedidos = await crudPedido.buscar(nunota=nunota)
            if not lista_pedidos:
                msg = f"Erro ao buscar pedidos relacionados à nunota {nunota}"
                raise Exception(msg)

            print("Atualizando pedidos no Olist...")
            lista_pedidos_com_erro:list[str]=[]
            for i, pedido in enumerate(lista_pedidos):
                time.sleep(self.req_time_sleep)  # Evita rate limit
                ack = await olist.remover_nunota(id=pedido.get('id_pedido'),nunota=nunota)
                if not ack:
                    await crudLogPed.criar(log_id=self.log_id,
                                           pedido_id=pedido.get('id'),
                                           evento='N',
                                           sucesso=False,
                                           obs="Não foi possível remover nunota")
                    lista_pedidos_com_erro.append(str(pedido.get('num_pedido')))
                else:
                    await crudLogPed.criar(log_id=self.log_id,
                                           pedido_id=pedido.get('id'),
                                           evento='N')
            if len(lista_pedidos_com_erro) == len(lista_pedidos):
                msg = "Erro ao remover número dos pedidos no Olist"
                raise Exception(msg)
            if lista_pedidos_com_erro:
                msg = f"Não foi possível remover número do(s) pedido(s) {', '.join(lista_pedidos_com_erro)} no Olist"
                status = True
                raise Exception(msg)

            if not await crudPedido.cancelar(nunota=nunota):
                msg = f"Não foi possível limpar os pedidos na base"                
                raise Exception(msg) 
                       
            status = True            
        except Exception as e:
            if status is True:
                pass
            erro = f'ERRO: {e}'
            print(erro)
            logger.error(erro)
            status = False            
        finally:        
            # Atualiza log
            status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
            await crudLog.atualizar(id=self.log_id,sucesso=status_log)
            res = {
                "sucesso":status_log,
                "__exception__":erro
            }
            return res
