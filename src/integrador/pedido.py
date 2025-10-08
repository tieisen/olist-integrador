import os
import re
import time
import logging
from dotenv import load_dotenv
from src.olist.pedido import Pedido as PedidoOlist
from src.olist.nota import Nota as NotaOlist
from src.sankhya.pedido import Pedido as PedidoSnk
from src.parser.pedido import Pedido as ParserPedido
from src.sankhya.nota import Nota as NotaSnk
from src.sankhya.conferencia import Conferencia as ConferenciaSnk
from src.parser.conferencia import Conferencia as ParserConferencia
from src.services.viacep import Viacep
from database.crud import venda, log, log_pedido
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

CONTEXTO = 'pedido'

class Pedido:

    def __init__(self):
        """ Inicializa a classe Pedido com a conexão ao Olist e o endpoint de pedidos. """
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))

    def validar_existentes(self, lista_pedidos: list) -> list:
        """ Valida se os pedidos já existem na base de dados.
        Args:
            lista_pedidos (list): Lista de IDs de pedidos a serem validados.
        Returns:
            list: Lista de IDs de pedidos que não existem na base de dados.
        """
        if not lista_pedidos:
            return []
        
        existentes = venda.buscar_lista_id(lista_pedidos)
        existentes = [p.id_pedido for p in existentes if p.id_pedido in lista_pedidos]
        return [pedido for pedido in lista_pedidos if pedido not in existentes]

    async def validar_cancelamentos(self):

        print("Validando pedidos cancelados...")
        pedido = PedidoOlist()
        nota = NotaOlist()

        pedidos_cancelados = await pedido.buscar(cancelados=True)
        notas_canceladas = await nota.buscar_canceladas()

        if not any([pedidos_cancelados, notas_canceladas]):
            # Nenhum pedido ou nota cancelada encontrado
            print("Nenhum pedido ou nota cancelada encontrado.")
            return True

        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO+'_cancelamentos')
        pedidos_pendente_cancelar_integrador = venda.validar_cancelamentos(lista_ids=pedidos_cancelados)
        if pedidos_pendente_cancelar_integrador:
            print(f"Pedidos pendentes de cancelamento no integrador: {len(pedidos_pendente_cancelar_integrador)}")
            for pedido in pedidos_pendente_cancelar_integrador:
                venda.atualizar_cancelada(id_pedido=pedido.id_pedido)                
            print("Pedidos atualizados no integrador com sucesso!")

        if notas_canceladas:
            print(f"Notas canceladas no Olist: {len(notas_canceladas)}")
            for nota_cancelada in notas_canceladas:
                print(nota_cancelada)
                venda.atualizar_cancelada(id_nota=nota_cancelada)                
            print("Notas atualizadas no integrador com sucesso!")

        print("Validação de pedidos cancelados concluída!")

        log.atualizar(id=log_id, sucesso=True)

        return True

    async def receber(self, lista_pedidos:list=None, atual:bool=True, num_pedido:int=None) -> bool:
        """ Recebe pedidos do Olist e os adiciona à base de dados.
        Args:
            lista_pedidos (list, optional): Lista de IDs de pedidos a serem recebidos. Se None, busca todos os pedidos novos.            
        Returns:
            bool: True se os pedidos foram recebidos com sucesso, False caso contrário.
        """

        ped_olist = PedidoOlist()

        if num_pedido:
            log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO+'_receber')
            # Importando pedido único
            print(f"Recebendo pedido {num_pedido}...")
            dados_pedido = await ped_olist.buscar(numero=num_pedido)
            if not dados_pedido:
                obs = f"Erro ao buscar dados do pedido {num_pedido} no Olist"
                print(obs)
                logger.error(obs)
                log_pedido.criar(log_id=log_id,
                                 id_loja=0,
                                 id_pedido=0,
                                 pedido_ecommerce='',
                                 status=False,
                                 obs=obs)                
                return False

            if dados_pedido.get('situacao') == 8:
                obs = f"Pedido {dados_pedido.get('numeroPedido')} dados incompletos"                
                log_pedido.criar(log_id=log_id,
                                 id_loja=dados_pedido['ecommerce'].get('id'),
                                 id_pedido=dados_pedido.get('id'),
                                 pedido_ecommerce=dados_pedido['ecommerce'].get('numeroPedidoEcommerce'),
                                 status=False,
                                 obs=obs)
                return False

            ack = venda.criar(id_loja=dados_pedido['ecommerce'].get('id'),
                              id_pedido=dados_pedido.get('id'),
                              cod_pedido=dados_pedido['ecommerce'].get('numeroPedidoEcommerce'),
                              num_pedido=dados_pedido.get('numeroPedido'),
                              dados_pedido=dados_pedido)
            if not ack:
                obs = f"Erro ao adicionar pedido {dados_pedido.get('numeroPedido')} à base de dados."
                print(obs)
                logger.error(obs)
                log_pedido.criar(log_id=log_id,
                                 id_loja=dados_pedido['ecommerce'].get('id'),
                                 id_pedido=dados_pedido.get('id'),
                                 pedido_ecommerce=dados_pedido['ecommerce'].get('numeroPedidoEcommerce'),
                                 status=False,
                                 obs=obs)
                return False

            log_pedido.criar(log_id=log_id,
                             id_loja=dados_pedido['ecommerce'].get('id'),
                             id_pedido=dados_pedido.get('id'),
                             pedido_ecommerce=dados_pedido['ecommerce'].get('numeroPedidoEcommerce'))
            
            print(f"Pedido {dados_pedido.get('numeroPedido')} adicionado à base de dados.")
            return True
        
        # Importando pedidos em lote
        print("Recebendo pedidos...")
        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO+'_receber')
        if not lista_pedidos:
            ack, lista = await ped_olist.buscar_novos(atual=atual)            
            if not ack:
                print("Nenhum pedido encontrado.")
                logger.info("Nenhum pedido encontrado.")
                log.atualizar(id=log_id,sucesso=False)
                return False
            print(f"Pedidos encontrados: {len(lista)}")
            lista_pedidos = self.validar_existentes(lista)
            
            if not lista_pedidos:
                print("Todos os pedidos já existem na base de dados.")
                logger.info("Todos os pedidos já existem na base de dados.")
                log.atualizar(id=log_id)
                return True
        
        print(f"Pedidos a serem recebidos: {len(lista_pedidos)}")
        first = True
        obs = None
        for i, pedido in enumerate(lista_pedidos):
            if not first:
                time.sleep(self.req_time_sleep)  # Evita rate limit
            first = False

            if obs:
                # Cria um log de erro se houver observação
                print(obs)
                log_pedido.criar(log_id=log_id,
                                 id_loja=dados_pedido['ecommerce'].get('id'),
                                 id_pedido=dados_pedido.get('id'),
                                 pedido_ecommerce=dados_pedido['ecommerce'].get('numeroPedidoEcommerce'),
                                 status=False,
                                 obs=obs)
                obs = None

            dados_pedido = await ped_olist.buscar(id=pedido)
            if not dados_pedido:
                obs = f"Erro ao buscar dados do pedido {pedido} no Olist"
                logger.error("Erro ao buscar dados do pedido %s no Olist",pedido)
                continue

            if dados_pedido.get('situacao') == 8:
                # Pedido status Dados Incompletos
                continue

            ack = venda.criar(id_loja=dados_pedido['ecommerce'].get('id'),
                              id_pedido=dados_pedido.get('id'),
                              cod_pedido=dados_pedido['ecommerce'].get('numeroPedidoEcommerce'),
                              num_pedido=dados_pedido.get('numeroPedido'),
                              dados_pedido=dados_pedido)
            if not ack:
                obs = f"Erro ao adicionar pedido {dados_pedido.get('numeroPedido')} à base de dados."
                logger.error("Erro ao adicionar pedido %s à base de dados.", dados_pedido.get('numeroPedido'))
                continue

            log_pedido.criar(log_id=log_id,
                             id_loja=dados_pedido['ecommerce'].get('id'),
                             id_pedido=dados_pedido.get('id'),
                             pedido_ecommerce=dados_pedido['ecommerce'].get('numeroPedidoEcommerce'))
            
            print(f"Pedido {dados_pedido.get('numeroPedido')} adicionado à base de dados.")
        
        status_log = False if log_pedido.buscar_status_false(log_id=log_id) else True
        log.atualizar(id=log_id, sucesso=status_log)
        print("Recebimento de pedidos concluído!")

        return True    

    async def importar(self):
        """ Importa novos pedidos do Olist para o Sankhya. """
        obs = None
        evento = 'I'

        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO+'_importar')

        # Busca novos pedidos
        novos_pedidos = venda.buscar_importar()
        if not novos_pedidos:
            print("Nenhum novo pedido encontrado.")
            log.atualizar(id=log_id, sucesso=True)
            return True
        
        print(f"Novos pedidos encontrados: {len(novos_pedidos)}")
        novos_pedidos = [{'id': pedido.id_pedido, 'numero': pedido.num_pedido} for pedido in novos_pedidos]        

        # Inicia as classes de integração
        olist = PedidoOlist()        
        snk = PedidoSnk()
        viacep = Viacep()
        parser = ParserPedido()

        first = True        
        for index, pedido in enumerate(novos_pedidos):
            if not first:
                time.sleep(self.req_time_sleep)  # Evita rate limit
            first = False

            if obs:
                # Cria um log de erro se houver observação
                print(obs)
                log_pedido.criar(log_id=log_id,
                                 id_loja=dados_pedido_olist['ecommerce'].get('id'),
                                 id_pedido=pedido.get('id'),
                                 pedido_ecommerce=dados_pedido_olist['ecommerce'].get('numeroPedidoEcommerce'),
                                 nunota_pedido=0,
                                 evento=evento,
                                 status=False,
                                 obs=obs)

            print("")
            print(f"Importando pedido {index + 1}/{len(novos_pedidos)}: {pedido.get('numero')}")
            # Verifica se o pedido já existe no Olist e só não foi atualizado na base do integrador
            validacao = await snk.buscar(id_olist=pedido.get('id'))
            if validacao:
                print(f"Pedido {pedido.get('numero')} já existe na base de dados.")
                venda.atualizar_importada(id_pedido=pedido.get('id'),
                                          nunota_pedido=validacao.get('nunota'))                 
                continue                      
            
            # Busca os dados do pedido no Olist
            print("Busca os dados do pedido no Olist")
            dados_pedido_olist = await olist.buscar(id=pedido.get('id'))
            if not dados_pedido_olist:
                obs = "Erro ao buscar dados do pedido no Olist ou dados incompletos"
                continue
            
            # Busca os dados da cidade do cliente
            print("Busca os dados da cidade do cliente")
            ibge = await viacep.busca_ibge_pelo_cep(dados_pedido_olist["cliente"]["endereco"].get("cep"))
            if not ibge:
                obs = "Erro ao buscar dados da cidade do cliente no Viacep"
                continue

            dados_cidade = await snk.buscar_cidade(ibge=ibge)
            if not dados_cidade:
                obs = "Erro ao buscar dados da cidade do cliente no Sankhya"
                continue
            
            # Valida itens e desmembra kits
            print("Valida itens e desmembra kits")
            itens_pedido_original = dados_pedido_olist.get('itens')
            itens_pedido_validado = []
            try:
                for item in itens_pedido_original:
                    if item['produto'].get('sku'):
                        itens_pedido_validado.append(item)
                    else:
                        ack, kit_desmembrado = await olist.validar_kit(id=item['produto'].get('id'),item_no_pedido=item)
                        if ack:
                            itens_pedido_validado+=kit_desmembrado
            except Exception as e:
                obs = f"Erro: {e}"
                continue

            if not itens_pedido_validado:
                obs = "Erro ao validar itens/desmembrar kits"
                continue
            
            dados_pedido_olist['itens'] = itens_pedido_validado            
            
            # Converte para o formato da API do Sankhya
            print("Converte para o formato da API do Sankhya")
            data_cabecalho, data_itens = parser.to_sankhya(dados_olist=dados_pedido_olist,dados_cidade=dados_cidade)
            if not data_cabecalho and not data_itens:
                obs = "Erro ao converter dados do pedido para o formato da API do Sankhya"
                continue

            # Insere os dados do pedido
            print("Insere os dados do pedido")
            pedido_incluido = await snk.lancar(dados_cabecalho=data_cabecalho,dados_itens=data_itens)
            if pedido_incluido == 0:
                obs = f"Erro ao inserir pedido no Sankhya."
                continue

            log_pedido.criar(log_id=log_id,
                             id_loja=dados_pedido_olist['ecommerce'].get('id'),
                             id_pedido=pedido.get('id'),
                             pedido_ecommerce=dados_pedido_olist['ecommerce'].get('numeroPedidoEcommerce'),
                             nunota_pedido=pedido_incluido,
                             evento=evento,
                             status=True)
            venda.atualizar_importada(id_pedido=pedido.get('id'),
                                      nunota_pedido=pedido_incluido)
            
            print(f"Pedido #{pedido.get('numero')} importado no código {pedido_incluido}")

            # Manda número do pedido pro Olist
            ack_observacao = await olist.atualizar_nunota(id=pedido.get('id'),
                                                          nunota=pedido_incluido,
                                                          observacao=dados_pedido_olist.get('observacoes'))
            
            if not ack_observacao:
                obs = "Erro ao enviar nunota do pedido para Olist"
                continue
                        
        status_log = False if log_pedido.buscar_status_false(log_id=log_id) else True
        log.atualizar(id=log_id, sucesso=status_log)
        print(f"-> Processo de importação de pedidos concluído! Status do log: {status_log}")
        return True

    async def confirmar(self):
        """ Confirma pedidos importados do Olist no Sankhya. """
        obs = None
        evento = 'C'

        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO+'_confirmar')
        # Busca pedidos pendentes de confirmação
        pedidos_confirmar = venda.buscar_confirmar()
        if not pedidos_confirmar:
            print("Nenhum pedido para confirmar.")
            log.atualizar(id=log_id)        
            return True

        print(f"Pedidos para confirmar: {len(pedidos_confirmar)}")
        pedidos_confirmar = [{'nunota': pedido.nunota_pedido,
                              'numero': pedido.num_pedido,
                              'id': pedido.id_pedido,
                              'separacao': pedido.id_separacao} for pedido in pedidos_confirmar]

        snk = PedidoSnk()
        first = True
        for index, pedido in enumerate(pedidos_confirmar):
            if not first:
                time.sleep(self.req_time_sleep)  # Evita rate limit
            first = False

            if obs:
                # Cria um log de erro se houver observação
                print(obs)
                log_pedido.criar(log_id=log_id,
                                 id_loja=0,
                                 id_pedido=0,
                                 pedido_ecommerce='',
                                 nunota_pedido=pedido.get('nunota'),
                                 evento=evento,
                                 status=False,
                                 obs=obs)
                obs = None
            
            print("")
            print(f"Confirmando pedido {index + 1}/{len(pedidos_confirmar)}: {pedido.get('nunota')}")

            # VALIDACAO DA SEPARACAO NO OLIST
            if not pedido.get('separacao'):
                obs = f"Pedido {pedido.get('numero')} ainda não foi enviado para separacao"
                print(obs)
                continue

            # Verifica se o pedido já foi confirmado e só não foi atualizado na base do integrador
            validacao = await snk.buscar(nunota=pedido.get('nunota'))
            if validacao.get('statusnota' == 'L'):
                print(f"Pedido {pedido.get('numero')} já foi confirmado.")
                venda.atualizar_confirmada(nunota_pedido=pedido.get('nunota'),
                                           dh_confirmado=validacao.get('dtmov'))                
                continue

            ack_confirmacao = await snk.confirmar(nunota=pedido.get('nunota'))
            if not ack_confirmacao:
                obs = f"Erro ao confirmar pedido {pedido.get('nunota')} no Sankhya"
                continue
            
            log_pedido.criar(log_id=log_id,
                             id_loja=0,
                             id_pedido=0,
                             pedido_ecommerce='',
                             nunota_pedido=pedido.get('nunota'),
                             evento=evento,
                             status=True,
                             obs=f"Pedido {pedido.get('nunota')} confirmado com sucesso!")
            venda.atualizar_confirmada(nunota_pedido=pedido.get('nunota'))
            print(f"Pedido {pedido.get('nunota')} confirmado com sucesso!")

        status_log = False if obs else True
        log.atualizar(id=log_id, sucesso=status_log)
        print(f"-> Processo de confirmação de pedidos concluído! Status do log: {status_log}")
        return True

    async def conferir(self):
        obs = None
        conferencia = ConferenciaSnk()
        print("Busca pedidos para conferir")
        lista_pedidos = await conferencia.buscar_aguardando_conferencia()
        if not lista_pedidos:
            print("Nenhum pedido para conferir.")
            return True

        print(f"{len(lista_pedidos)} pedidos encontrados")
        nota_olist = NotaOlist()
        parser_conferencia = ParserConferencia()

        for i, pedido in enumerate(lista_pedidos):
            time.sleep(self.req_time_sleep)
            print("")
            print(f"Pedido {i+1}/{len(lista_pedidos)}: {pedido.get('nunota')}")

            print("Busca a nota")
            dados_nota = await nota_olist.buscar_legado(id_ecommerce=pedido.get('ad_mkp_codped'))
            if not dados_nota:
                obs = "Erro ao buscar nota"
                print(obs)             
                continue

            print("Gerando conferência do pedido")            
            if not await conferencia.criar(nunota=pedido.get('nunota')):
                obs = "Erro ao criar conferência do pedido no Sankhya"
                print(obs)
                continue

            # Vincula a conferencia ao pedido
            print("Vincula a conferencia ao pedido")
            if not await conferencia.vincular_pedido(nunota=pedido.get('nunota'), nuconf=conferencia.nuconf):
                obs = "Erro ao vincular conferência ao pedido no Sankhya"
                print(obs)
                continue

            # Informa os itens na conferência
            print("Informa os itens na conferência")
            itens_para_conferencia = parser_conferencia.to_sankhya_itens(nuconf=conferencia.nuconf, dados_olist=dados_nota.get('itens'))
            if not itens_para_conferencia:
                obs = "Erro ao converter itens da nota para o formato da API do Sankhya"
                print(obs)
                continue

            ack_insercao_itens = await conferencia.insere_itens(dados_item=itens_para_conferencia)
            if not ack_insercao_itens:
                obs = "Erro ao inserir itens na conferência no Sankhya"
                print(obs)
                continue

            print("Itens inseridos na conferência")
            
            # Conclui a conferência do pedido
            print("Conclui a conferência do pedido")
            if not await conferencia.concluir(nuconf=conferencia.nuconf):
                obs = "Erro ao concluir conferência do pedido no Sankhya"
                print(obs)
                continue

            print(f"Conferência do pedido {pedido.get('nunota')} concluída!")

        status_log = False if obs else True
        return status_log

    async def faturar_legado(self):

        from src.sankhya.nota import Nota as NotaSnk

        # Busca os pedidos pendentes de faturamento
        pedidos_faturar = venda.buscar_faturar()
        if not pedidos_faturar:
            print("Nenhum pedido para faturamento.")
            return True

        print(f"Pedidos para faturamento: {len(pedidos_faturar)}")

        snk = PedidoSnk()
        nota = NotaSnk()
        first = True
        try:
            for i, pedido in enumerate(pedidos_faturar):
                if not first:
                    time.sleep(self.req_time_sleep)  # Evita rate limit
                first = False

                print("")
                print(f"Faturando pedido {i + 1}/{len(pedidos_faturar)}: {pedido.num_pedido}/{pedido.nunota_pedido}")

                # Verifica se o pedido já foi faturado e só não foi atualizado na base do integrador
                validacao = await nota.buscar(codpedido=pedido.cod_pedido)
                if validacao:
                    print(f"Pedido {pedido.num_pedido} já foi faturado.")
                    venda.atualizar_faturada(nunota_pedido=pedido.nunota_pedido,
                                             nunota_nota=int(validacao.get('nunota')),
                                             dh_faturado=validacao.get('dtneg'))
                    continue

                ack, nunota_nota = await snk.faturar(nunota=pedido.nunota_pedido)
                if not ack:
                    print(f"Erro ao faturar pedido {pedido.nunota_pedido}")
                    logger.error("Erro ao faturar pedido %s",pedido.nunota_pedido)
                    continue

                venda.atualizar_faturada(nunota_pedido=pedido.nunota_pedido,
                                         nunota_nota=nunota_nota)
                
                print(f"Pedido {pedido.nunota_pedido} faturado com sucesso!")

            print("-> Processo de faturamento concluído!")
            return True
        except:
            return False

    async def buscar_lote(self, id_loja:int=None):
        # Busca novos pedidos
        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO+'_buscar_lote')
        # novos_pedidos = venda.buscar_importar()
        novos_pedidos = venda.buscar_importar_por_loja(id_loja=id_loja)
        if not novos_pedidos:
            print("Nenhum novo pedido encontrado.")
            log.atualizar(id=log_id)
            return True
        
        print(f"Novos pedidos encontrados: {len(novos_pedidos)}")
        novos_pedidos = [{'id': pedido.id_pedido, 'numero': pedido.num_pedido, 'loja': pedido.id_loja, 'codigo': pedido.cod_pedido, 'dados': pedido.dados_pedido} for pedido in novos_pedidos]        

        # Inicia as classes de integração
        olist = PedidoOlist()
        first = True
        lote_pedidos = []
        obs = None
        for index, pedido in enumerate(novos_pedidos):

            # if not first:
            #     time.sleep(self.req_time_sleep)  # Evita rate limit
            # first = False

            if obs:
                # Cria um log de erro se houver observação
                print(obs)
                logger.error(obs)
                log_pedido.criar(log_id=log_id,
                                 id_loja=pedido.get('loja'),
                                 id_pedido=pedido.get('id'),
                                 pedido_ecommerce=pedido.get('codigo'),
                                 status=False,
                                 obs=obs)                
                obs = None
            
            print(f"Buscando pedido {index + 1}/{len(novos_pedidos)}: {pedido.get('numero')}")
            
            # Busca os dados do pedido no Olist
            dados_pedido_olist = pedido.get('dados')
            # dados_pedido_olist = await olist.buscar(id=pedido.get('id'))
            if not dados_pedido_olist:
                #obs = "Erro ao buscar dados do pedido no Olist ou dados incompletos"
                obs = "Erro ao buscar dados do pedido"
                continue

            # Valida itens e desmembra kits
            itens_pedido_original = dados_pedido_olist.get('itens')
            itens_pedido_validado = []
            for item in itens_pedido_original:
                codprod = item['produto'].get('sku')
                if len(codprod) > 8:
                    codprod = None   
                                     
                if codprod:
                    itens_pedido_validado.append(item)
                else:
                    ack, kit_desmembrado = await olist.validar_kit(id=item['produto'].get('id'),item_no_pedido=item)
                    time.sleep(self.req_time_sleep)  # Evita rate limit
                    if ack:
                        itens_pedido_validado+=kit_desmembrado
                    else:
                        ack = kit_desmembrado = None

            if not itens_pedido_validado:
                obs = "Erro ao validar itens/desmembrar kits"
                continue
            
            dados_pedido_olist['itens'] = itens_pedido_validado
            lote_pedidos.append(dados_pedido_olist)
        
            log_pedido.criar(log_id=log_id,
                             id_loja=pedido.get('loja'),
                             id_pedido=pedido.get('id'),
                             pedido_ecommerce=pedido.get('codigo'),
                             status=True)
            
        status_log = False if log_pedido.buscar_status_false(log_id=log_id) else True
        log.atualizar(id=log_id, sucesso=status_log)
        print(f"-> Processo de busca de pedidos concluído! Status do log: {status_log}")
        return lote_pedidos
    
    def unificar(self, lista_pedidos:list[dict]) -> tuple[int, list,list]:

        if not lista_pedidos:
            print("Lista de pedidos não informada.")
            return True
        
        pedidos = []
        itens = []
        
        for pedido in lista_pedidos:
            ack_itens = True
            itens_pedido = pedido.get('itens')
            for item_pedido in itens_pedido:
                if not ack_itens:
                    continue

                try:
                    codprod = re.search(r'^\d{8}', item_pedido['produto'].get('sku'))
                    codprod = codprod.group()
                except Exception as e:
                    logger.error("Código do produto inválido: %s", item_pedido['produto'].get('sku'))
                    print(f"Código do produto inválido: {item_pedido['produto'].get('sku')}")
                    ack_itens = False
                    continue

                valor = {
                    'codprod': item_pedido['produto'].get('sku'),
                    'qtdneg': item_pedido.get('quantidade'),
                    'vlrunit': item_pedido.get('valorUnitario')
                }

                aux = None
                for item in itens:
                    if (valor.get('codprod') == item.get('codprod')) and (valor.get('vlrunit') == item.get('vlrunit')):
                        aux = item
                        break
                
                if not aux:
                    itens.append(valor)
                    continue

                aux['qtdneg']+=valor.get('qtdneg')
            
            if ack_itens:
                pedidos.append({
                    "numero":pedido.get('numeroPedido'),
                    "origem":pedido['ecommerce'].get('id'),
                    "codigo":pedido['ecommerce'].get('numeroPedidoEcommerce')
                })

        return pedidos, itens

    async def atualizar_nunota_lote(self, lista_pedidos:list, nunota:int):
        olist = PedidoOlist()
        for pedido in lista_pedidos:
            time.sleep(self.req_time_sleep)  # Evita rate limit            
            dados_pedido_olist = await olist.buscar(id=pedido.get('id'))
            if not dados_pedido_olist:
                continue
            ack = await olist.atualizar_nunota(id=pedido.get('id'), nunota=nunota,
                                               observacao=dados_pedido_olist.get('observacoes'))
            if not ack:
                print(f"Erro ao enviar nunota para o pedido {pedido.get('numeroPedido')} no Olist")
                logger.error("Erro ao enviar nunota para o pedido %s no Olist", pedido.get('numeroPedido'))
                continue        
        return True

    async def importar_lote(self):
        """ Importa novos pedidos do Olist para o Sankhya. """

        def atualizar_historico(lista_pedidos:list, nunota:int):
            for pedido in lista_pedidos:
                venda.atualizar_importada(id_pedido=pedido.get('id'),nunota_pedido=nunota)
            return True

        async def importar(log_id,pedidos_lote):
            
            # Unifica os pedidos
            print("Unificando os pedidos...")
            dados_pedidos, dados_itens = self.unificar(pedidos_lote)

            # Converte para o formato da API do Sankhya
            parser = ParserPedido()
            print("Convertendo para o formato da API do Sankhya...")        
            cabecalho, itens, id_origem = parser.to_sankhya_lote(lista_pedidos=dados_pedidos,lista_itens=dados_itens)

            if not cabecalho and not itens:
                print("Erro ao converter dados do pedido para o formato da API do Sankhya")
                logger.error("Erro ao converter dados do pedido para o formato da API do Sankhya")
                log_pedido.criar(log_id=log_id,
                                id_loja=id_origem,
                                id_pedido=0,
                                pedido_ecommerce='varios',
                                status=False,
                                obs="Erro ao converter dados do pedido para o formato da API do Sankhya")            
                return False

            # Insere os dados do pedido
            snk = PedidoSnk()
            print("Inserindo os dados no Sankhya...")
            pedido_incluido = await snk.lancar(dados_cabecalho=cabecalho,dados_itens=itens)
            if pedido_incluido == 0:
                print("Erro ao inserir pedido no Sankhya.")
                logger.error("Erro ao inserir pedido no Sankhya.")
                log_pedido.criar(log_id=log_id,
                                id_loja=id_origem,
                                id_pedido=0,
                                pedido_ecommerce='varios',
                                status=False,
                                obs="Erro ao inserir pedido no Sankhya")                
                return False
            
            atualizar_historico(lista_pedidos=pedidos_lote,nunota=pedido_incluido)
            
            # Envia número único do pedido pro Olist
            print("Enviando número único para os pedidos no Olist...")
            ack = await self.atualizar_nunota_lote(lista_pedidos=pedidos_lote,nunota=pedido_incluido)
            if not ack:
                print("Erro ao enviar número único para os pedidos no Olist.")
                logger.error("Erro ao enviar número único para os pedidos no Olist.")
                log_pedido.criar(log_id=log_id,
                                id_loja=id_origem,
                                id_pedido=0,
                                pedido_ecommerce='varios',
                                nunota_pedido=pedido_incluido,
                                status=False,
                                obs="Erro ao enviar número único para os pedidos no Olist")
                return False
            
            print(f"Pedidos importados no código {pedido_incluido}")
            log_pedido.criar(log_id=log_id,
                             id_loja=id_origem,
                             id_pedido=0,
                             pedido_ecommerce='varios',
                             nunota_pedido=pedido_incluido)
            
            return True        
        
        # Busca pedidos
        print("Buscando pedidos em lote Shopee...")
        pedidos_lote_shopee = await self.buscar_lote(id_loja=9227)
        if not pedidos_lote_shopee:
            msg = "Erro ao buscar pedidos da shopee"
            logger.error(msg)
            print(msg)
        print("Buscando pedidos em lote Blz na web...")
        pedidos_lote_blz = await self.buscar_lote(id_loja=10940)
        if not pedidos_lote_blz:
            msg = "Erro ao buscar pedidos da blz na web"
            logger.error(msg)
            print(msg)        

        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO+'_importar_lote')
        if not pedidos_lote_shopee and not pedidos_lote_blz:
            log.atualizar(id=log_id,sucesso=False)
            return False
        
        if pedidos_lote_shopee:
            ack_shopee = await importar(log_id=log_id,pedidos_lote=pedidos_lote_shopee)
        else:
            ack_shopee = True
        
        if pedidos_lote_blz:
            ack_blz = await importar(log_id=log_id,pedidos_lote=pedidos_lote_blz)
        else:
            ack_blz = True

        log.atualizar(id=log_id, sucesso=all([ack_shopee,ack_blz]))
        print("")
        print(f"-> PROCESSO DE IMPORTAÇÃO DE PEDIDOS CONCLUÍDO!")
        return True

    async def confirmar_lote(self):
        """ Confirma pedidos importados do Olist no Sankhya. """
        
        # Busca pedidos pendentes de confirmação
        pedidos_confirmar = venda.buscar_confirmar()
        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO+'_confirmar_lote')
        if not pedidos_confirmar:
            print("Nenhum pedido para confirmar.")
            log.atualizar(id=log_id)
            return True

        id_loja = list(set([p.id_loja for p in pedidos_confirmar]))
        pedidos_confirmar = list(set([p.nunota_pedido for p in pedidos_confirmar]))
        
        print(f"Pedidos para confirmar: {len(pedidos_confirmar)}")
        
        snk = PedidoSnk()
        first = True
        obs = None
        evento = 'C'
        for pedido in pedidos_confirmar:
            if not first:
                time.sleep(self.req_time_sleep)  # Evita rate limit
            first = False

            if obs:
                # Cria um log de erro se houver observação
                print(obs)
                log_pedido.criar(log_id=log_id,
                                 id_loja=id_loja[0],
                                 id_pedido=0,
                                 pedido_ecommerce='',
                                 nunota_pedido=pedido,
                                 evento=evento,
                                 status=False,
                                 obs=obs)
                obs = None
            
            print("")
            print(f"Confirmando pedido {pedido}")

            # Verifica se o pedido já foi confirmado e só não foi atualizado na base do integrador
            validacao = await snk.buscar(nunota=pedido)
            if validacao.get('statusnota') == 'L':
                print(f"Pedido {pedido} já foi confirmado.")
                venda.atualizar_confirmada_lote(nunota_pedido=pedido,
                                                dh_confirmado=validacao.get('dtmov'))
                log_pedido.criar(log_id=log_id,
                                 id_loja=id_loja[0],
                                 id_pedido=0,
                                 pedido_ecommerce='',
                                 nunota_pedido=pedido,
                                 evento=evento,
                                 obs=f"Pedido {pedido} já foi confirmado")
                continue

            ack_confirmacao = await snk.confirmar(nunota=pedido)
            if not ack_confirmacao:
                obs = f"Erro ao confirmar pedido {pedido} no Sankhya"
                continue
            
            log_pedido.criar(log_id=log_id,
                             id_loja=id_loja[0],
                             id_pedido=0,
                             pedido_ecommerce='varios',
                             nunota_pedido=pedido,
                             evento=evento)
            venda.atualizar_confirmada_lote(nunota_pedido=pedido)
            print(f"Pedido {pedido} confirmado com sucesso!")

        status_log = False if obs else True
        log.atualizar(id=log_id, sucesso=status_log)
        print(f"-> Processo de confirmação de pedidos concluído!")
        return True

    async def devolver_lote(self):
        """ Emite devolução no Sankhya dos pedidos cancelados após emissão da nota fiscal. """

        def extrair_devolucoes(lista_pedidos:list) -> tuple[list,list]:
            itens = []
            try:
                for pedido in lista_pedidos:
                    # Extrai a lista de itens
                    for item in pedido.get('itens'):
                        for it in itens:
                            if it.get('sku') == item['produto'].get('sku'):
                                it['quantidade'] = it.get('quantidade')+item.get('quantidade')
                                continue
                        itens.append({
                            "sku":item['produto'].get('sku'),
                            "quantidade":item.get('quantidade')
                        })
            except Exception as e:
                logger.error("Erro: %s",e)
                print(f"Erro: {e}")
            finally:
                return itens
        
        def filtrar_pedidos_devolver(nunota:int,lista_pedidos_cancelados:list) -> list:
            pedidos_filtrados = []
            for pedido in lista_pedidos_cancelados:
                if pedido.nunota_pedido == nunota:
                    pedidos_filtrados.append(pedido)            
            return pedidos_filtrados

        # Busca pedidos cancelados depois de já terem sido faturados
        print("Buscando pedidos cancelados após faturamento...")
        lista_pedidos_cancelados = venda.buscar_importadas_cancelar()
        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO+'_devolver_lote')
        if not lista_pedidos_cancelados:
            print("Sem devoluções pendentes")
            log.atualizar(id=log_id)
            print("PROCESSO DE DEVOLUÇÃO CONCLUÍDO!")            
            return True  
              
        pedidos_pendente_cancelar_snk = list(set([p.nunota_pedido for p in lista_pedidos_cancelados]))
        print(f"Pedidos pendentes de cancelamento no Sankhya: {len(pedidos_pendente_cancelar_snk)}")

        olist = PedidoOlist()
        snk = PedidoSnk()
        nota = NotaSnk()
        parser = ParserPedido()
        obs:str=None
        status:bool=None
        obs_olist:str=None
        status_olist:bool=None 

        for i, pedido_snk in enumerate(pedidos_pendente_cancelar_snk):

            if obs:
                # Cria um log de erro se houver observação
                print(obs)
                if status:
                    logger.warning(obs)
                else:
                    logger.error(obs)                
                log_pedido.criar(log_id=log_id,
                                 id_loja=0,
                                 id_pedido=0,
                                 pedido_ecommerce='',
                                 nunota_pedido=pedido_snk,
                                 evento='F',
                                 status=status,
                                 obs=obs)
                obs = None
                status = None
            
            print("")
            print(f"Pedido {i+1}/{len(pedidos_pendente_cancelar_snk)}: {pedido_snk}")
            pedidos = []
            itens_olist = []
            dados_pedidos_cancelar = []

            # Busca dados do faturamento do pedido lançado no Sankhya
            print("Buscando dados do faturamento do pedido lançado no Sankhya...")
            dados_snk = await snk.buscar_nota_do_pedido(nunota=pedido_snk)

            if dados_snk==0:
                obs = f"Pedido {pedido_snk} não encontrado no Sankhya"
                status = True
                continue
            
            if not dados_snk:
                obs = f"Erro ao buscar dados do faturamento do pedido {pedido_snk} no Sankhya"
                status = False
                continue            

            pedidos_olist = filtrar_pedidos_devolver(pedido_snk,lista_pedidos_cancelados)

            for j, pedido in enumerate(pedidos_olist):
                if obs_olist:
                    # Cria um log de erro se houver observação
                    print(obs_olist)
                    if status_olist:
                        logger.warning(obs_olist)
                    else:
                        logger.error(obs_olist)     

                    log_pedido.criar(log_id=log_id,
                                     id_loja=pedidos_olist[j-1].id_loja,
                                     id_pedido=pedidos_olist[j-1].id_pedido,
                                     pedido_ecommerce=pedidos_olist[j-1].pedido_ecommerce,
                                     nunota_pedido=pedidos_olist[j-1].nunota_pedido,
                                     evento='F',
                                     status=status_olist,
                                     obs=obs_olist)
                    obs_olist = None
                    status_olist = None

                time.sleep(self.req_time_sleep)  # Evita rate limit
                # Busca dados dos pedidos no Olist
                print("Buscando dados dos pedidos no Olist...")                
                dados_pedido_olist = await olist.buscar(id=pedido.id_pedido)
                if not dados_pedido_olist:
                    obs_olist = f"Erro ao buscar dados do pedido {pedido.num_pedido} no Olist"
                    status_olist = False
                    continue
                
                dados_pedidos_cancelar.append(dados_pedido_olist)

            itens_olist = extrair_devolucoes(dados_pedidos_cancelar)
            if not itens_olist:
                obs = f"Erro ao extrair dados da devolucao do pedido {pedido_snk}"
                status = False
                continue

            # Converte para o formato da API do Sankhya
            print("Convertendo dados para o formato da API do Sankhya...")
            dados_formatados = parser.to_sankhya_devolucao(dados_olist=itens_olist,
                                                           dados_sankhya=dados_snk.get('itens'))
            if not dados_formatados:
                obs = f"Erro ao converter dados da devolucao do pedido {pedido_snk} para o formato da API do Sankhya"
                status = False
                continue
        
            # Lança devolução
            print(f"Lançando devolução do pedido {pedido_snk}/nota {dados_snk.get('nunota')}...")
            ack, nunota_devolucao = await nota.devolver(nunota=dados_snk.get('nunota'),
                                                        itens=dados_formatados)
            if not ack:
                obs = f"Erro ao lançar devolução do pedido {pedido_snk}/nota {dados_snk.get('nunota')}"
                status = False
                continue
            
            # Informa observação
            print(f"Atualizando campo da observação...")
            observacao = 'Devolução de ecommerce referente ao(s) pedido(s):\n'+',\n'.join(list(set([str(p.num_pedido) for p in pedidos_olist])))
            ack = await nota.alterar_observacao(nunota=nunota_devolucao,
                                                observacao=observacao)
            if not ack:
                obs_olist = f"Erro ao atualizar observação da nota {nunota_devolucao}"
                status_olist = False
                continue                       
            
            # Confirma a devolução
            print("Confirmando devolução...")
            ack = await nota.confirmar(nunota=nunota_devolucao)
            if not ack:
                obs_olist = f"Erro ao confirmar devolução {nunota_devolucao}"
                status_olist = False
                continue  

            # Registra log que os pedidos foram devolvidos
            for pedido in pedidos_olist:
                time.sleep(self.req_time_sleep)  # Evita rate limit
                dados_olist = await olist.buscar(id=pedido.id_pedido)
                if dados_olist.get('situacao') == 2:
                    venda.atualizar_devolvido(id_pedido=pedido.id_pedido)
                else:
                    venda.resetar_pedido(id_pedido=pedido.id_pedido)
                log_pedido.criar(log_id=log_id,
                                 id_loja=pedido.id_loja,
                                 id_pedido=pedido.id_pedido,
                                 pedido_ecommerce=pedido.cod_pedido,
                                 nunota_pedido=pedido.nunota_pedido,
                                 evento='F')
                    
        status_log = False if log_pedido.buscar_status_false(log_id=log_id) else True
        log.atualizar(id=log_id, sucesso=status_log)
        print("================================")
        print("PROCESSO DE DEVOLUÇÃO CONCLUÍDO!")

    async def anular(self, nunota:int):
        """ Exclui pedido que ainda não foi conferido do Sankhya. """

        snk = PedidoSnk()
        olist = PedidoOlist()

        # Validando pedido no Sankhya
        print("Validando pedido no Sankhya...")
        dados_snk = await snk.buscar_nota_do_pedido(nunota=nunota)
        log_id = log.criar(de='olist', para='sankhya', contexto=CONTEXTO+'_anular')
        if not dados_snk:
            obs = f"Pedido {nunota} não encontrado no Sankhya"
            print(obs)
            logger.error(obs)
            log_pedido.criar(log_id=log_id,
                             id_loja=0,
                             id_pedido=0,
                             pedido_ecommerce=0,
                             nunota_pedido=nunota,
                             evento='F',
                             obs=obs,
                             status=False)            
            log.atualizar(id=log_id,sucesso=False)
            return False
        
        if isinstance(dados_snk,dict):
            obs = f"Pedido já foi faturado e não pode ser excluído"
            print(obs)
            logger.error(obs)
            log_pedido.criar(log_id=log_id,
                             id_loja=0,
                             id_pedido=0,
                             pedido_ecommerce=0,
                             nunota_pedido=nunota,
                             evento='F',
                             obs=obs,
                             status=False)            
            log.atualizar(id=log_id,sucesso=False)
            return False

        # Exclui pedido no Sankhya
        print(f"Excluindo pedido {nunota} no Sankhya...")
        ack = await snk.excluir(nunota=nunota)
        if not ack:
            obs = f"Erro ao excluir pedido {nunota} no Sankhya"
            print(obs)
            logger.error(obs)
            log_pedido.criar(log_id=log_id,
                             id_loja=0,
                             id_pedido=0,
                             pedido_ecommerce=0,
                             nunota_pedido=nunota,
                             evento='F',
                             obs=obs,
                             status=False)            
            log.atualizar(id=log_id,sucesso=False)            
            return False
        
        # Busca pedidos relacionados no Olist
        lista_pedidos = venda.buscar_nunota_pedido(nunota_pedido=nunota)
        if not lista_pedidos:
            obs = f"Erro ao buscar pedidos relacionados à nunota {nunota}"
            print(obs)
            logger.error(obs)
            log_pedido.criar(log_id=log_id,
                             id_loja=0,
                             id_pedido=0,
                             pedido_ecommerce=0,
                             nunota_pedido=nunota,
                             evento='F',
                             obs=obs,
                             status=False)            
            log.atualizar(id=log_id,sucesso=False)            
            return False

        print("Atualizando pedidos no Olist...")
        for i, pedido in enumerate(lista_pedidos):
            time.sleep(self.req_time_sleep)  # Evita rate limit
            if obs:
                print(obs)
                logger.error(obs)
                log_pedido.criar(log_id=log_id,
                                 id_loja=lista_pedidos[i-1].id_loja,
                                 id_pedido=lista_pedidos[i-1].id_pedido,
                                 pedido_ecommerce=lista_pedidos[i-1].cod_pedido,
                                 evento='F',
                                 obs=obs,
                                 status=False)
                obs = None

            ack = await olist.remover_nunota(id=pedido.id_pedido)
            if not ack:
                obs = f"Erro ao atualizar pedido {pedido} no Olist"                
                continue
            venda.atualizar_anulado(id_pedido=pedido.id_pedido)

            log_pedido.criar(log_id=log_id,
                             id_loja=pedido.id_loja,
                             id_pedido=pedido.id_pedido,
                             pedido_ecommerce=pedido.cod_pedido,
                             evento='F')

        print("================================")
        print("PROCESSO DE ANULACAO DE PEDIDO CONCLUIDO!")            
        status_log = False if log_pedido.buscar_status_false(log_id=log_id) else True
        log.atualizar(id=log_id, sucesso=status_log)
        return True