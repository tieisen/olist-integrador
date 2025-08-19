import os
import time
import logging
from dotenv import load_dotenv
from src.olist.pedido import Pedido as PedidoOlist
from src.olist.nota import Nota as NotaOlist
from src.sankhya.pedido import Pedido as PedidoSnk
from src.parser.pedido import Pedido as ParserPedido
from src.sankhya.conferencia import Conferencia as ConferenciaSnk
from src.parser.conferencia import Conferencia as ParserConferencia
from src.services.viacep import Viacep
from database.schemas import log as SchemaLog
from database.schemas import log_pedido as SchemaLogPedido
from database.crud import venda, log, log_pedido
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
        """ Inicializa a classe Pedido com a conexão ao Olist e o endpoint de pedidos. """
        self.contexto = 'pedido'
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))
        # Cria um log para o processo
        self.log_id = log.create(log=SchemaLog.LogBase(de='olist', para='sankhya', contexto=self.contexto))        

    def validar_existentes(self, lista_pedidos: list) -> list:
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
        
        existentes = venda.read_by_list_idpedido(lista_pedidos)
        existentes = [p.id_pedido for p in existentes if p.id_pedido in lista_pedidos]
        return [pedido for pedido in lista_pedidos if pedido not in existentes]

    async def validar_cancelamentos(self):

        print("Validando pedidos cancelados...")
        olist = PedidoOlist()

        cancelados = await olist.buscar(cancelados=True)

        if not cancelados:
            print("Nenhum pedido cancelado encontrado.")
            logger.info("Nenhum pedido cancelado encontrado.")
            return False

        print(f"Pedidos dos últimos 3 dias cancelados no Olist: {len(cancelados)}")

        pedidos_pendente_cancelar_integrador = venda.read_valida_cancelamentos(lista_ids=cancelados)
        if pedidos_pendente_cancelar_integrador:
            print(f"Pedidos pendentes de cancelamento no integrador: {len(pedidos_pendente_cancelar_integrador)}")
            for pedido in pedidos_pendente_cancelar_integrador:
                venda.update_pedido_cancelado_olist(id_pedido=pedido.id_pedido)
                time.sleep(self.req_time_sleep)  # Evita rate limit
            print("Pedidos atualizados no integrador com sucesso!")
        else:
            print("Nenhum pedido pendente de cancelamento encontrado.")
            logger.info("Nenhum pedido pendente de cancelamento encontrado.")

        pedidos_pendente_cancelar_snk = venda.read_valida_importados_cancelados()
        if pedidos_pendente_cancelar_snk:
            print(f"Pedidos pendentes de cancelamento no Sankhya: {len(pedidos_pendente_cancelar_snk)}")            
            snk = PedidoSnk()
            for pedido in pedidos_pendente_cancelar_snk:
                time.sleep(self.req_time_sleep)  # Evita rate limit
                dados_pedido_snk = await snk.buscar(nunota=pedido.nunota_pedido)
                if not dados_pedido_snk:
                    print("Pedido já foi excluido")
                else:
                    print(f"Cancelando pedido #{pedido.num_pedido} no Sankhya")
                    ack = await snk.cancelar(nunota=pedido.nunota_pedido,
                                             num_pedido=pedido.num_pedido)
                    if not ack:
                        print(f"Erro ao cancelar pedido #{pedido.num_pedido} no Sankhya")
                        logger.error("Erro ao cancelar pedido %s no Sankhya", pedido.num_pedido)                    
                    print(f"Pedido #{pedido.num_pedido} cancelado no Sankhya")
                    logger.info("Pedido %s cancelado no Sankhya", pedido.num_pedido)
                venda.update_pedido_cancelado_snk(id_pedido=pedido.id_pedido)
            print("Pedidos atualizados no Sankhya com sucesso!")
        else:
            print("Nenhum pedido pendente de cancelamento encontrado.")
            logger.info("Nenhum pedido pendente de cancelamento encontrado.")            

        print("Validação de pedidos cancelados concluída!")
        
        return True

    async def receber(self, lista_pedidos:list=None, atual:bool=True) -> bool:
        """ Recebe pedidos do Olist e os adiciona à base de dados.
        Args:
            lista_pedidos (list, optional): Lista de IDs de pedidos a serem recebidos. Se None, busca todos os pedidos novos.            
        Returns:
            bool: True se os pedidos foram recebidos com sucesso, False caso contrário.
        """

        print("Recebendo pedidos...")

        ped_olist = PedidoOlist()
        if not lista_pedidos:
            ack, lista = await ped_olist.buscar_novos(atual=atual)            
            if not ack:
                print("Nenhum pedido encontrado.")
                logger.info("Nenhum pedido encontrado.")
                return False
            print(f"Pedidos encontrados: {len(lista)}")
            lista_pedidos = self.validar_existentes(lista)
            
            if not lista_pedidos:
                print("Todos os pedidos já existem na base de dados.")
                logger.info("Todos os pedidos já existem na base de dados.")
                return True
        
        print(f"Pedidos a serem recebidos: {len(lista_pedidos)}")
        first = True
        db_con = venda.open()
        for pedido in lista_pedidos:
            if not first:
                time.sleep(self.req_time_sleep)  # Evita rate limit
            first = False

            dados_pedido = await ped_olist.buscar(id=pedido)
            if not dados_pedido:
                print(f"Erro ao buscar dados do pedido {pedido} no Olist")
                logger.error("Erro ao buscar dados do pedido %s no Olist",pedido)
                continue

            if dados_pedido.get('situacao') == 8:
                print(f"Pedido {dados_pedido.get('numeroPedido')} dados incompletos")
                logger.warning("Pedido %s dados incompletos", dados_pedido.get('numeroPedido'))
                continue

            ack = venda.create(db=db_con,
                               id_loja=dados_pedido['ecommerce'].get('id'),
                               id_pedido=dados_pedido.get('id'),
                               cod_pedido=dados_pedido['ecommerce'].get('numeroPedidoEcommerce'),
                               num_pedido=dados_pedido.get('numeroPedido'))
            if not ack:
                print(f"Erro ao adicionar pedido {dados_pedido.get('numeroPedido')} à base de dados.")
                logger.error("Erro ao adicionar pedido %s à base de dados.", dados_pedido.get('numeroPedido'))
                continue

            print(f"Pedido {dados_pedido.get('numeroPedido')} adicionado à base de dados.")
        
        db_con.close()
        print("Recebimento de pedidos concluído!")

        return True    

    async def importar(self):
        """ Importa novos pedidos do Olist para o Sankhya. """
        obs = None
        evento = 'I'

        # Busca novos pedidos
        novos_pedidos = venda.read_new_venda_to_snk()
        if not novos_pedidos:
            print("Nenhum novo pedido encontrado.")
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
                log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=self.log_id,
                                                                    id_loja=dados_pedido_olist['ecommerce'].get('id'),
                                                                    id_pedido=pedido.get('id'),
                                                                    pedido_ecommerce=dados_pedido_olist['ecommerce'].get('numeroPedidoEcommerce'),
                                                                    nunota_pedido=0,
                                                                    evento=evento,
                                                                    status=False,
                                                                    obs=obs))

            print("")
            print(f"Importando pedido {index + 1}/{len(novos_pedidos)}: {pedido.get('numero')}")
            # Verifica se o pedido já existe no Olist e só não foi atualizado na base do integrador
            validacao = await snk.buscar(id_olist=pedido.get('id'))
            if validacao:
                print(f"Pedido {pedido.get('numero')} já existe na base de dados.")
                venda.update_new_venda_to_snk(id_pedido=pedido.get('id'),
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
            for item in itens_pedido_original:
                if item['produto'].get('sku'):
                    itens_pedido_validado.append(item)
                else:
                    ack, kit_desmembrado = await olist.validar_kit(id=item['produto'].get('id'),item_no_pedido=item)
                    if ack:
                        itens_pedido_validado+=kit_desmembrado
            
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

            log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=self.log_id,
                                                                id_loja=dados_pedido_olist['ecommerce'].get('id'),
                                                                id_pedido=pedido.get('id'),
                                                                pedido_ecommerce=dados_pedido_olist['ecommerce'].get('numeroPedidoEcommerce'),
                                                                nunota_pedido=pedido_incluido,
                                                                evento=evento,
                                                                status=True))
            venda.update_new_venda_to_snk(id_pedido=pedido.get('id'),
                                          nunota_pedido=pedido_incluido)
            
            print(f"Pedido #{pedido.get('numero')} importado no código {pedido_incluido}")

            # Manda número do pedido pro Olist
            ack_observacao = await olist.atualizar_nunota(id=pedido.get('id'),
                                                          nunota=pedido_incluido,
                                                          observacao=dados_pedido_olist.get('observacoes'))
            
            if not ack_observacao:
                obs = "Erro ao enviar nunota do pedido para Olist"
                continue
                        
        status_log = False if log_pedido.read_by_logid_status_false(log_id=self.log_id) else True
        log.update(id=self.log_id, log=SchemaLog.LogBase(sucesso=status_log))
        print(f"-> Processo de importação de pedidos concluído! Status do log: {status_log}")
        return True

    async def confirmar(self):
        """ Confirma pedidos importados do Olist no Sankhya. """
        obs = None
        evento = 'C'
        
        # Busca pedidos pendentes de confirmação
        pedidos_confirmar = venda.read_venda_confirmar_snk()
        if not pedidos_confirmar:
            print("Nenhum pedido para confirmar.")
            return True

        print(f"Pedidos para confirmar: {len(pedidos_confirmar)}")
        pedidos_confirmar = [{'nunota': pedido.nunota_pedido, 'numero': pedido.num_pedido, 'id': pedido.id_pedido, 'separacao': pedido.id_separacao} for pedido in pedidos_confirmar]

        snk = PedidoSnk()
        first = True
        for index, pedido in enumerate(pedidos_confirmar):
            if not first:
                time.sleep(self.req_time_sleep)  # Evita rate limit
            first = False

            if obs:
                # Cria um log de erro se houver observação
                print(obs)
                log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=self.log_id,
                                                                    id_loja=0,
                                                                    id_pedido=0,
                                                                    pedido_ecommerce='',
                                                                    nunota_pedido=pedido.get('nunota'),
                                                                    evento=evento,
                                                                    status=False,
                                                                    obs=obs))
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
                venda.update_venda_confirmar_snk(nunota_pedido=pedido.get('nunota'),
                                                 dh_confirmado=validacao.get('dtmov'))                
                continue

            ack_confirmacao = await snk.confirmar(nunota=pedido.get('nunota'))
            if not ack_confirmacao:
                obs = f"Erro ao confirmar pedido {pedido.get('nunota')} no Sankhya"
                continue
            
            log_pedido.create(log=SchemaLogPedido.LogPedidoBase(log_id=self.log_id,
                                                                id_loja=0,
                                                                id_pedido=0,
                                                                pedido_ecommerce='',
                                                                nunota_pedido=pedido.get('nunota'),
                                                                evento=evento,
                                                                status=True,
                                                                obs=f"Pedido {pedido.get('nunota')} confirmado com sucesso!"))
            venda.update_venda_confirmar_snk(nunota_pedido=pedido.get('nunota'))
            print(f"Pedido {pedido.get('nunota')} confirmado com sucesso!")

        # separacao = SeparacaoOlist()
        # await separacao.receber_separacoes()

        status_log = False if obs else True
        log.update(id=self.log_id, log=SchemaLog.LogBase(sucesso=status_log))
        print(f"-> Processo de confirmação de pedidos concluído! Status do log: {status_log}")
        return True

    async def conferir(self):
        #log_id = log.create(log=SchemaLog.LogBase(de='olist', para='sankhya', contexto=CONTEXTO))
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
            #dados_nota = await nota_olist.buscar(id_ecommerce=pedido.get('ad_mkp_codped'))
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
        pedidos_faturar = venda.read_venda_faturar_snk()
        if not pedidos_faturar:
            print("Nenhum pedido para faturamento.")
            return True

        print(f"Pedidos para faturamento: {len(pedidos_faturar)}")

        snk = PedidoSnk()
        olist = PedidoOlist()
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
                    venda.update_venda_fatura_snk(nunota_pedido=pedido.nunota_pedido,
                                                nunota_nota=int(validacao.get('nunota')),
                                                dh_faturado=validacao.get('dtneg'))
                    continue

                ack, nunota_nota = await snk.faturar(nunota=pedido.nunota_pedido)
                if not ack:
                    print(f"Erro ao faturar pedido {pedido.nunota_pedido}")
                    logger.error("Erro ao faturar pedido %s",pedido.nunota_pedido)
                    continue

                venda.update_venda_fatura_snk(nunota_pedido=pedido.nunota_pedido,
                                              nunota_nota=nunota_nota)
                
                print(f"Pedido {pedido.nunota_pedido} faturado com sucesso!")

            print("-> Processo de faturamento concluído!")
            return True
        except:
            return False
