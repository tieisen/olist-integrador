import os
import time
import logging
from dotenv import load_dotenv
from src.sankhya.faturamento import Faturamento as FaturamentoSnk
from src.sankhya.estoque import Estoque as EstoqueSnk
from src.sankhya.transferencia import Transferencia as TransferenciaSnk
from src.sankhya.transferencia import Itens as ItemTransfSnk
from src.sankhya.pedido import Pedido as PedidoSnk
from src.sankhya.nota import Nota as NotaSnk
from src.parser.transferencia import Transferencia as ParserTransferencia
from src.olist.pedido import Pedido as PedidoOlist
from src.olist.nota import Nota as NotaOlist
from src.olist.separacao import Separacao as SeparacaoOlist
from database.crud import venda
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

CONTEXTO = 'faturamento'

class Faturamento:

    def __init__(self):
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))

    async def venda_entre_empresas_em_lote(self):

        faturamento = FaturamentoSnk()
        estoque = EstoqueSnk()
        transferencia = TransferenciaSnk()
        item_transf = ItemTransfSnk()
        parser = ParserTransferencia()

        saldo_pedidos = await faturamento.buscar_itens()
        if not saldo_pedidos:
            logger.error("Erro ao buscar itens conferidos no dia.")
            print("Erro ao buscar itens conferidos no dia.")
            return False

        saldo_estoque = await estoque.buscar_saldo_por_lote(lista_produtos=saldo_pedidos)
        if not saldo_estoque:
            logger.error("Erro ao buscar saldo de estoque.")
            print("Erro ao buscar saldo de estoque.")
            return False

        itens_venda_interna = await faturamento.compara_saldos(saldo_estoque=saldo_estoque,
                                                               saldo_pedidos=saldo_pedidos)

        if not itens_venda_interna:
            logger.info("Nenhum item para lançar venda interna.")
            print("Nenhum item para lançar venda interna.")
            return True

        codigos_produtos = [item.get('codprod') for item in itens_venda_interna]
        valores_produtos = await item_transf.busca_valor_transferencia(lista_itens=codigos_produtos)
        if not valores_produtos:
            logger.error("Erro ao buscar valores de transferência.")
            print("Erro ao buscar valores de transferência.")
            return False

        for item in itens_venda_interna:
            for valor in valores_produtos:
                if item.get('codprod') == valor.get('codprod'):
                    item['valor'] = float(valor.get('valor')) if valor.get('valor') else 0.1
                    break

        cabecalho, itens = parser.to_sankhya(objeto='nota',
                                             itens_transferencia=itens_venda_interna)
        
        #return cabecalho, itens
        if not all([cabecalho, itens]):
            logger.error("Erro ao preparar dados da nota de transferência.")
            print("Erro ao preparar dados da nota de transferência.")
            return False
        
        ack, nunota = await transferencia.criar(cabecalho=cabecalho,
                                                itens=itens)
        
        if not ack:
            logger.error("Erro ao lançar nota de transferência.")
            print("Erro ao lançar nota de transferência.")
            return False
        
        print("Nota de venda entre empresas lançada com sucesso.")
        logger.info("Nota de venda entre empresas lançada com sucesso.")
        
        ack = await transferencia.confirmar(nunota=nunota)
        if not ack:
            logger.error("Erro ao confirmar nota de transferência.")
            print("Erro ao confirmar nota de transferência.")
            return False
        
        print("Nota de venda entre empresas confirmada com sucesso.")
        logger.info("Nota de venda entre empresas confirmada com sucesso.")        
        
        return True

    async def venda_entre_empresas_por_item(self, nunota:int=None):

        faturamento = FaturamentoSnk()
        estoque = EstoqueSnk()
        transferencia = TransferenciaSnk()
        item_transf = ItemTransfSnk()
        parser = ParserTransferencia()

        saldo_pedidos = await faturamento.buscar_itens(nunota=nunota)
        if not saldo_pedidos:
            logger.error("Erro ao buscar itens conferidos no dia.")
            print("Erro ao buscar itens conferidos no dia.")
            return False

        saldo_estoque = await estoque.buscar_saldo_por_lote(lista_produtos=saldo_pedidos)
        if not saldo_estoque:
            logger.error("Erro ao buscar saldo de estoque.")
            print("Erro ao buscar saldo de estoque.")
            return False

        itens_venda_interna = await faturamento.compara_saldos(saldo_estoque=saldo_estoque,
                                                               saldo_pedidos=saldo_pedidos)
        if not itens_venda_interna:
            logger.info("Nenhum item para lançar venda interna.")
            print("Nenhum item para lançar venda interna.")
            return True

        codigos_produtos = [item.get('codprod') for item in itens_venda_interna]
        valores_produtos = await item_transf.busca_valor_transferencia(lista_itens=codigos_produtos)
        if not valores_produtos:
            logger.error("Erro ao buscar valores de transferência.")
            print("Erro ao buscar valores de transferência.")
            return False

        for item in itens_venda_interna:
            for valor in valores_produtos:
                if item.get('codprod') == valor.get('codprod'):
                    item['valor'] = float(valor.get('valor')) if valor.get('valor') else 0.1
                    break

        ack_nota_transferencia, dados_nota_transferencia = await transferencia.buscar(itens=True)
        if not ack_nota_transferencia:
            logger.error("Erro ao buscar dados da nota de transferência.")
            print("Erro ao buscar dados da nota de transferência.")
            return
        if not dados_nota_transferencia:
            ack_criacao_transferencia, nunota_transferencia = await transferencia.criar(cabecalho=parser.to_sankhya(objeto='cabecalho'))
            if not ack_criacao_transferencia:
                logger.error("Erro ao criar nota de transferência.")
                print("Erro ao criar nota de transferência.")
                return False
            
        if ack_criacao_transferencia:            
            itens = parser.to_sankhya(objeto='item',
                                      nunota=nunota_transferencia,
                                      itens_transferencia=itens_venda_interna)
        elif dados_nota_transferencia:
            itens = parser.to_sankhya(objeto='item',
                                      nunota=dados_nota_transferencia.get('nunota'),
                                      itens_transferencia=itens_venda_interna,
                                      itens_transferidos=dados_nota_transferencia.get('itens'))

        ack_item_transf = await item_transf.lancar(nunota=nunota_transferencia,
                                                   dados_item=itens)
        if not ack_item_transf:
            logger.error("Erro ao lançar item na nota de transferência.")
            print("Erro ao lançar item na nota de transferência.")
            return False
        
        return True
    
    async def faturar(self):        

        # Busca os pedidos pendentes de faturamento
        pedidos_faturar = venda.read_venda_faturar_snk()
        #pedidos_faturar = venda.read_faturar_olist()
        if not pedidos_faturar:
            print("Nenhum pedido para faturamento.")
            return True

        print(f"Pedidos para faturamento: {len(pedidos_faturar)}")

        pedido_snk = PedidoSnk()
        pedido_olist = PedidoOlist()
        nota_snk = NotaSnk()
        nota_olist = NotaOlist()
        separacao = SeparacaoOlist()
        first = True
        try:
            for i, pedido in enumerate(pedidos_faturar):
                if not first:
                    time.sleep(self.req_time_sleep)  # Evita rate limit
                first = False

                print("")
                print(f"Pedido {i + 1}/{len(pedidos_faturar)}: {pedido.num_pedido}")

                # Verifica se o pedido já foi faturado e só não foi atualizado na base do integrador
                validacao = await nota_snk.buscar(codpedido=pedido.cod_pedido)
                if validacao:
                    print(f"Pedido {pedido.num_pedido} já foi faturado.")
                    venda.update_venda_fatura_snk(nunota_pedido=pedido.nunota_pedido,
                                                  nunota_nota=int(validacao.get('nunota')),
                                                  dh_faturado=validacao.get('dtneg'))
                    continue

                # Fatura pedido no Sankhya
                print("Faturando pedido no Sankhya...")
                ack, nunota_nota = await pedido_snk.faturar(nunota=pedido.nunota_pedido)
                if not ack:
                    print(f"Erro ao faturar pedido {pedido.nunota_pedido}")
                    logger.error("Erro ao faturar pedido %s",pedido.nunota_pedido)
                    continue
                venda.update_venda_fatura_snk(nunota_pedido=pedido.nunota_pedido,
                                              nunota_nota=nunota_nota)
                
                # Fatura pedido no Olist
                print("Gerando NF no Olist...")
                dados_nota_olist = await pedido_olist.gerar_nf(id=pedido.id_pedido)
                if not dados_nota_olist:
                    print(f"Erro ao gerar NF do pedido {pedido.nunota_pedido}")
                    logger.error("Erro ao gerar NF do pedido %s",pedido.nunota_pedido)
                    continue
                venda.update_gera_nf_olist(cod_pedido=pedido.cod_pedido,
                                           num_nota=dados_nota_olist.get('numero'),
                                           id_nota=dados_nota_olist.get('id'))
                
                # Emite NF no Olist
                print(f"Autorizando NF {dados_nota_olist.get('numero')} pelo Olist...")                
                dados_emissao_nf_olist = await nota_olist.emitir(id=dados_nota_olist.get('id'))
                if not dados_emissao_nf_olist:
                    print(f"Erro ao emitir nota {dados_nota_olist.get('numero')} ref. pedido {pedido.cod_pedido}")
                    logger.error("Erro ao emitir nota %s ref. pedido %s",dados_nota_olist.get('numero'),pedido.cod_pedido)
                    continue
                venda.update_nota_autorizada(id_nota=dados_nota_olist.get('id'))                
                
                # Envia pedido pra separação no Olist
                print("Enviando pedido para separação no Olist...")
                id_separacao = venda.read_separacao_pedido(cod_pedido=pedido.cod_pedido)
                if not id_separacao:
                    print(f"Separação do pedido {pedido.num_pedido} não localizada na base")
                    logger.error("Separação do pedido %s não localizada na base",pedido.num_pedido)
                    continue
                ack_separacao = await separacao.separar(id=id_separacao)
                if not ack_separacao:
                    print(f"Erro ao separar pedido {pedido.num_pedido}.")
                    logger.error("Erro ao separar pedido %s.",pedido.num_pedido)
                    continue

                # Atualiza Nota no Sankhya
                print("Importando dados da NF para o Sankhya...")
                ack_importa_dados_nota = await nota_snk.informar_numero_e_chavenfe(nunota=nunota_nota,
                                                                                   chavenfe=dados_emissao_nf_olist.get('chaveAcesso'),
                                                                                   numero=dados_nota_olist.get('numero'),
                                                                                   id_nota=dados_emissao_nf_olist.get('id'))
                if not ack_importa_dados_nota:
                    print(f"Erro ao informar dados da nota {dados_nota_olist.get('numero')} na venda {nunota_nota} do Sankhya")
                    logger.error("Erro ao informar dados da nota %s na venda %s do Sankhya",dados_nota_olist.get('numero'),nunota_nota)
                    continue

                # # Confirma Nota no Sankhya
                # print("Confirmando Nota no Sankhya...")
                # ack_confirma_nota_snk = await nota_snk.confirmar(nunota=nunota_nota)
                # if not ack_confirma_nota_snk:
                #     dados_nota = await nota_snk.buscar(nunota=nunota_nota)
                #     if not dados_nota:
                #         print(f"Erro ao confirmar nota {nunota_nota} no Sankhya")
                #         logger.error("Erro ao confirmar nota %s no Sankhya",nunota_nota)
                #         continue
                #     else:
                #         print(f"Nota {nunota_nota} já foi confirmada no Sankhya")
                # venda.update_nota_confirma_snk(nunota_nota=nunota_nota)

                # Baixa contas a receber no Olist
                print("Baixando contas a receber no Olist...")
                dados_financeiro = await nota_olist.buscar_financeiro(serie=dados_nota_olist.get('serie'),
                                                                      numero=str(dados_nota_olist.get('numero')).zfill(6))
                if not dados_financeiro:
                    print(f"Erro ao buscar financeiro da nota no Olist")
                    logger.error("Erro ao buscar financeiro da nota %s no Olist",dados_nota_olist.get('numero'))
                    continue
                
                ack_financeiro = await nota_olist.baixar_financeiro(id=dados_financeiro.get('id'),
                                                                    valor=dados_financeiro.get('valor'))
                if ack_financeiro is None:
                    print(f"Financeiro da nota já está baixado no Olist")                    
                if ack_financeiro is False:
                    print(f"Erro ao baixar financeiro da nota no Olist")
                    logger.error("Erro ao baixar financeiro da nota %s no Olist",dados_nota_olist.get('numero'))
                    continue                
                venda.update_baixa_financeiro(num_nota=dados_nota_olist.get('numero'),
                                              id_financeiro=dados_financeiro.get('id'))                

                print(f"-> Faturamento do pedido {pedido.num_pedido} concluído!")    

            print("=====================================================")
            print("-> PROCESSO DE FATURAMENTO CONCLUÍDO!")
            return True
        except:
            return False
    
    async def consolidar(self):        

        # Busca as notas pendentes de confirmação
        notas_confirmar = venda.read_venda_faturada_confirmar_snk()
        if not notas_confirmar:
            print("Nenhuma nota para confirmar.")
            return True

        print(f"Notas para confirmar: {len(notas_confirmar)}")

        pedido = PedidoSnk()
        nota = NotaSnk()
        transferencia = TransferenciaSnk()
        first = True

        try:
            # Confirmar nota de transferência
            print("Confirmando nota de transferência...")
            ack_transf, dados_transferencia = await transferencia.buscar()
            if not ack_transf:
                print("Erro ao buscar dados da nota de transferência.")
                logger.error("Erro ao buscar dados da nota de transferência.")
                return False
            if ack_transf and not dados_transferencia:
                print("Nenhuma nota de transferência para confirmar.")
            if all([ack_transf,dados_transferencia]):
                ack_nota_transf = await transferencia.confirmar(nunota=dados_transferencia.get('nunota'))
                if not ack_nota_transf:
                    print("Erro ao confirmar nota de transferência. Verifique e tente novamente.")
                    logger.error("Erro ao confirmar nota de transferência.")
                    return False

            for i, pedido in enumerate(notas_confirmar):
                if not first:
                    time.sleep(self.req_time_sleep)  # Evita rate limit
                first = False

                print("")
                print(f"Nota {i + 1}/{len(notas_confirmar)}: {pedido.num_nota}")

                # Verifica se a nota já foi confirmada e só não foi atualizado na base do integrador
                validacao = await nota.buscar(nunota=pedido.nunota_nota)
                if validacao:
                    print(f"Nota {pedido.nunota_nota} já foi confirmada.")
                    venda.update_nota_confirma_snk(nunota_nota=pedido.nunota_nota,
                                                   dh_faturado=validacao.get('dtneg'))
                    continue

                # Confirma Nota no Sankhya
                print("Confirmando Nota no Sankhya...")
                ack = await nota.confirmar(nunota=pedido.nunota_nota)
                if not ack:
                    print(f"Erro ao confirmar nota {pedido.nunota_nota}")
                    logger.error("Erro ao confirmar nota %s",pedido.nunota_nota)
                    continue
                venda.update_nota_confirma_snk(nunota_nota=pedido.nunota_nota)              

                print(f"-> Confirmação da Nota {pedido.nunota_nota} concluída!")    

            print("=====================================================")
            print("-> PROCESSO DE CONSOLIDAÇÃO CONCLUÍDO!")
            return True
        except:
            return False