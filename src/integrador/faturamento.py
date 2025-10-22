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

    async def venda_entre_empresas_em_lote(self, nunota:int=None):

        faturamento = FaturamentoSnk()
        estoque = EstoqueSnk()
        transferencia = TransferenciaSnk()
        item_transf = ItemTransfSnk()
        parser = ParserTransferencia()

        print("Buscando itens conferidos no dia...")
        saldo_pedidos = await faturamento.buscar_itens(nunota=nunota)
        if isinstance(saldo_pedidos,list) and not saldo_pedidos:
            print("Nenhum item pendente de transferência.")
            return True
        
        if not saldo_pedidos:
            logger.error("Erro ao buscar itens conferidos no dia.")
            print("Erro ao buscar itens conferidos no dia.")
            return False

        print("Buscando saldo de estoque...")
        saldo_estoque = await estoque.buscar_saldo_por_lote(lista_produtos=saldo_pedidos)
        if not saldo_estoque:
            logger.error("Erro ao buscar saldo de estoque.")
            print("Erro ao buscar saldo de estoque.")
            return False

        print("Comparando saldos...")
        itens_venda_interna = await faturamento.compara_saldos(saldo_estoque=saldo_estoque,
                                                               saldo_pedidos=saldo_pedidos)

        if not itens_venda_interna:
            logger.info("Nenhum item para lançar venda interna.")
            print("Nenhum item para lançar venda interna.")
            return True

        print("Buscando valores de transferência...")
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

        print("Convertendo para o formato da API Sankhya...")
        cabecalho, itens = parser.to_sankhya(objeto='nota',
                                             itens_transferencia=itens_venda_interna)
        if not all([cabecalho, itens]):
            logger.error("Erro ao preparar dados da nota de transferência.")
            print("Erro ao preparar dados da nota de transferência.")
            return False

        print("Lançando nota de transferência...")
        ack, nunota = await transferencia.criar(cabecalho=cabecalho,
                                                itens=itens)        
        if not ack:
            logger.error("Erro ao lançar nota de transferência.")
            logger.info(f"cabecalho\n{cabecalho}")
            logger.info(f"itens\n{itens}")
            print("Erro ao lançar nota de transferência.")
            return False
        
        print("Confirmando nota de transferência...")
        ack = await transferencia.confirmar(nunota=nunota)
        if not ack:
            logger.error("Erro ao confirmar nota de transferência.")
            print("Erro ao confirmar nota de transferência.")
            return False
        
        print("VENDA ENTRE EMPRESAS REALIZADA COM SUCESSO!")        
        
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
        pedidos_faturar = venda.buscar_faturar()
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
                    venda.atualizar_faturada(nunota_pedido=pedido.nunota_pedido,
                                             nunota_nota=int(validacao.get('nunota')),
                                             dh_faturamento=validacao.get('dtneg'))
                    continue

                # Lança os itens na nota de transferência
                print("Lançando itens na nota de transferência...")
                ack_transferencia = await self.venda_entre_empresas_por_item(nunota=pedido.nunota_pedido)
                if not ack_transferencia:
                    print(f"Erro ao lançar itens na nota de transferência para o pedido {pedido.num_pedido}")
                    logger.error("Erro ao lançar itens na nota de transferência para o pedido %s",pedido.num_pedido)
                    continue

                # Fatura pedido no Sankhya
                print("Faturando pedido no Sankhya...")
                ack, nunota_nota = await pedido_snk.faturar(nunota=pedido.nunota_pedido)
                if not ack:
                    print(f"Erro ao faturar pedido {pedido.nunota_pedido}")
                    logger.error("Erro ao faturar pedido %s",pedido.nunota_pedido)
                    continue
                venda.atualizar_faturada(nunota_pedido=pedido.nunota_pedido,
                                         nunota_nota=nunota_nota)
                
                # Fatura pedido no Olist
                print("Gerando NF no Olist...")
                dados_nota_olist = await pedido_olist.gerar_nf(id=pedido.id_pedido)
                if not dados_nota_olist:
                    print(f"Erro ao gerar NF do pedido {pedido.nunota_pedido}")
                    logger.error("Erro ao gerar NF do pedido %s",pedido.nunota_pedido)
                    continue
                venda.atualizar_nf_gerada(cod_pedido=pedido.cod_pedido,
                                          num_nota=dados_nota_olist.get('numero'),
                                          id_nota=dados_nota_olist.get('id'))
                
                # Emite NF no Olist
                print(f"Autorizando NF {dados_nota_olist.get('numero')} pelo Olist...")                
                dados_emissao_nf_olist = await nota_olist.emitir(id=dados_nota_olist.get('id'))
                if not dados_emissao_nf_olist:
                    print(f"Erro ao emitir nota {dados_nota_olist.get('numero')} ref. pedido {pedido.cod_pedido}")
                    logger.error("Erro ao emitir nota %s ref. pedido %s",dados_nota_olist.get('numero'),pedido.cod_pedido)
                    continue
                venda.atualizar_nf_autorizada(id_nota=dados_nota_olist.get('id'))                
                
                # Envia pedido pra separação no Olist
                print("Enviando pedido para separação no Olist...")
                id_separacao = venda.buscar_idseparacao(cod_pedido=pedido.cod_pedido)
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
                venda.atualizar_financeiro(num_nota=dados_nota_olist.get('numero'),
                                           id_financeiro=dados_financeiro.get('id'))                

                print(f"-> Faturamento do pedido {pedido.num_pedido} concluído!")    

            print("=====================================================")
            print("-> PROCESSO DE FATURAMENTO CONCLUÍDO!")
            return True
        except:
            return False
    
    async def consolidar(self):

        # Busca as notas pendentes de confirmação
        notas_confirmar = venda.buscar_confirmar_nota()
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
                    venda.atualizar_confirmada_nota(nunota_nota=pedido.nunota_nota,
                                                    dh_confirmado=validacao.get('dtneg'))
                    continue

                # Confirma Nota no Sankhya
                print("Confirmando Nota no Sankhya...")
                ack = await nota.confirmar(nunota=pedido.nunota_nota)
                if not ack:
                    print(f"Erro ao confirmar nota {pedido.nunota_nota}")
                    logger.error("Erro ao confirmar nota %s",pedido.nunota_nota)
                    continue
                venda.atualizar_confirmada_nota(nunota_nota=pedido.nunota_nota)              

                print(f"-> Confirmação da Nota {pedido.nunota_nota} concluída!")    

            print("=====================================================")
            print("-> PROCESSO DE CONSOLIDAÇÃO CONCLUÍDO!")
            return True
        except:
            return False
        
    async def faturar_lote(self):        

        async def faturar(pedidos_faturar):

            print(f"Pedidos para faturamento: {len(pedidos_faturar)}")

            pedido_olist = PedidoOlist()
            nota_olist = NotaOlist()
            separacao = SeparacaoOlist()
            first = True
            try:                
                # Fatura os pedidos no Olist
                for i, pedido in enumerate(pedidos_faturar):
                    if not first:                    
                        time.sleep(self.req_time_sleep)  # Evita rate limit
                    first = False

                    print("")
                    print(f"Pedido {i + 1}/{len(pedidos_faturar)}: {pedido.num_pedido}")

                    # Fatura pedido no Olist
                    print("Gerando NF no Olist...")
                    dados_nota_olist = await pedido_olist.gerar_nf(id=pedido.id_pedido)
                    if not dados_nota_olist:
                        print(f"Erro ao gerar NF do pedido {pedido.nunota_pedido}")
                        logger.error("Erro ao gerar NF do pedido %s",pedido.nunota_pedido)
                        continue
                    venda.atualizar_nf_gerada(cod_pedido=pedido.cod_pedido,
                                              num_nota=dados_nota_olist.get('numero'),
                                              id_nota=dados_nota_olist.get('id'))
                    
                    # Emite NF no Olist
                    print(f"Autorizando NF {dados_nota_olist.get('numero')} pelo Olist...")                
                    dados_emissao_nf_olist = await nota_olist.emitir(id=dados_nota_olist.get('id'))
                    if not dados_emissao_nf_olist:
                        print(f"Erro ao emitir nota {dados_nota_olist.get('numero')} ref. pedido {pedido.cod_pedido}")
                        logger.error("Erro ao emitir nota %s ref. pedido %s",dados_nota_olist.get('numero'),pedido.cod_pedido)
                        continue
                    venda.atualizar_nf_autorizada(id_nota=dados_nota_olist.get('id'))                
                    
                    # Envia pedido pra separação no Olist
                    print("Enviando pedido para separação no Olist...")
                    id_separacao = venda.buscar_idseparacao(cod_pedido=pedido.cod_pedido)
                    if not id_separacao:
                        print(f"Separação do pedido {pedido.num_pedido} não localizada na base")
                        logger.error("Separação do pedido %s não localizada na base",pedido.num_pedido)
                        continue
                    ack_separacao = await separacao.separar(id=id_separacao)
                    if not ack_separacao:
                        print(f"Erro ao separar pedido {pedido.num_pedido}.")
                        logger.error("Erro ao separar pedido %s.",pedido.num_pedido)
                        continue

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
                    venda.atualizar_financeiro(num_nota=dados_nota_olist.get('numero'),
                                            id_financeiro=dados_financeiro.get('id'))                

                    print(f"-> Faturamento do pedido {pedido.num_pedido} concluído!")    

                print("-> PROCESSO DE FATURAMENTO CONCLUÍDO NO OLIST!")
                print("=====================================================")
                print("-> FINALIZANDO PROCESSO NO SANKHYA...") 

                try:
                    pedidos_sankhya = list(set([p.nunota_pedido for p in pedidos_faturar]))
                except Exception as e:
                    print(f"Erro ao extrair lista de pedidos para faturar no sankhya. {e}")
                    return False
                
                pedido_snk = PedidoSnk()
                nota_snk = NotaSnk()

                # Verifica se o pedido foi faturado ou trancou
                print("Verificando se o pedido foi faturado...")
                # val = 0
                for nunota in pedidos_sankhya:
                    try:
                        status_faturamento = await pedido_snk.buscar_nunota_nota(nunota=nunota)                    
                        if status_faturamento:
                            # val+=1
                            print(f"Pedido {nunota} já foi faturado.")
                            # Atualiza base de dados
                            venda.atualizar_faturada_lote(nunota_pedido=nunota,
                                                        nunota_nota=status_faturamento[0].get('nunota'))                    
                            venda.atualizar_confirmada_nota_lote(nunota_nota=status_faturamento[0].get('nunota'))
                            pedidos_sankhya.pop(pedidos_sankhya.index(nunota))
                    except Exception as e:
                        print(f"Erro ao verificar se o pedido {nunota} foi faturado. {e}")
                        return False

                #if val == len(pedidos_sankhya):
                if not pedidos_sankhya:
                    print("=====================================================")
                    print("-> PROCESSO CONCLUÍDO!")                
                    return True
                
                # Se o pedido não foi faturado...
                # Faz a nota de transferência
                print("Gerando a nota de transferência...")
                if not await self.venda_entre_empresas_em_lote():
                    logger.error("Erro ao gerar a nota de transferência.")
                    print("Erro ao gerar a nota de transferência.")
                    return False

                # Fatura no Sankhya
                nunotas_nota = []
                for nunota in pedidos_sankhya:
                    print(f"Faturando pedido {nunota} no Sankhya...")
                    ack, nunota_nota = await pedido_snk.faturar(nunota=nunota)
                    if not ack:
                        print(f"Erro ao faturar pedido {nunota}")
                        logger.error("Erro ao faturar pedido %s",nunota)
                        continue
                    nunotas_nota.append(nunota_nota)
                    venda.atualizar_faturada_lote(nunota_pedido=nunota,
                                                  nunota_nota=nunota_nota)
                if not nunotas_nota:
                    print("Erro ao faturar pedido(s) no Sankhya")
                    logger.error("Erro ao faturar pedido(s) no Sankhya")
                    return False

                # Confirma Nota(s) no Sankhya
                for nunota in nunotas_nota:
                    print(f"Confirmando Nota {nunota} no Sankhya...")
                    ack = await nota_snk.confirmar(nunota=nunota)
                    if not ack:
                        print(f"Erro ao confirmar nota {nunota}")
                        logger.error("Erro ao confirmar nota %s",nunota)                    
                    else:
                        venda.atualizar_confirmada_nota_lote(nunota_nota=nunota)
                        print(f"-> Confirmação da Nota {nunota} concluída!")

                return True
            except:
                return False
            
        # Busca os pedidos pendentes de faturamento
        pedidos_faturar_shopee = venda.buscar_faturar_por_loja(id_loja=9227)
        if not pedidos_faturar_shopee:
            print("Nenhum pedido para faturamento na shopee.")            
        
        # Busca os pedidos pendentes de faturamento
        pedidos_faturar_blz = venda.buscar_faturar_por_loja(id_loja=10940)
        if not pedidos_faturar_blz:
            print("Nenhum pedido para faturamento na blz na web.")

        if not any([pedidos_faturar_shopee,pedidos_faturar_blz]):
            ack_snk = await self.faturar_sankhya()
            return ack_snk
        
        if pedidos_faturar_shopee:
            print("-> FATURANDO SHOPEE NO OLIST...")
            ack_shopee = await faturar(pedidos_faturar=pedidos_faturar_shopee)
        else:
            ack_shopee = True
        
        if pedidos_faturar_blz:
            print("-> FATURANDO BLZ NO OLIST...")
            ack_blz = await faturar(pedidos_faturar=pedidos_faturar_blz)
        else:
            ack_blz = True        

        print("")
        print(f"-> PROCESSO DE FATURAMENTO DE PEDIDOS CONCLUÍDO!")
        return all([ack_shopee,ack_blz])

    async def faturar_sankhya(self):        

        # Busca os pedidos pendentes de faturamento
        pedidos_faturar = venda.buscar_faturar()
        if not pedidos_faturar:
            print("Nenhum pedido para faturamento.")
            return True

        print(f"Pedidos para faturamento: {len(pedidos_faturar)}")

        try:
            pedidos_sankhya = list(set([p.nunota_pedido for p in pedidos_faturar]))
            pedido_snk = PedidoSnk()
            nota_snk = NotaSnk()

            # Verifica se o pedido foi faturado ou trancou
            print("Verificando se o pedido foi faturado...")
            val = 0
            for nunota in pedidos_sankhya:
                status_faturamento = await pedido_snk.buscar_nunota_nota(nunota=nunota)
                if status_faturamento:
                    val+=1
                    print(f"Pedido {nunota} já foi faturado.")
                    # Atualiza base de dados
                    venda.atualizar_faturada_lote(nunota_pedido=nunota,
                                                  nunota_nota=status_faturamento[0].get('nunota'))                    
                    venda.atualizar_confirmada_nota_lote(nunota_nota=status_faturamento[0].get('nunota'))
            if val != 0:
                print("=====================================================")
                print("-> PROCESSO CONCLUÍDO!")                
                return True
            
            for nunota in pedidos_sankhya:
                # Se o pedido não foi faturado...
                # Faz a nota de transferência
                print("Gerando a nota de transferência...")
                if not await self.venda_entre_empresas_em_lote(nunota=nunota):
                    logger.error("Erro ao gerar a nota de transferência.")
                    print("Erro ao gerar a nota de transferência.")
                    return False

            # Fatura no Sankhya
            nunotas_nota = []
            for nunota in pedidos_sankhya:
                print(f"Faturando pedido {nunota} no Sankhya...")
                ack, nunota_nota = await pedido_snk.faturar(nunota=nunota)
                if not ack:
                    print(f"Erro ao faturar pedido {nunota}")
                    logger.error("Erro ao faturar pedido %s",nunota)
                    continue
                nunotas_nota.append(nunota_nota)
                venda.atualizar_faturada_lote(nunota_pedido=nunota,
                                              nunota_nota=nunota_nota)
            if not nunotas_nota:
                print("Erro ao faturar pedido(s) no Sankhya")
                logger.error("Erro ao faturar pedido(s) no Sankhya")
                return False

            # Confirma Nota(s) no Sankhya
            for nunota in nunotas_nota:
                print(f"Confirmando Nota {nunota} no Sankhya...")
                ack = await nota_snk.confirmar(nunota=nunota)
                if not ack:
                    print(f"Erro ao confirmar nota {nunota}")
                    logger.error("Erro ao confirmar nota %s",nunota)                    
                else:
                    venda.atualizar_confirmada_nota_lote(nunota_nota=nunota)
                    print(f"-> Confirmação da Nota {nunota} concluída!")                

            print("=====================================================")
            print("-> PROCESSO CONCLUÍDO!")

            return True
        except:
            return False