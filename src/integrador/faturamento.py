import os
import time
from datetime import datetime
from src.sankhya.faturamento import Faturamento as FaturamentoSnk
from src.sankhya.estoque import Estoque as EstoqueSnk
from src.sankhya.transferencia import Transferencia as TransferenciaSnk
from src.sankhya.transferencia import Itens as ItemTransfSnk
from src.sankhya.pedido import Pedido as PedidoSnk
from src.sankhya.nota import Nota as NotaSnk
from src.integrador.nota import Nota as IntegradorNota
from src.integrador.pedido import Pedido as IntegradorPedido
from src.integrador.financeiro import Financeiro as IntegradorFinanceiro
from src.parser.transferencia import Transferencia as ParserTransferencia
from src.parser.pedido import Pedido as ParserPedido
from src.olist.separacao import Separacao as SeparacaoOlist
from database.crud import log as crudLog
from database.crud import log_pedido as crudLogPed
from database.crud import pedido as crudPedido
from database.crud import nota as crudNota
from src.utils.decorador import contexto, carrega_dados_empresa, carrega_dados_ecommerce, log_execucao, interno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Faturamento:

    def __init__(self, codemp:int=None, empresa_id:int=None, id_loja:int=None):
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.id_loja = id_loja
        self.log_id = None
        self.contexto = 'faturamento'
        self.dados_empresa:dict = None
        self.dados_ecommerce:dict = None
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))

    @contexto
    @carrega_dados_empresa
    async def venda_entre_empresas_em_lote(self,**kwargs) -> dict:
        """
        Rotina para lançar a nota de ressuprimento de estoque da matriz para o e-commerce considerando tudo que foi conferido no dia.
            :return dict: dicionário com status e erro
        """

        nunota:int = kwargs.get('nunota',None) 
        loja_unica:bool=kwargs.get('loja_unica',False)
        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                              de='sankhya',
                                              para='sankhya',
                                              contexto=kwargs.get('_contexto'))        
        
        faturamento = FaturamentoSnk(codemp=self.dados_empresa.get('snk_codemp'))
        estoque = EstoqueSnk(codemp=self.dados_empresa.get('snk_codemp'))
        transferencia = TransferenciaSnk(codemp=self.dados_empresa.get('snk_codemp'))
        item_transf = ItemTransfSnk(codemp=self.dados_empresa.get('snk_codemp'))
        parser = ParserTransferencia(codemp=self.dados_empresa.get('snk_codemp'))
        
        try:            
            # Busca itens conferidos no dia
            if loja_unica:
                saldo_pedidos = await faturamento.buscar_itens(nunota=nunota)
            else:
                saldo_pedidos = await faturamento.buscar_itens()
                
            if isinstance(saldo_pedidos,bool):
                msg = "Erro ao buscar itens conferidos no dia."
                raise Exception(msg)
            elif not saldo_pedidos:
                # Sem itens conferidos
                return {"success": True, "__exception__": None}
            
            # Busca saldo de estoque
            saldo_estoque = await estoque.buscar_saldo_por_lote(lista_produtos=saldo_pedidos)
            if not saldo_estoque:
                msg = "Erro ao buscar saldo de estoque."
                raise Exception(msg)

            # Compara quantidade conferida com estoque disponível
            itens_venda_interna = await faturamento.compara_saldos(saldo_estoque=saldo_estoque,
                                                                   saldo_pedidos=saldo_pedidos)
            if not itens_venda_interna:
                # Sem necessidade de venda interna
                return {"success": True, "__exception__": None}

            # Busca valor de tranferência dos itens
            codigos_produtos = [item.get('codprod') for item in itens_venda_interna]
            valores_produtos = await item_transf.busca_valor_transferencia(lista_itens=codigos_produtos)
            if not valores_produtos:
                msg = "Erro ao buscar valores de transferência."
                raise Exception(msg)

            # Vincula o valor de transferência o respectivo produto
            for item in itens_venda_interna:
                for valor in valores_produtos:
                    if item.get('codprod') == valor.get('codprod'):
                        item['valor'] = float(valor.get('valor')) if valor.get('valor') else 0.1
                        break

            # Converte para o formato da API Sankhya
            cabecalho, itens = await parser.to_sankhya(objeto='nota',
                                                       itens_transferencia=itens_venda_interna,
                                                       itens_transferidos=[])
            if not all([cabecalho, itens]):
                msg = "Erro ao preparar dados da nota de transferência."
                raise Exception(msg)

            # Lança nota de transferência
            ack, nunota = await transferencia.criar(cabecalho=cabecalho,
                                                    itens=itens)        
            if not ack:
                msg = "Erro ao lançar nota de transferência."
                raise Exception(msg)
            
            # Confirma nota de transferência
            ack = await transferencia.confirmar(nunota=nunota)
            if not ack:
                msg = "Erro ao confirmar nota de transferência."
                raise Exception(msg)
            return {"success": True, "__exception__": None}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    @contexto
    @interno
    @carrega_dados_empresa
    async def venda_entre_empresas_por_item(self,nunota:int=None,**kwargs) -> dict:
        """
        Rotina para lançar a nota de ressuprimento de estoque da matriz para o e-commerce por item.
            :param nunota: número único do pedido de venda (Sankhya)        
            :return dict: dicionário com status e erro
        """        

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                              de='sankhya',
                                              para='sankhya',
                                              contexto=kwargs.get('_contexto'))        
        try:
            # Busca itens conferidos no dia
            faturamento = FaturamentoSnk(codemp=self.codemp)
            saldo_pedidos = await faturamento.buscar_itens()
            if not saldo_pedidos:
                msg = "Erro ao buscar itens conferidos no dia."
                raise Exception(msg)            
            
            # Busca saldo de estoque
            estoque = EstoqueSnk(codemp=self.codemp)
            saldo_estoque = await estoque.buscar_saldo_por_lote(lista_produtos=saldo_pedidos)
            if not saldo_estoque:
                msg = "Erro ao buscar saldo de estoque."
                raise Exception(msg)

            # Compara quantidade conferida com estoque disponível
            itens_venda_interna = await faturamento.compara_saldos(saldo_estoque=saldo_estoque,
                                                                   saldo_pedidos=saldo_pedidos)
            if not itens_venda_interna:
                return {"success": True, "__exception__": None}
            # Busca valor de tranferência dos itens
            item_transf = ItemTransfSnk(codemp=self.codemp)
            codigos_produtos = [item.get('codprod') for item in itens_venda_interna]
            valores_produtos = await item_transf.busca_valor_transferencia(lista_itens=codigos_produtos)
            if not valores_produtos:
                msg = "Erro ao buscar valores de transferência."
                raise Exception(msg)
            
            # Vincula o valor de transferência o respectivo produto
            for item in itens_venda_interna:
                for valor in valores_produtos:
                    if item.get('codprod') == valor.get('codprod'):
                        item['valor'] = float(valor.get('valor')) if valor.get('valor') else 0.1
                        break

            # Verifica se existe uma nota de transferência em aberto
            transferencia = TransferenciaSnk(codemp=self.codemp)                    
            ack_nota_transferencia, dados_nota_transferencia = await transferencia.buscar(itens=True)
            if not ack_nota_transferencia:
                msg = "Erro ao buscar dados da nota de transferência."
                raise Exception(msg)            
            if dados_nota_transferencia:
                nunota = dados_nota_transferencia.get('nunota')

            # Converte para o formato da API Sankhya
            parser = ParserTransferencia(codemp=self.codemp)
            if dados_nota_transferencia:
                # Formato para adicionar itens na nota de transferência existente                
                dados_cabecalho, dados_itens = parser.to_sankhya(objeto='item',
                                                                 nunota=nunota,
                                                                 itens_transferencia=itens_venda_interna,
                                                                 itens_transferidos=dados_nota_transferencia.get('itens'))
            else:
                # Formato para criar nova nota de transferência
                dados_cabecalho, dados_itens = parser.to_sankhya(objeto='nota',
                                                                 itens_transferencia=itens_venda_interna,
                                                                 itens_transferidos=[])

            # Lança nota de transferência
            if dados_nota_transferencia:
                ack = await item_transf.lancar(nunota=nunota,
                                               dados_item=dados_itens)
            else:                
                ack, nunota = await transferencia.criar(cabecalho=dados_cabecalho,
                                                        itens=dados_itens)
            if not ack:
                msg = "Erro ao lançar/atualizar nota de transferência."
                raise Exception(msg)
            return {"success": True, "__exception__": None}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    @contexto
    @interno
    @carrega_dados_ecommerce
    async def faturar_olist(self,pedido:dict,**kwargs) -> dict:
        """
        Rotina para faturar os pedidos (emitir NF, enviar para separação e baixar contas a receber) no Olist.
            :param pedido: dicionário com os dados do pedido (Olist)
            :return dict: dicionário com status e erro
        """        

        integra_nota = IntegradorNota(id_loja=self.dados_ecommerce.get('id_loja'))
        separacao = SeparacaoOlist(codemp=self.codemp)

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='sankhya',
                                              para='olist',
                                              contexto=kwargs.get('_contexto'))
        try:
            # Cria NF no Olist
            ack_criacao = await integra_nota.gerar(dados_pedido=pedido)
            if not ack_criacao.get('success'):
                raise Exception(f"Erro na criação da NF: {ack_criacao.get('__exception__')}")
                            
            # Emite NF
            ack_emissao = await integra_nota.emitir(ack_criacao.get('dados_nota'))
            if not ack_emissao.get('success'):
                raise Exception(f"Erro na emissão da NF: {ack_emissao.get('__exception__')}")
            
            # Envia pedido pra separação no Olist
            ack_separacao = await separacao.separar(id=pedido.get('id_separacao'))
            if not ack_separacao:
                raise Exception(f"Erro na separação do pedido: {ack_separacao.get('__exception__')}")

            # Recebe contas a receber do Olist
            ack_conta = await integra_nota.receber_conta(ack_criacao.get('dados_nota'))
            if not ack_conta.get('success'):
                raise Exception(f"Erro ao receber conta: {ack_conta.get('__exception__')}")
            
            return {"success": True, "__exception__": None}
        except Exception as e:
            logger.error("Erro ao faturar pedido no Olist: %s",str(e))
            return {"success": False, "__exception__": str(e)}

    @contexto
    @interno
    @carrega_dados_ecommerce
    async def faturar_sankhya(self,nunota:int,**kwargs) -> dict:
        """
        Rotina para faturar os pedidos (gerar ressuprimento, faturar pedido, confirmar nota de venda, baixar estoque do local e-commerce) no Sankhya.
            :param nunota: número único do pedido (Sankhya)
            :return dict: dicionário com status e erro
        """  

        loja_unica:bool=kwargs.get('loja_unica',False)
        pedido_snk = PedidoSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        nota_snk = NotaSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        parser_pedido = ParserPedido(id_loja=self.dados_ecommerce.get('id_loja'))

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='olist',
                                              para='sankhya',
                                              contexto=kwargs.get('_contexto'))
        try:
            if nunota != -1:
                # Verifica se o pedido foi faturado ou trancou
                status_faturamento = await pedido_snk.buscar_nunota_nota(nunota=nunota)
                if status_faturamento:
                    nunota_nota = status_faturamento[0].get('nunota')
                    # Atualiza base de dados
                    await crudPedido.atualizar(nunota=nunota,
                                            dh_faturamento=datetime.now())
                    await crudNota.atualizar(nunota_pedido=nunota,
                                            nunota=nunota_nota)
                else:
                    # Se o pedido não foi faturado...
                    # Fatura no Sankhya
                    ack, nunota_nota = await pedido_snk.faturar(nunota=nunota)
                    if not ack:
                        msg = f"Erro ao faturar pedido {nunota}"
                        raise Exception(msg)
                    
                    # Atualiza local
                    dados_nota:dict = await nota_snk.buscar(nunota=nunota_nota,itens=True)
                    if not dados_nota:
                        msg = f"Erro ao buscar dados da nota {nunota_nota}"
                        raise Exception(msg)                
                    sequencias:list = [int(item.get('sequencia')) for item in dados_nota.get('itens')]
                    payload:list[dict] = await parser_pedido.to_sankhya_atualiza_local(nunota=nunota_nota,
                                                                                    lista_sequencias=sequencias)
                    if not payload:
                        msg = f"Erro ao preparar dados da nota {nunota_nota}"
                        raise Exception(msg)                
                    ack = await pedido_snk.atualizar_local(nunota=nunota_nota,payload=payload)
                    if not ack:
                        msg = f"Erro ao atualizar local de destino dos itens da nota {nunota_nota}"
                        raise Exception(msg)

                    # Atualiza base de dados
                    await crudPedido.atualizar(nunota=nunota,
                                               dh_faturamento=datetime.now())                
                    await crudNota.atualizar(nunota_pedido=nunota,
                                             nunota=nunota_nota)

                # Confirma nota no Sankhya
                ack = await nota_snk.confirmar(nunota=nunota_nota)
                if ack is False:
                    msg = f"Erro ao confirmar nota {nunota_nota}"
                    raise Exception(msg)
                else:
                    await crudNota.atualizar(nunota_nota=nunota_nota,dh_confirmacao=datetime.now())
            else:
                # Atualiza base de dados
                await crudPedido.atualizar(nunota=nunota,
                                           dh_faturamento=datetime.now())
                await crudNota.atualizar(nunota_pedido=nunota,
                                         dh_confirmacao=datetime.now())
                nunota_nota = nunota

            # Realiza baixa de estoque do local e-commerce
            ack = await self.baixar_ecommerce(nunota_nota=nunota_nota)
            if not ack.get('success'):
                raise Exception(ack.get('__exception__'))
            return {"success": True, "__exception__": None}
        
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    def validar_quantidades_baixa_ecommerce(self,itens_pedidos:list[dict],itens_nota:list[dict]) -> list[dict]:
        """
        Valida o saldo de estoque para realizar a baixa do local do e-commerce no Sankhya.
            :param itens_pedidos: lista de dicionários com os dados dos itens dos pedidos (Olist)
            :param itens_nota: lista de dicionários com os dados dos itens da nota de ressuprimento (Sankhya)
            :return list[dict]: lista de dicionários com os dados dos itens a serem baixados do estoque
        """  
        lista_produtos_baixa:list[dict] = []

        # Unifica itens da nota
        aux_nota:list[dict] = []
        try:
            if itens_nota:
                for item_nota in itens_nota:
                    if not next((b for b in aux_nota if int(b['codprod']) == int(item_nota['codprod'])),None):
                        aux_nota.append({
                            'codprod':int(item_nota.get('codprod')),
                            'qtdneg':int(item_nota.get('qtdneg'))
                        })
                    else:
                        for item in aux_nota:
                            if item.get('codprod') == item_nota.get('codprod'):
                                item['qtdneg']+=int(item_nota.get('qtdneg'))

            for item_pedido in itens_pedidos:
                lista_produtos_baixa.append(item_pedido)
                if itens_nota:
                    match = next((b for b in aux_nota if int(b['codprod']) == int(item_pedido['codprod'])),None)
                    # print(f"Validando produto {item_pedido.get('codprod')} - Qtd pedido: {item_pedido.get('qtdneg')} x Qtd nota: {match.get('qtdneg') if match else '0'}")
                    if not match:
                        pass
                    elif int(item_pedido.get('qtdneg')) > int(match.get('qtdneg')):
                        lista_produtos_baixa[lista_produtos_baixa.index(item_pedido)]['qtdneg'] = int(item_nota.get('qtdneg')) - int(item_pedido.get('qtdneg'))
                    else:
                        pass
        except Exception as e:
            logger.error("Erro ao validar baixa de estoque: %s",str(e))
        finally:
            pass           
        return lista_produtos_baixa

    @carrega_dados_empresa
    @carrega_dados_ecommerce
    async def desmembrar_lotes_baixa_ecommerce(self,lista_itens:list[dict]) -> list[dict]:
        """
        Desmembra a quantidade total dos itens para baixa do local do e-commerce no Sankhya dentre os lotes disponíveis no sistema.
            :param lista_itens: lista de dicionários com os dados dos itens a serem baixados do estoque
            :return list[dict]: lista de dicionários com os dados dos itens a serem baixados do estoque por lote
        """  

        lista_produtos_baixa:list[dict] = []

        # Buscar o estoque atual no e-commerce por lote
        estoque = EstoqueSnk(codemp=self.codemp, empresa_id=self.dados_ecommerce.get('empresa_id'))
        lista_produtos_busca:list[int] = [int(item.get('codprod')) for item in lista_itens]
        estoque_atual:list[dict] = await estoque.buscar_saldo_ecommerce_por_lote(lista_produtos=lista_produtos_busca)
        if not estoque_atual:
            print("Saldo de estoque atual não encontrado.")
            return lista_produtos_baixa

        try:
            for item in lista_itens:
                lista_produtos_baixa.append(item)
                qtd_pendente:int = int(item.get('qtdneg'))
                
                while qtd_pendente > 0:
                    # Percorrer os lotes verificando se o lote tem saldo para baixar a quantidade total do item
                    saldo_produto = next((b for b in estoque_atual if int(b['codprod']) == int(item['codprod']) and int(b['estoque']) > 0),None)
                    if not saldo_produto:
                        qtd_pendente = -1
                    else:
                        if qtd_pendente <= int(saldo_produto.get('estoque')):
                            lista_produtos_baixa[lista_produtos_baixa.index(item)]['controle'] = saldo_produto.get('controle')
                            qtd_pendente = -1
                        else:
                            lista_produtos_baixa[lista_produtos_baixa.index(item)]['controle'] = saldo_produto.get('controle')
                            lista_produtos_baixa[lista_produtos_baixa.index(item)]['qtdneg'] = saldo_produto.get('estoque')
                            qtd_pendente -= int(saldo_produto.get('estoque'))
                            estoque_atual.remove(saldo_produto)
        except Exception as e:
            logger.error("Erro ao desmembrar lotes para baixa de estoque: %s",str(e))
        finally:
            pass                       
        return lista_produtos_baixa

    @contexto
    @carrega_dados_ecommerce
    async def baixar_ecommerce(self,nunota_nota:int,**kwargs) -> dict:
        """
        Realiza a baixa de estoque do local e-commerce no Sankhya.
            :param nunota_nota: número único da nota de venda no Sankhya
            :return dict: dicionário com status, dados da nota e erro
        """ 

        loja_unica:bool=kwargs.get('loja_unica',False)
        pedido_snk = PedidoSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        nota_snk = NotaSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        integrador_pedido = IntegradorPedido(id_loja=self.dados_ecommerce.get('id_loja'))
        
        try:
            estoque_baixar:list[dict] = await crudPedido.buscar_baixar_estoque(ecommerce_id=self.dados_ecommerce.get('id'))
            if not estoque_baixar:
                return True
            
            # Unifica os itens dos pedidos
            dados_pedidos_olist:list[dict] = [pedido.get('dados_pedido') for pedido in estoque_baixar]
            pedidos_agrupados, itens_agrupados = integrador_pedido.unificar(lista_pedidos=dados_pedidos_olist)
            if not all([pedidos_agrupados, itens_agrupados]):
                msg = "Erro ao unificar pedidos"
                raise Exception(msg)

            dados_nota:dict={}
            if nunota_nota != -1:
                # Busca dados da nota
                dados_nota:dict = await nota_snk.buscar(nunota=nunota_nota,itens=True)
                if not dados_nota:
                    msg = "Erro ao buscar dados da nota"
                    raise Exception(msg)

            # Validando saldos
            lista_produtos_baixa:list[dict] = self.validar_quantidades_baixa_ecommerce(itens_pedidos=itens_agrupados, itens_nota=dados_nota.get('itens'))
            if not lista_produtos_baixa:
                msg = "Erro ao validar quantidades para baixa de estoque"
                raise Exception(msg)

            lista_produtos_baixa = await self.desmembrar_lotes_baixa_ecommerce(lista_itens=lista_produtos_baixa)
            if not lista_produtos_baixa:
                msg = "Erro ao desmembrar lotes para baixa de estoque"
                raise Exception(msg)
            
            # Converte para o formato da API do Sankhya
            parser = ParserPedido(id_loja=self.id_loja)
            dados_cabecalho, dados_itens = await parser.to_sankhya_baixa_estoque_ecommerce(lista_itens=lista_produtos_baixa)
            if not all([dados_cabecalho, dados_itens]):
                msg = "Erro ao converter dados dos pedidos para o formato da API do Sankhya"
                raise Exception(msg)

            # Insere os dados do pedido
            pedido_incluido = await pedido_snk.lancar(dados_cabecalho=dados_cabecalho,
                                                      dados_itens=dados_itens)
            if not pedido_incluido:
                msg = f"Erro ao inserir pedido no Sankhya."
                raise Exception(msg)

            # Atualiza base de dados
            await crudNota.atualizar(nunota_nota=nunota_nota,
                                     baixa_estoque_ecommerce=True)

            # Confirma nota no Sankhya
            ack = await pedido_snk.confirmar(nunota=pedido_incluido)
            if ack is False:
                msg = f"Erro ao confirmar baixa {pedido_incluido}"
                raise Exception(msg)
            else:
                pass

            return {"success": True, "__exception__": None}
        except Exception as e:
            logger.error("Erro ao baixar estoque do ecommerce: %s",str(e))
            return {"success": False, "__exception__": str(e)}

    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def integrar_olist(self,**kwargs) -> bool:
        """
        Busca os pedidos pendentes e executa a rotina para faturar os pedidos no Olist.
            :return bool: status da operação
        """           
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='sankhya',
                                          para='olist',
                                          contexto=kwargs.get('_contexto')) 

        # Busca os pedidos pendentes de faturamento
        pedidos_faturar = await crudPedido.buscar_faturar(ecommerce_id=self.dados_ecommerce.get('id'))
        if not pedidos_faturar:
            # Nenhum pedido para faturamento
            await crudLog.atualizar(id=self.log_id)
            return True

        for i, pedido in enumerate(pedidos_faturar):
            time.sleep(self.req_time_sleep)
            # Fatura pedido no Olist
            ack_pedido = await self.faturar_olist(pedido=pedido)
            await crudLogPed.criar(log_id=self.log_id,
                                   pedido_id=pedido.get('id'),
                                   evento='F',
                                   sucesso=ack_pedido.get('success'),
                                   obs=ack_pedido.get('__exception__',None))
                    
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        return status_log
    
    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def integrar_snk(self,**kwargs) -> bool:
        """
        Busca os pedidos pendentes e executa a rotina para faturar os pedidos no Sankhya.
            :return bool: status da operação
        """  
        loja_unica:bool=kwargs.get('loja_unica',False)
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto')) 

        # Busca os pedidos pendentes de faturamento
        pedidos_faturar = await crudPedido.buscar_faturar(ecommerce_id=self.dados_ecommerce.get('id'))
        if not pedidos_faturar:
            # Nenhum pedido para faturamento
            await crudLog.atualizar(id=self.log_id)
            return True
        
        print(f"Pedidos para faturar: {len(pedidos_faturar)}")
        
        pedidos_faturar = list(set([p.get("nunota") for p in pedidos_faturar]))

        for i, pedido in enumerate(pedidos_faturar):
            print(f"Faturando pedido {pedido} ({i+1}/{len(pedidos_faturar)})")
            time.sleep(self.req_time_sleep)
            # Fatura pedido no Olist
            ack_pedido = await self.faturar_sankhya(nunota=pedido,loja_unica=loja_unica)
            await crudLogPed.criar(log_id=self.log_id,
                                   nunota=pedido,
                                   evento='F',
                                   sucesso=ack_pedido.get('success'),
                                   obs=ack_pedido.get('__exception__',None))               
        
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        return status_log    
    
    @contexto
    @log_execucao
    @carrega_dados_empresa
    async def realizar_venda_interna(self,**kwargs) -> bool:
        """
        Executa a rotina da nota de ressuprimento de estoque considerando tudo que foi conferido no dia.
            :return bool: status da operação
        """
        
        self.log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))
        ack_venda = await self.venda_entre_empresas_em_lote()    
        await crudLogPed.criar(log_id=self.log_id,
                               pedido_id=None,
                               evento='F',
                               sucesso=ack_venda.get('success'),
                               obs=ack_venda.get('__exception__',None))        
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        return status_log