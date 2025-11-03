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
from src.parser.transferencia import Transferencia as ParserTransferencia
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
    async def venda_entre_empresas_em_lote(
            self,
            **kwargs
        ) -> dict:

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
            # print("-> Buscando itens conferidos no dia...")
            if loja_unica:
                saldo_pedidos = await faturamento.buscar_itens(nunota=nunota)
            else:
                saldo_pedidos = await faturamento.buscar_itens()
                
            if isinstance(saldo_pedidos,bool):
                msg = "Erro ao buscar itens conferidos no dia."
                raise Exception(msg)
            elif not saldo_pedidos:
                # Sem itens conferidos
                return {"success": True}
            
            # Busca saldo de estoque
            # print("-> Buscando saldo de estoque...")
            saldo_estoque = await estoque.buscar_saldo_por_lote(lista_produtos=saldo_pedidos)
            if not saldo_estoque:
                msg = "Erro ao buscar saldo de estoque."
                raise Exception(msg)

            # Compara quantidade conferida com estoque disponível
            # print("Comparando quantidade conferida com estoque disponível...")
            itens_venda_interna = await faturamento.compara_saldos(saldo_estoque=saldo_estoque,
                                                                   saldo_pedidos=saldo_pedidos)
            if not itens_venda_interna:
                print("Sem necessidade de venda interna")
                return {"success": True}

            # Busca valor de tranferência dos itens
            # print("Buscando valores de transferência...")
            codigos_produtos = [item.get('codprod') for item in itens_venda_interna]
            valores_produtos = await item_transf.busca_valor_transferencia(lista_itens=codigos_produtos)
            if not valores_produtos:
                msg = "Erro ao buscar valores de transferência."
                raise Exception(msg)

            # Vincula o valor de transferência o respectivo produto
            # print("Vinculando valores aos produtos...")
            for item in itens_venda_interna:
                for valor in valores_produtos:
                    if item.get('codprod') == valor.get('codprod'):
                        item['valor'] = float(valor.get('valor')) if valor.get('valor') else 0.1
                        break

            # Converte para o formato da API Sankhya
            # print("Convertendo para o formato da API Sankhya...")
            cabecalho, itens = await parser.to_sankhya(objeto='nota',
                                                       itens_transferencia=itens_venda_interna,
                                                       itens_transferidos=[])
            if not all([cabecalho, itens]):
                msg = "Erro ao preparar dados da nota de transferência."
                raise Exception(msg)

            # Lança nota de transferência
            # print("Lançando nota de transferência...")
            ack, nunota = await transferencia.criar(cabecalho=cabecalho,
                                                    itens=itens)        
            if not ack:
                msg = "Erro ao lançar nota de transferência."
                raise Exception(msg)
            
            # Confirma nota de transferência
            # print(f"Confirmando nota de transferência {nunota}...")
            ack = await transferencia.confirmar(nunota=nunota)
            if not ack:
                msg = "Erro ao confirmar nota de transferência."
                raise Exception(msg)
            return {"success": True}
        except Exception as e:
            print(str(e))
            return {"success": False, "__exception__": str(e)}

    @contexto
    @interno
    @carrega_dados_empresa
    async def venda_entre_empresas_por_item(
            self,
            nunota:int=None,
            **kwargs
        ) -> dict:

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                              de='sankhya',
                                              para='sankhya',
                                              contexto=kwargs.get('_contexto'))        
        try:
            # Busca itens conferidos no dia        
            print("-> Buscando itens conferidos no dia...")
            faturamento = FaturamentoSnk(codemp=self.codemp)
            saldo_pedidos = await faturamento.buscar_itens()
            if not saldo_pedidos:
                msg = "Erro ao buscar itens conferidos no dia."
                raise Exception(msg)            
            
            # Busca saldo de estoque
            print("-> Buscando saldo de estoque...")
            estoque = EstoqueSnk(codemp=self.codemp)
            saldo_estoque = await estoque.buscar_saldo_por_lote(lista_produtos=saldo_pedidos)
            if not saldo_estoque:
                msg = "Erro ao buscar saldo de estoque."
                raise Exception(msg)

            # Compara quantidade conferida com estoque disponível
            print("-> Comparando quantidade conferida com estoque disponível...")
            itens_venda_interna = await faturamento.compara_saldos(saldo_estoque=saldo_estoque,
                                                                   saldo_pedidos=saldo_pedidos)
            if not itens_venda_interna:
                return {"success": True}
            # Busca valor de tranferência dos itens
            print("-> Buscando valores de transferência...")
            item_transf = ItemTransfSnk(codemp=self.codemp)
            codigos_produtos = [item.get('codprod') for item in itens_venda_interna]
            valores_produtos = await item_transf.busca_valor_transferencia(lista_itens=codigos_produtos)
            if not valores_produtos:
                msg = "Erro ao buscar valores de transferência."
                raise Exception(msg)
            
            # Vincula o valor de transferência o respectivo produto
            print("-> Vinculando valores aos produtos...")
            for item in itens_venda_interna:
                for valor in valores_produtos:
                    if item.get('codprod') == valor.get('codprod'):
                        item['valor'] = float(valor.get('valor')) if valor.get('valor') else 0.1
                        break

            # Verifica se existe uma nota de transferência em aberto
            print("-> Verificando se existe uma nota de transferência em aberto...")
            transferencia = TransferenciaSnk(codemp=self.codemp)                    
            ack_nota_transferencia, dados_nota_transferencia = await transferencia.buscar(itens=True)
            if not ack_nota_transferencia:
                msg = "Erro ao buscar dados da nota de transferência."
                raise Exception(msg)            
            if dados_nota_transferencia:
                nunota = dados_nota_transferencia.get('nunota')
                print(f"Nota encontrada: {nunota}")
            else:
                print("Nenhuma nota de transferência encontrada. Criando nova...")

            # Converte para o formato da API Sankhya
            print("-> Convertendo para o formato da API Sankhya...")
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
            print("-> Lançando/atualizando nota de transferência...")
            if dados_nota_transferencia:
                ack = await item_transf.lancar(nunota=nunota,
                                               dados_item=dados_itens)
            else:                
                ack, nunota = await transferencia.criar(cabecalho=dados_cabecalho,
                                                        itens=dados_itens)
            if not ack:
                msg = "Erro ao lançar/atualizar nota de transferência."
                raise Exception(msg)
            return {"success": True}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    @contexto
    @interno
    @carrega_dados_ecommerce
    async def faturar_olist(
            self,
            pedido:dict,
            **kwargs
        ) -> bool:

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
                # pass
                # print(ack_criacao.get('__exception__'))
                raise Exception(ack_criacao.get('__exception__'))
                            
            # Emite NF
            ack_emissao = await integra_nota.emitir(ack_criacao.get('dados_nota'))
            if not ack_emissao.get('success'):
                # pass
                # print(ack_emissao.get('__exception__'))
                raise Exception(ack_emissao.get('__exception__'))
            
            # Envia pedido pra separação no Olist
            # print("Enviando pedido para separação no Olist...")
            ack_separacao = await separacao.separar(id=pedido.get('id_separacao'))
            if not ack_separacao:
                # pass
                # print(ack_separacao.get('__exception__'))
                raise Exception(ack_separacao.get('__exception__'))

            # Recebe contas a receber do Olist
            # print("Recebe contas a receber do Olist")
            ack_conta = await integra_nota.receber_conta(ack_criacao.get('dados_nota'))
            if not ack_conta.get('success'):
                # pass
                print(ack_conta.get('__exception__'))                
                # raise Exception(ack_conta.get('__exception__'))
            
            # Baixa contas a receber do Olist
            # print("Baixa contas a receber do Olist")
            ack_baixa = await integra_nota.baixar_conta(id_nota=ack_criacao['dados_nota'].get('id'),                                                       
                                                        dados_financeiro=ack_conta.get('dados_financeiro'))
            if not ack_baixa.get('success'):
                # pass
                print(ack_baixa.get('__exception__'))                   
                # raise Exception(ack_baixa.get('__exception__'))
            
            print(f"Faturamento do pedido concluído!")    
            return {"success": True}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    @contexto
    @interno
    @carrega_dados_ecommerce
    async def faturar_sankhya(
            self,
            pedido:int,
            **kwargs
        ) -> bool:
            
        loja_unica:bool=kwargs.get('loja_unica',False)
        pedido_snk = PedidoSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        nota_snk = NotaSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='olist',
                                              para='sankhya',
                                              contexto=kwargs.get('_contexto'))
        try:
            # Verifica se o pedido foi faturado ou trancou
            print("Verificando se o pedido foi faturado...")
            nunota = pedido
            status_faturamento = await pedido_snk.buscar_nunota_nota(nunota=nunota)
            if status_faturamento:
                nunota_nota = status_faturamento[0].get('nunota')
                print(f"Pedido {nunota} já foi faturado na nota de venda {nunota_nota}.")
                # Atualiza base de dados
                await crudPedido.atualizar(nunota=nunota,
                                           dh_faturamento=datetime.now())
                await crudNota.atualizar(nunota_pedido=nunota,
                                         nunota=nunota_nota)
            else:
                # Se o pedido não foi faturado...
                # Faz a nota de transferência
                print("Gerando a nota de transferência...")
                ack = await self.venda_entre_empresas_em_lote(loja_unica=loja_unica, nunota=nunota)
                if not ack:
                    msg = "Erro ao gerar a nota de transferência."
                    raise Exception(msg)

                # Fatura no Sankhya
                print(f"Faturando pedido {nunota} no Sankhya...")
                ack, nunota_nota = await pedido_snk.faturar(nunota=nunota)
                if not ack:
                    msg = f"Erro ao faturar pedido {nunota}"
                    raise Exception(msg)
                
                # Atualiza base de dados
                await crudPedido.atualizar(nunota=nunota,
                                           dh_faturamento=datetime.now())                
                await crudNota.atualizar(nunota_pedido=nunota,
                                         nunota=nunota_nota)

            # Confirma nota no Sankhya
            print(f"Confirmando nota de venda {nunota_nota} no Sankhya...")
            ack = await nota_snk.confirmar(nunota=nunota_nota)
            if ack is False:
                msg = f"Erro ao confirmar nota {nunota_nota}"
                raise Exception(msg)
            if ack is None:
                print(f"Nota de venda {nunota_nota} já foi confirmada")
            if ack:
                pass
            # Atualiza base de dados
            await crudNota.atualizar(nunota_pedido=nunota,
                                     dh_confirmacao=datetime.now())
            print(f"Faturamento do pedido {nunota} concluído!")
            return {"success": True}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def integrar_olist(self,**kwargs):
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='sankhya',
                                          para='olist',
                                          contexto=kwargs.get('_contexto')) 

        # Busca os pedidos pendentes de faturamento
        print("-> Buscando os pedidos pendentes de faturamento...")
        pedidos_faturar = await crudPedido.buscar_faturar(ecommerce_id=self.dados_ecommerce.get('id'))
        if not pedidos_faturar:
            print("Nenhum pedido para faturamento.")
            await crudLog.atualizar(id=self.log_id)
            return True

        print(f"{len(pedidos_faturar)} pedidos para faturar")

        for i, pedido in enumerate(pedidos_faturar):
            time.sleep(self.req_time_sleep)
            print(f"-> Pedido {i + 1}/{len(pedidos_faturar)}: {pedido.get("num_pedido")}")
            # Fatura pedido no Olist
            ack_pedido = await self.faturar_olist(pedido=pedido)
            # print(ack_pedido)
            await crudLogPed.criar(log_id=self.log_id,
                                   pedido_id=pedido.get('id'),
                                   evento='F',
                                   sucesso=ack_pedido.get('success'),
                                   obs=ack_pedido.get('__exception__',None))           
        
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        print("--> FATURAMENTO DOS PEDIDOS NO OLIST CONCLUÍDO!")
        return status_log
    
    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def integrar_snk(self,**kwargs):

        loja_unica:bool=kwargs.get('loja_unica',False)
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto')) 

        # Busca os pedidos pendentes de faturamento
        print("-> Buscando os pedidos pendentes de faturamento...")
        pedidos_faturar = await crudPedido.buscar_faturar(ecommerce_id=self.dados_ecommerce.get('id'))
        if not pedidos_faturar:
            print("Nenhum pedido para faturamento.")
            await crudLog.atualizar(id=self.log_id)
            return True
        
        pedidos_faturar = list(set([p.get("nunota") for p in pedidos_faturar]))
        print(f"{len(pedidos_faturar)} pedidos para faturar")

        for i, pedido in enumerate(pedidos_faturar):
            time.sleep(self.req_time_sleep)
            print(f"-> Pedido {i + 1}/{len(pedidos_faturar)}: {pedido}")
            # Fatura pedido no Olist
            ack_pedido = await self.faturar_sankhya(pedido=pedido,loja_unica=loja_unica)
            await crudLogPed.criar(log_id=self.log_id,
                                   nunota=pedido,
                                   evento='F',
                                   sucesso=ack_pedido.get('success'),
                                   obs=ack_pedido.get('__exception__',None))               
        
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        print("--> FATURAMENTO DOS PEDIDOS NO SANKHYA CONCLUÍDO!")
        return status_log    
    
    @contexto
    @log_execucao
    @carrega_dados_empresa
    async def realizar_venda_interna(self,**kwargs):
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
        print(f"--> VENDA INTERNA FINALIZADA")        
        return status_log
    