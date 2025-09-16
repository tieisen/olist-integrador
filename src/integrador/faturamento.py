import os
import time
import logging
from dotenv import load_dotenv
from datetime import datetime

from src.sankhya.faturamento       import Faturamento   as FaturamentoSnk
from src.sankhya.estoque           import Estoque       as EstoqueSnk
from src.sankhya.transferencia     import Transferencia as TransferenciaSnk
from src.sankhya.transferencia     import Itens         as ItemTransfSnk
from src.sankhya.pedido            import Pedido        as PedidoSnk
from src.sankhya.nota              import Nota          as NotaSnk
from src.parser.transferencia      import Transferencia as ParserTransferencia
from src.olist.pedido              import Pedido        as PedidoOlist
from src.olist.nota                import Nota          as NotaOlist
from src.olist.separacao           import Separacao     as SeparacaoOlist
from database.crud                 import log           as crudLog
from database.crud                 import log_pedido    as crudLogPed
from database.crud                 import pedido        as crudPedido
from database.crud                 import nota          as crudNota
from src.utils.log                 import Log
from src.utils.decorador.contexto  import contexto
from src.utils.decorador.empresa   import ensure_dados_empresa
from src.utils.decorador.ecommerce import ensure_dados_ecommerce
from src.utils.decorador.log       import log_execucao


load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Faturamento:

    def __init__(self, codemp:int=None, empresa_id:int=None, id_loja:int=None):
        self.codemp = codemp
        self.empresa_id = empresa_id
        self.id_loja = id_loja
        self.log_id = None
        self.contexto = 'faturamento'
        self.dados_empresa = None
        self.dados_ecommerce = None
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))

    @contexto
    @log_execucao
    @ensure_dados_empresa
    async def venda_entre_empresas_em_lote(self,**kwargs):
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
                logger.error("Erro ao buscar itens conferidos no dia.")
                raise Exception("Erro ao buscar itens conferidos no dia.")            
            
            # Busca saldo de estoque
            print("-> Buscando saldo de estoque...")
            estoque = EstoqueSnk(codemp=self.codemp)
            saldo_estoque = await estoque.buscar_saldo_por_lote(lista_produtos=saldo_pedidos)
            if not saldo_estoque:
                logger.error("Erro ao buscar saldo de estoque.")
                raise Exception("Erro ao buscar saldo de estoque.")

            # Compara quantidade conferida com estoque disponível
            print("Comparando quantidade conferida com estoque disponível...")
            itens_venda_interna = await faturamento.compara_saldos(saldo_estoque=saldo_estoque,
                                                                saldo_pedidos=saldo_pedidos)
            if not itens_venda_interna:
                logger.info("Nenhum item para lançar venda interna.")
                raise Exception("Nenhum item para lançar venda interna.")

            # Busca valor de tranferência dos itens
            print("Buscando valores de transferência...")
            item_transf = ItemTransfSnk(codemp=self.codemp)
            codigos_produtos = [item.get('codprod') for item in itens_venda_interna]
            valores_produtos = await item_transf.busca_valor_transferencia(lista_itens=codigos_produtos)
            if not valores_produtos:
                logger.error("Erro ao buscar valores de transferência.")
                raise Exception("Erro ao buscar valores de transferência.")

            # Vincula o valor de transferência o respectivo produto
            print("Vinculando valores aos produtos...")
            for item in itens_venda_interna:
                for valor in valores_produtos:
                    if item.get('codprod') == valor.get('codprod'):
                        item['valor'] = float(valor.get('valor')) if valor.get('valor') else 0.1
                        break

            # Converte para o formato da API Sankhya
            print("Convertendo para o formato da API Sankhya...")
            parser = ParserTransferencia(codemp=self.codemp)
            cabecalho, itens = parser.to_sankhya(objeto='nota',
                                                 itens_transferencia=itens_venda_interna,
                                                 itens_transferidos=[])
            if not all([cabecalho, itens]):
                logger.error("Erro ao preparar dados da nota de transferência.")
                raise Exception("Erro ao preparar dados da nota de transferência.")

            # Lança nota de transferência
            print("Lançando nota de transferência...")
            transferencia = TransferenciaSnk(codemp=self.codemp)                    
            ack, nunota = await transferencia.criar(cabecalho=cabecalho,
                                                    itens=itens)        
            if not ack:
                logger.error("Erro ao lançar nota de transferência.")
                raise Exception("Erro ao lançar nota de transferência.")
            
            # Confirma nota de transferência
            print(f"Confirmando nota de transferência {nunota}...")
            ack = await transferencia.confirmar(nunota=nunota)
            if not ack:
                logger.error("Erro ao confirmar nota de transferência.")
                raise Exception("Erro ao confirmar nota de transferência.")
            await crudLog.atualizar(id=self.log_id)            
            print("--> VENDA ENTRE EMPRESAS REALIZADA COM SUCESSO!")
            return True        
        except Exception as e:
            obs = f"{e}"
            await crudLogPed.criar(log_id=self.log_id,
                                   pedido_id=0,
                                   evento='F',
                                   status=False,
                                   obs=obs)            
            await crudLog.atualizar(id=self.log_id,
                                    sucesso=False)
            return False

    @contexto
    @log_execucao
    @ensure_dados_empresa
    async def venda_entre_empresas_por_item(self, nunota:int=None, **kwargs):

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
                logger.error("Erro ao buscar itens conferidos no dia.")
                raise Exception("Erro ao buscar itens conferidos no dia.")            
            
            # Busca saldo de estoque
            print("-> Buscando saldo de estoque...")
            estoque = EstoqueSnk(codemp=self.codemp)
            saldo_estoque = await estoque.buscar_saldo_por_lote(lista_produtos=saldo_pedidos)
            if not saldo_estoque:
                logger.error("Erro ao buscar saldo de estoque.")
                raise Exception("Erro ao buscar saldo de estoque.")

            # Compara quantidade conferida com estoque disponível
            print("-> Comparando quantidade conferida com estoque disponível...")
            itens_venda_interna = await faturamento.compara_saldos(saldo_estoque=saldo_estoque,
                                                                saldo_pedidos=saldo_pedidos)
            if not itens_venda_interna:
                logger.info("Nenhum item para lançar venda interna.")
                raise Exception("Nenhum item para lançar venda interna.")

            # Busca valor de tranferência dos itens
            print("-> Buscando valores de transferência...")
            item_transf = ItemTransfSnk(codemp=self.codemp)
            codigos_produtos = [item.get('codprod') for item in itens_venda_interna]
            valores_produtos = await item_transf.busca_valor_transferencia(lista_itens=codigos_produtos)
            if not valores_produtos:
                logger.error("Erro ao buscar valores de transferência.")
                raise Exception("Erro ao buscar valores de transferência.")
            
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
                logger.error("Erro ao buscar dados da nota de transferência.")
                raise Exception("Erro ao buscar dados da nota de transferência.")            
            if dados_nota_transferencia:
                nunota = dados_nota_transferencia.get('nunota')
                print(f"Nota encontrada: {nunota}")
            else:
                print("Nenhuma nota de transferência encontrada.")

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
                logger.error("Erro ao lançar/atualizar nota de transferência.")
                raise Exception("Erro ao lançar/atualizar nota de transferência.")
            
            await crudLog.atualizar(id=self.log_id)
            print(f"--> NOTA DE TRANSFERÊNCIA {nunota} LANÇADA/ATUALIZADA COM SUCESSO!")
            return True
        except Exception as e:
            obs = f"{e}"
            await crudLogPed.criar(log_id=self.log_id,
                                   pedido_id=0,
                                   evento='F',
                                   status=False,
                                   obs=obs)            
            await crudLog.atualizar(id=self.log_id,
                                    sucesso=False)
            return False        

    @contexto
    @ensure_dados_ecommerce
    async def faturar_olist(
            self,
            pedido:dict,
            **kwargs
        ) -> bool:

        pedido_olist = PedidoOlist(codemp=self.codemp)
        nota_olist = NotaOlist(id_loja=self.id_loja)
        separacao = SeparacaoOlist(codemp=self.codemp)
        
        dados_nota_olist:dict=None
        dados_emissao_nf_olist:dict=None

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                        de='sankhya',
                                        para='olist',
                                        contexto=kwargs.get('_contexto'))

        try:
            # Cria NF no Olist
            print("-> Criando NF no Olist...")
            dados_nota_olist = await pedido_olist.gerar_nf(id=pedido.get('id_pedido'))
            if not dados_nota_olist:
                raise Exception(f"Erro ao gerar NF do pedido {pedido.get('num_pedido')}")
            else:
                await crudNota.criar(id_pedido=pedido.get('id_pedido'),
                                     id_nota=dados_nota_olist.get('id'),
                                     numero=dados_nota_olist.get('numero'),
                                     serie=dados_nota_olist.get('serie'))
                
            # Autoriza NF na Sefaz
            print(f"-> Autorizando NF {dados_nota_olist.get('numero')} na Sefaz...")                
            dados_emissao_nf_olist = await nota_olist.emitir(id=dados_nota_olist.get('id'))
            if not dados_emissao_nf_olist:
                raise Exception(f"Erro ao emitir nota {dados_nota_olist.get('numero')} ref. pedido {pedido.get('num_pedido')}")
            else:
                await crudNota.atualizar(id_nota=dados_nota_olist.get('id'),
                                         chave_acesso=dados_emissao_nf_olist.get('chaveAcesso'))            
            
            # Envia pedido pra separação no Olist
            print("-> Enviando pedido para separação no Olist...")
            ack_separacao = await separacao.separar(id=pedido.get('id_separacao'))
            if not ack_separacao:
                raise Exception(f"Erro ao separar pedido {pedido.get('num_pedido')}.")
            
            # Baixa contas a receber no Olist
            print("-> Baixando contas a receber no Olist...")
            dados_financeiro = await nota_olist.buscar_financeiro(serie=dados_nota_olist.get('serie'),
                                                                  numero=str(dados_nota_olist.get('numero')).zfill(6))
            if not dados_financeiro:
                raise Exception(f"Erro ao buscar financeiro da nota {dados_nota_olist.get('numero')} no Olist")
            else:
                await crudNota.atualizar(id_nota=dados_nota_olist.get('id'),
                                         id_financeiro=dados_financeiro.get('id'))                
            
            ack_financeiro = await nota_olist.baixar_financeiro(id=dados_financeiro.get('id'),
                                                                valor=dados_financeiro.get('valor'))
            if ack_financeiro is False:
                raise Exception(f"Erro ao baixar financeiro da nota {dados_nota_olist.get('numero')} no Olist")
            else:
                if ack_financeiro is None:
                    print(f"Financeiro da nota já está baixado no Olist")                
                await crudNota.atualizar(id_nota=dados_nota_olist.get('id'),
                                         dh_baixa_financeiro=datetime.now())
            
            print(f"-> Faturamento do pedido {pedido.get('num_pedido')} concluído!")    
        except Exception as e:
            obs = f"{e}"
            logger.error(e)
            await crudLogPed.criar(log_id=self.log_id,
                                   pedido_id=pedido.get('id_pedido'),
                                   evento='F',
                                   status=False,
                                   obs=obs) 
            return False

    @contexto
    async def faturar_sankhya(
            self,
            pedido:int,
            **kwargs
        ) -> bool:
            
        pedido_snk = PedidoSnk(codemp=self.codemp)
        nota_snk = NotaSnk(codemp=self.codemp)

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                              de='olist',
                                              para='sankhya',
                                              contexto=kwargs.get('_contexto'))

        try:
            # Verifica se o pedido foi faturado ou trancou
            print("-> Verificando se o pedido foi faturado...")
            nunota = pedido
            status_faturamento = await pedido_snk.buscar_nunota_nota(nunota=nunota)
            if status_faturamento:
                nunota_nota = status_faturamento[0].get('nunota')
                print(f"Pedido {nunota} já foi faturado na nota de venda {nunota_nota}.")
                # Atualiza base de dados
                ack = crudNota.atualizar(nunota_pedido=nunota,
                                         nunota=nunota_nota)
            else:
                # Se o pedido não foi faturado...
                # Faz a nota de transferência
                print("-> Gerando a nota de transferência...")
                ack = await self.venda_entre_empresas_em_lote()
                if not ack:
                    msg = "Erro ao gerar a nota de transferência."
                    raise Exception(msg)

                # Fatura no Sankhya
                print(f"-> Faturando pedido {nunota} no Sankhya...")
                ack, nunota_nota = await pedido_snk.faturar(nunota=nunota)
                if ack:
                    print(f"Pedido {nunota} faturado na nota de venda {nunota_nota}")
                else:
                    msg = f"Erro ao faturar pedido {nunota}"
                    raise Exception(msg)

            # Confirma nota no Sankhya
            print(f"-> Confirmando nota de venda {nunota_nota} no Sankhya...")
            ack = await nota_snk.confirmar(nunota=nunota_nota)
            if ack is False:
                msg = f"Erro ao confirmar nota {nunota_nota}"
                raise Exception(msg)
            if ack is None:
                print(f"Nota de venda {nunota_nota} já foi confirmada")
            if ack:
                pass
            # Atualiza base de dados
            ack = crudNota.atualizar(nunota_pedido=nunota,
                                        dh_confirmacao=datetime.now())
            print(f"-> Faturamento do pedido {nunota} concluído!")
            return True
        except Exception as e:
            obs = f"{e}"
            logger.error(e)
            await crudLogPed.criar(log_id=self.log_id,
                                   pedido_id=pedido.get('id_pedido'),
                                   evento='F',
                                   status=False,
                                   obs=obs) 
            return False

    @contexto
    @log_execucao
    @ensure_dados_ecommerce
    async def integrar_olist(self,**kwargs):
        self.log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                          de='sankhya',
                                          para='olist',
                                          contexto=kwargs.get('_contexto')) 

        # Busca os pedidos pendentes de faturamento
        print("-> Buscando os pedidos pendentes de faturamento...")
        pedidos_faturar = crudPedido.buscar_faturar(ecommerce_id=self.dados_ecommerce.get('id'))
        if not pedidos_faturar:
            print("Nenhum pedido para faturamento.")
            await crudLog.atualizar(id=self.log_id)
            return True

        print(f"{len(pedidos_faturar)} pedidos para faturar")

        for i, pedido in enumerate(pedidos_faturar):
            time.sleep(self.req_time_sleep)
            print(f"-> Pedido {i + 1}/{len(pedidos_faturar)}: {pedido.get("num_pedido")}")            
            # Fatura pedido no Olist
            ack = await self.faturar_olist(pedido=pedido)
        
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        print("--> FATURAMENTO DOS PEDIDOS NO OLIST CONCLUÍDO!")
        return True
    
    @contexto
    @log_execucao
    @ensure_dados_ecommerce
    async def integrar_snk(self,**kwargs):
        self.log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto')) 

        # Busca os pedidos pendentes de faturamento
        print("-> Buscando os pedidos pendentes de faturamento...")
        pedidos_faturar = crudPedido.buscar_faturar(ecommerce_id=self.dados_ecommerce.get('id'))
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
            ack = await self.faturar_sankhya(pedido=pedido)
        
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        print("--> FATURAMENTO DOS PEDIDOS NO SANKHYA CONCLUÍDO!")
        return True
    