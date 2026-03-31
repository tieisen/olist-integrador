import os, time
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from database.crud import nota as crudNota
from src.services.shopee import Pagamento as PagamentoShopee
from src.olist.financeiro import Receita as FinReceita, Despesa as FinDespesa
from src.parser.financeiro import Receita as ParseReceita, Despesa as ParseDespesa
from src.utils.decorador import carrega_dados_ecommerce, carrega_dados_empresa
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Receita:

    def __init__(self, empresaId:int, idLoja:int=None):
        self.id_loja:int = idLoja
        self.empresa_id:int = empresaId
        self.codemp:int = None
        self.log_id:int = None
        self.contexto:str = 'financeiro'
        self.dados_ecommerce:dict = None
        self.dados_empresa:dict = None
        self.req_time_sleep:float = float(os.getenv('REQ_TIME_SLEEP', 1.5))
        self.payload_lcto:dict = None
        self.payload_baixa:dict = None
        self.id_nota:int = None
        self.id_financeiro:int = None
        self.cod_pedido:str = None
        self.parse = ParseReceita()
        self.finOlist = FinReceita(empresa_id=self.empresa_id)
        self.data_vcto:datetime = None

    async def calcularFinanceiroBlzWeb(self,codPedido:str,vlrNota:float) -> dict:
        vlr_taxa:float = await self.parse.calculaComissaoBlzWeb(vlrPedido=vlrNota)
        return {
            "order_sn": codPedido,
            "amount_paid": vlrNota,
            "released_amount": round(vlrNota - vlr_taxa,2),
            "fee_blz": vlr_taxa
        }
    
    async def buscarContasShopee(self, ecommerceId:int=None, dias:int=0, dtFim:str=None) -> bool:
        
        dt_fim:datetime = datetime.now() if not dtFim else datetime.strptime(dtFim, '%Y-%m-%d')
        dt_inicio:datetime = (dt_fim - timedelta(days=dias))
        dados_contas:list[dict] = []

        pag_shopee = PagamentoShopee(empresaId=self.empresa_id,ecommerceId=ecommerceId)
        dados_contas = await pag_shopee.getIncomeDetail(dateFrom=dt_inicio.strftime('%Y-%m-%d'),dateTo=dt_fim.strftime('%Y-%m-%d'))
        if dados_contas:
            for conta in dados_contas:
                time.sleep(self.req_time_sleep)
                try:
                    ack = await crudNota.atualizarDadosContaShopee(codPedido=conta.get('order_sn'),dadosConta=conta)
                    if not ack:
                        logger.error("Erro ao atualizar dados da conta do pedido %s",conta.get('order_sn'))                    
                        logger.info("dadosConta: %s",conta)
                except Exception as e:
                    logger.error("Erro ao salvar dados da conta do pedido %s: %s",conta.get('order_sn'),str(e))
                    logger.info("dadosConta: %s",conta)
                    
        return True

    def calcularDataVctoShopee(self,dataBase:datetime) -> datetime:
        nova_data:datetime = None
        # 0=segunda ... 2=quarta
        alvo = 2
        dias_ate = (alvo - dataBase.weekday() + 7) % 7
        # if dias_ate == 0:
        #     dias_ate = 7
        nova_data = dataBase + timedelta(days=dias_ate)
        self.data_vcto = nova_data
        return nova_data
    
    def calcularDataVctoBlz(self,dataBase:datetime) -> datetime:
        nova_data:datetime = None
        nova_data = dataBase + relativedelta(months=1)
        nova_data = nova_data.replace(day=9)
        self.data_vcto = nova_data
        return nova_data
    
    @carrega_dados_ecommerce
    async def calcularVcto(self,dataBase:datetime=None) -> str:
        
        ecommerce:str=self.dados_ecommerce.get('nome').upper()
        data_vcto:datetime=None
        
        if not dataBase:
            dataBase = datetime.today()
        
        if 'SHOPEE' in str(ecommerce).upper():
            data_vcto = self.calcularDataVctoShopee(dataBase=dataBase)
            return data_vcto.strftime('%Y-%m-%d')
        elif 'BELEZA' in str(ecommerce).upper():
            data_vcto = self.calcularDataVctoBlz(dataBase=dataBase)
            return data_vcto.strftime('%Y-%m-%d')
        else:
            return ''

    @carrega_dados_ecommerce
    async def formatarPayloadLcto(self, dadosConta:dict) -> bool:
        
        dados_pagamento:dict=dadosConta.get('income_data')        
        id_cliente:int = dadosConta.get('id_cliente')
        id_categoria_financeiro:int = self.dados_ecommerce.get('id_categoria_financeiro')
        id_forma_recebimento:int = self.dados_ecommerce.get('id_forma_rec_padrao')
        vlr_titulo:float = dados_pagamento.get('amount_paid')
        cod_pedido:str = dados_pagamento.get('order_sn')
        dt_nf:str = dadosConta.get('dh_emissao').strftime('%Y-%m-%d') if dadosConta.get('dh_emissao') else ''
        dt_venc:str = await self.calcularVcto()
        num_documento:str = str(dadosConta.get('numero'))
        num_nf:str = str(dadosConta.get('numero'))
        self.id_nota = dadosConta.get('id_nota')
        
        if not all([id_cliente,id_categoria_financeiro,vlr_titulo,cod_pedido,dt_nf,dt_venc,num_documento,num_nf,id_forma_recebimento]):
            raise ValueError(f"Dados incompletos. id_cliente: {id_cliente}, id_categoria_financeiro: {id_categoria_financeiro}, vlr_titulo: {vlr_titulo}, cod_pedido: {cod_pedido}, dt_nf: {dt_nf}, dt_venc: {dt_venc}, num_documento: {num_documento}, num_nf: {num_nf}, id_forma_recebimento: {id_forma_recebimento}")
        
        self.payload_lcto = await self.parse.lancamento(dtNf=dt_nf,
                                                        dtVenc=dt_venc,
                                                        vlrTitulo=vlr_titulo,
                                                        numDocumento=num_documento,
                                                        numNf=num_nf,
                                                        codPedido=cod_pedido,
                                                        idCliente=id_cliente,
                                                        idCategoriaFinanceiro=id_categoria_financeiro,
                                                        idFormaRecebimento=id_forma_recebimento)
                                    
        if not self.payload_lcto:
            msg = f"Erro montar payload"
            raise Exception(msg)   
        
        return True

    @carrega_dados_ecommerce
    async def formatarPayloadBaixa(self, dadosConta:dict) -> bool:
        
        dados_pagamento:dict=dadosConta.get('income_data')
        id_categoria_financeiro:int = self.dados_ecommerce.get('id_categoria_financeiro')
        id_conta_destino:int = self.dados_ecommerce.get('id_conta_destino')
        vlr_pago:float = dados_pagamento.get('released_amount')
        dt_recebimento:str = datetime.now().strftime('%Y-%m-%d')
        self.id_financeiro = dadosConta.get('id_financeiro')
        self.cod_pedido = dadosConta['income_data'].get('order_sn')
        
        if not all([id_conta_destino,dt_recebimento,vlr_pago,id_categoria_financeiro]):
            raise ValueError("Dados incompletos.")
        
        self.payload_baixa = await self.parse.baixa(idContaDestino=id_conta_destino,
                                                    dtRecebimento=dt_recebimento,
                                                    vlrPago=vlr_pago,                                       
                                                    idCategoriaFinanceiro=id_categoria_financeiro)

        if not self.payload_baixa :
            msg = f"Erro montar payload"
            raise Exception(msg)
        
        return True

    async def validaValorRecebimento(self, dadosConta:dict) -> dict:
        
        if dadosConta['income_data'].get('released_amount') > 0:
            return True

        # se o valor liberado é menor que zero, o pedido foi devolvido
        if not await crudNota.atualizar(cod_pedido=dadosConta['income_data'].get('order_sn'),
                                        parcelado=False,
                                        dh_baixa_financeiro=datetime(2000,1,1)):
            msg = f"Erro ao atualizar contas a receber da nota"
            raise ValueError(msg)
        return False

    async def lancarConta(self,id_nota:int|None=None,payload:dict|None=None) -> bool:

        id_financeiro:int = None
        payload = self.payload_lcto if not payload else payload
        id_nota = self.id_nota if not id_nota else id_nota        
        
        id_financeiro = await self.finOlist.lancar(payload=payload)
        if not id_financeiro:
            msg = f"Erro ao lançar título a receber"
            raise Exception(msg)
        
        # Salva ID do financeiro
        if not await crudNota.atualizar(id_nota=id_nota,id_financeiro=id_financeiro,dh_baixa_financeiro=self.data_vcto):
            msg = f"Erro ao salvar ID do financeiro"
            raise Exception(msg)            
        
        return True

    async def baixarConta(self,idFinanceiro:int|None=None,codPedido:str|None=None,payload:dict|None=None) -> bool:

        payload = self.payload_baixa if not payload else payload
        idFinanceiro = self.id_financeiro if not idFinanceiro else idFinanceiro
        codPedido = self.cod_pedido if not codPedido else codPedido
        
        if not await self.finOlist.baixar(id=idFinanceiro,payload=payload):
            msg = f"Erro ao baixar contas a receber da nota"
            raise Exception(msg)
        
        if not await crudNota.atualizar(cod_pedido=codPedido,
                                        dh_baixa_financeiro=datetime.now(),
                                        parcelado=False):
            msg = f"Erro ao atualizar contas a receber da nota"
            raise Exception(msg)    

        self.id_financeiro = None
        self.payload_baixa = None                 
           
        return True
    
    async def validaPrecisaProcessar(self,listaNotas:list[dict]) -> list[dict]:
        
        listaNotasProcessar:list[dict]=[]
        for nota in listaNotas:            
            if await crudNota.buscaPendenteIncomeData(idNota=nota.get('id')):
                listaNotasProcessar.append(nota)
        return listaNotasProcessar
    
    async def processarNotas(self,listaNotas:list[dict]) -> bool:

        listaNotas = await self.validaPrecisaProcessar(listaNotas=listaNotas)
        for i, nota in enumerate(listaNotas):
            try:
                id_cliente:int = nota.get('cliente', {}).get('id')
                id_nota:int = nota.get('id')
                ecommerce_nome:str = nota.get('ecommerce', {}).get('nome')
                cod_pedido:str = nota.get('ecommerce', {}).get('numeroPedidoEcommerce')
                vlr_nota:float = round(nota.get('valor', 0) + nota.get('valorDesconto', 0), 2)
                dados_financeiro:dict = None
                
                if not all([id_cliente,id_nota,ecommerce_nome,cod_pedido,vlr_nota]):
                    raise ValueError("Dados incompletos.")
                
                if ('SHOPEE' in ecommerce_nome.upper()):
                    dados_financeiro = {
                        "order_sn": cod_pedido,
                        "amount_paid": vlr_nota,
                        "released_amount": 0.0,
                        "fee_shopee": 0.0
                    } 
                elif ('BELEZA' in ecommerce_nome.upper()):
                    dados_financeiro = await self.calcularFinanceiroBlzWeb(codPedido=cod_pedido,vlrNota=vlr_nota)                
                else:
                    dados_financeiro = {
                        "order_sn": cod_pedido,
                        "amount_paid": vlr_nota
                    }
                
                time.sleep(self.req_time_sleep)
                await crudNota.atualizar(id_nota=id_nota,
                                         id_cliente=id_cliente,
                                         income_data=dados_financeiro)
            except Exception as e:
                logger.error("Erro ao processar nota %s: %s",nota.get('numero'),str(e))
            finally:
                pass
        
        return True     
    
class Despesa:

    def __init__(self, empresaId:int, idLoja:int=None):
        self.id_loja:int = idLoja
        self.empresa_id:int = empresaId
        self.codemp:int = None
        self.log_id:int = None
        self.contexto:str = 'financeiro'
        self.dados_ecommerce:dict = None
        self.dados_empresa:dict = None
        self.req_time_sleep:float = float(os.getenv('REQ_TIME_SLEEP', 1.5))
        self.payload_lcto:dict = None
        self.payload_baixa:dict = None
        self.eh_frete:bool = False 
        self.id_nota:int = None
        self.id_financeiro:int = None
        self.parse = ParseDespesa()
        self.finDespesa = FinDespesa(empresa_id=self.empresa_id)
        self.data_vcto:datetime = None        

    def calcularDataVctoShopee(self,dataBase:datetime) -> datetime:
        nova_data:datetime = None
        # 0=segunda ... 2=quarta
        alvo = 2
        dias_ate = (alvo - dataBase.weekday() + 7) % 7
        # if dias_ate == 0:
        #     dias_ate = 7
        nova_data = dataBase + timedelta(days=dias_ate)
        self.data_vcto = nova_data        
        return nova_data
    
    def calcularDataVctoBlz(self,dataBase:datetime) -> datetime:
        nova_data:datetime = None
        nova_data = dataBase + relativedelta(months=1)
        nova_data = nova_data.replace(day=9)
        self.data_vcto = nova_data
        return nova_data
    
    @carrega_dados_ecommerce
    async def calcularVcto(self,dataBase:datetime=None) -> str:
        
        ecommerce:str=self.dados_ecommerce.get('nome').upper()
        data_vcto:datetime=None
        
        if not dataBase:
            dataBase = datetime.today()
        
        if 'SHOPEE' in str(ecommerce).upper():
            data_vcto = self.calcularDataVctoShopee(dataBase=dataBase)
            return data_vcto.strftime('%Y-%m-%d')
        elif 'BELEZA' in str(ecommerce).upper():
            data_vcto = self.calcularDataVctoBlz(dataBase=dataBase)
            return data_vcto.strftime('%Y-%m-%d')
        else:
            return ''
    
    @carrega_dados_ecommerce
    async def buscarEstornoShopee(self, orderSn:str, ecommerceId:int=None) -> bool:
        
        escrow_details:list[dict] = []
        conta:dict = {}
        vlr_total_pedido:float = 0.0
        vlr_liquido:float = 0.0
        vlr_taxa:float = 0.0
        vlr_frete:float = 0.0
        motivo_estorno:str = None

        pag_shopee = PagamentoShopee(empresaId=self.empresa_id,ecommerceId=ecommerceId or self.dados_ecommerce.get('id'))
        escrow_details = await pag_shopee.getEscrowDetail(orderSn=orderSn)
        conta = escrow_details.get('response',{}).get('order_income',{})
        if not conta.get('order_adjustment'):
            return False

        vlr_total_pedido = conta.get('buyer_total_amount',0) - conta.get('buyer_transaction_fee',0)
        vlr_liquido = conta.get('escrow_amount_after_adjustment',0)
        vlr_frete = conta.get('actual_shipping_fee',0)
        vlr_taxa = round(vlr_total_pedido - vlr_liquido - vlr_frete,2)
        motivo_estorno = conta.get('order_adjustment',{}).get('adjustment_reason')

        try:
            ack = await crudNota.atualizarDadosContaEstornoShopee(codPedido=orderSn,
                                                                  vlrLiquido=vlr_liquido,
                                                                  vlrFrete=vlr_frete,
                                                                  vlrTaxa=vlr_taxa,
                                                                  motivoEstorno=motivo_estorno)
            if not ack:
                logger.error("Erro ao atualizar dados do estorno do pedido %s",conta.get('order_sn'))                    
                logger.info("dadosConta: %s",conta)
        except Exception as e:
            logger.error("Erro ao salvar dados do estorno do pedido %s: %s",conta.get('order_sn'),str(e))
            logger.info("dadosConta: %s",conta)
                    
        return True

    def validaDespesaFrete(self,dadosConta:dict):
        self.eh_frete = True if ('fee_frete' in dadosConta.get('income_data',{})) and (not dadosConta.get('id_financeiro_frete')) and (not dadosConta.get('income_data',{}).get('id_financeiro')) else False
        return        

    @carrega_dados_ecommerce
    @carrega_dados_empresa
    async def formatarPayloadLcto(self, dadosConta:dict|None=None, dadosTransferencia:dict|None=None) -> bool:
        """_summary_

        Args:
            dadosConta (dict | None): _description_
            dadosTransferencia (dict | None): _description_

        Raises:
            ValueError: _description_
            ValueError: _description_
            Exception: _description_

        Returns:
            bool: _description_
        """
        
        if not any([dadosConta,dadosTransferencia]):
            raise ValueError("Dados incompletos")

        if isinstance(dadosConta,list):
            dadosConta = dadosConta[0]
            if not isinstance(dadosConta,dict):
                raise ValueError("Dados da conta devem ser um dicionário")

        if isinstance(dadosTransferencia,list):
            dadosTransferencia = dadosTransferencia[0]
            if not isinstance(dadosTransferencia,dict):
                raise ValueError("Dados da transferência devem ser um dicionário")        
        
        if dadosConta:
            
            dados_pagamento:dict=dadosConta.get('income_data')
            vlr_titulo:float=0
            id_categoria_despesa:int=0
            historico:str=''
            cod_pedido:str = dados_pagamento.get('order_sn')
            self.validaDespesaFrete(dadosConta=dadosConta)
            
            if self.eh_frete:
                vlr_titulo = dados_pagamento.get('fee_frete')
                id_categoria_despesa = self.dados_empresa.get('olist_id_categoria_frete_padrao')
                historico = f"Taxa ref. a devolução do Pedido #{cod_pedido}"
            elif 'fee_shopee' in dados_pagamento:
                vlr_titulo = dados_pagamento.get('fee_shopee')
                id_categoria_despesa = self.dados_empresa.get('olist_id_categoria_taxa_padrao')
                historico = f"Taxa do e-commerce || Ref. ao Pedido #{cod_pedido}"
            elif 'fee_blz' in dados_pagamento:
                vlr_titulo = dados_pagamento.get('fee_blz')
                id_categoria_despesa = self.dados_empresa.get('olist_id_categoria_taxa_padrao')
                historico = f"Taxa do e-commerce || Ref. ao Pedido #{cod_pedido}"
            else:
                vlr_titulo = dados_pagamento.get('amount_paid')
            
            if dados_pagamento.get('adjustment_reason'):
                historico += f" || Motivo do ajuste: {dados_pagamento.get('adjustment_reason')}"
            
            dt_neg:str = dadosConta.get('dh_emissao').strftime('%Y-%m-%d') if dadosConta.get('dh_emissao') else ''
            dt_venc:str = await self.calcularVcto()
            num_documento:str = str(dadosConta.get('numero'))
            id_fornecedor:int = self.dados_ecommerce.get('id_fornecedor_olist')
            id_forma_pgto:int = self.dados_ecommerce.get('id_forma_pgto_padrao')
            self.id_nota = dadosConta.get('id_nota')
        
        if dadosTransferencia:
            dtneg:datetime = datetime.strptime(dadosTransferencia.get('dtneg'),'%d/%m/%Y')
            vlr_titulo:float = dadosTransferencia.get('vlrnota')
            dt_neg:str = dtneg.strftime('%Y-%m-%d')
            dt_venc:str = (dtneg + timedelta(days=30)).strftime('%Y-%m-%d')
            num_documento:str = str(dadosTransferencia.get('numnota'))
            id_fornecedor:int = self.dados_empresa.get('olist_id_fornecedor_padrao')
            id_categoria_despesa:int = self.dados_empresa.get('olist_id_categoria_despesa_padrao')
            id_forma_pgto:int = self.dados_ecommerce.get('id_forma_pgto_padrao')
            historico:str = f"Ref. NF nº {dadosTransferencia.get('numnota')}. {dadosTransferencia.get('observacao')}"
            self.id_nota = -1
        
        if not all([dt_neg,dt_venc,vlr_titulo,num_documento,id_fornecedor,id_categoria_despesa,historico,id_forma_pgto]):
            raise ValueError(f"Dados incompletos. dt_neg: {dt_neg}, dt_venc: {dt_venc}, vlr_titulo: {vlr_titulo}, num_documento: {num_documento}, id_fornecedor: {id_fornecedor}, id_categoria_despesa: {id_categoria_despesa}, historico: {historico}, id_forma_pgto: {id_forma_pgto}")
        
        self.payload_lcto = await self.parse.lancamento(dtNeg=dt_neg,
                                                        dtVcto=dt_venc,
                                                        valor=vlr_titulo,
                                                        numDocumento=num_documento,
                                                        historico=historico,
                                                        idFornecedor=id_fornecedor,
                                                        idCategoriaDespesa=id_categoria_despesa,
                                                        idFormaPgto=id_forma_pgto)
                                    
        if not self.payload_lcto:
            msg = f"Erro montar payload"
            raise Exception(msg)   
        
        return True

    @carrega_dados_ecommerce
    async def formatarPayloadBaixa(self, dadosConta:dict|None=None, dadosTransferencia:dict|None=None) -> bool:
        
        if not any([dadosConta,dadosTransferencia]):
            raise ValueError("Dados incompletos")
        
        id_categoria_despesa:int = None
        id_conta_destino:int = None
        vlr_pago:float = None
        dt_recebimento:str = None
        
        if dadosConta:
            dados_pagamento:dict=dadosConta.get('income_data')
            id_categoria_despesa = self.dados_ecommerce.get('olist_id_categoria_taxa_padrao')
            id_conta_destino = self.dados_ecommerce.get('id_conta_destino')
            vlr_pago = dados_pagamento.get('released_amount')
            dt_recebimento = datetime.now().strftime('%Y-%m-%d')
            self.id_financeiro = dadosConta.get('id_financeiro')
            
        if dadosTransferencia:
            id_categoria_despesa = self.dados_ecommerce.get('olist_id_categoria_despesa_padrao')
            id_conta_destino = self.dados_ecommerce.get('id_conta_destino')
            vlr_pago = dadosTransferencia.get('vlrnota')
            dt_recebimento = datetime.now().strftime('%Y-%m-%d')

        if not all([id_conta_destino,dt_recebimento,vlr_pago,id_categoria_despesa]):
            raise ValueError("Dados incompletos")
        
        self.payload_baixa = await self.parse.baixa(idContaDestino=id_conta_destino,
                                                    dtRecebimento=dt_recebimento,
                                                    vlrPago=vlr_pago,
                                                    idCategoriaDespesa=id_categoria_despesa)

        if not self.payload_baixa:
            msg = f"Erro montar payload"
            raise Exception(msg)
        
        return True

    @carrega_dados_ecommerce
    async def lancarConta(self,id_nota:int|None=None,payload:dict|None=None) -> bool:

        id_financeiro:int = None
        payload = self.payload_lcto if not payload else payload
        id_nota = self.id_nota if not id_nota else id_nota

        if not all([payload,id_nota]):
            raise ValueError("Dados incompletos")
        
        id_financeiro = await self.finDespesa.lancar(payload=payload)
        if not id_financeiro:
            msg = f"Erro ao lançar título a pagar"
            raise Exception(msg)
        
        if self.eh_frete:
            # logger.info(f"Salvando ID do financeiro do frete: {id_financeiro}")
            if not await crudNota.atualizar(id_nota=id_nota,id_financeiro_frete=id_financeiro):
                msg = f"Erro ao salvar ID do financeiro do frete"
                raise Exception(msg)        
        elif id_nota != -1:            
            # logger.info(f"Salvando ID do financeiro da taxa: {id_financeiro}")
            if not await crudNota.atualizar(id_nota=id_nota,id_financeiro_taxa=id_financeiro,dh_baixa_financeiro=self.data_vcto):
                msg = f"Erro ao salvar ID do financeiro da taxa"
                raise Exception(msg)

        self.payload_lcto = None
        self.id_nota = None
        self.eh_frete = False

        return True

    @carrega_dados_ecommerce
    async def ignorarTaxa(self,id_nota:int) -> bool:

        id_nota = self.id_nota if not id_nota else id_nota
        if await crudNota.atualizar(id_nota=id_nota,id_financeiro_taxa=0):
            msg = f"Erro ao ignorar taxa da nota"
            raise Exception(msg)              
        
        return True

    @carrega_dados_ecommerce
    async def baixarConta(self,idFinanceiro:int|None=None,payload:dict|None=None) -> bool:

        payload = self.payload_baixa if not payload else payload
        idFinanceiro = self.id_financeiro if not idFinanceiro else idFinanceiro
        
        if not all([idFinanceiro,payload]):
            raise ValueError("Dados incompletos")
        
        if not await self.finDespesa.baixar(id=idFinanceiro,payload=payload):
            msg = f"Erro ao baixar contas a receber da nota"
            raise Exception(msg)
        
        self.id_financeiro = None
        self.payload_baixa = None       
           
        return True
    