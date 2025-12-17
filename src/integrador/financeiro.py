import os
import time
from datetime import datetime, timedelta
from database.crud import log as crudLog
from database.crud import nota as crudNota
from src.services.bot import Bot
from src.olist.financeiro import Financeiro as FinOlist
from src.sankhya.nota import Nota as NotaSnk
from src.parser.financeiro import Financeiro as ParseFin
from src.utils.decorador import contexto, carrega_dados_ecommerce, carrega_dados_empresa, log_execucao
from src.utils.log import set_logger
from src.utils.load_env import load_env
from src.utils.buscar_arquivo import buscar_relatorio_custos
load_env()
logger = set_logger(__name__)

class Financeiro:

    def __init__(self, id_loja:int, empresa_id:int):
        self.id_loja = id_loja
        self.empresa_id = empresa_id
        self.codemp = None
        self.log_id = None
        self.contexto = 'financeiro'
        self.dados_ecommerce:dict = None
        self.dados_empresa:dict = None
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))

    @contexto
    @carrega_dados_ecommerce
    async def baixar_conta_liquido(self,data_baixa:datetime,dados_conta:dict,dados_custo:dict,**kwargs) -> dict:
        """
        Faz a baixa do contas a receber referente à NF no Olist com base no relatório de custos
            :param data_baixa: data da baixa
            :param dados_conta: dicionário com os dados do lançamento do contas a receber
            :param dados_custo: dicionário com os dados da planilha de custos do e-commerce
            :return dict: dicionário com status e erro
        """

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='olist',
                                              para='olist',
                                              contexto=kwargs.get('_contexto'))
        try:
            parse = ParseFin()
            finOlist = FinOlist(id_loja=self.id_loja,empresa_id=self.dados_ecommerce.get('empresa_id'))
            payload = {}
            payload = parse.olist_receber(dados_ecommerce=self.dados_ecommerce,
                                          dados_conta=dados_conta,
                                          dados_custo=dados_custo,
                                          data=data_baixa.strftime('%d/%m/%Y'))
            if not payload:
                msg = f"Erro montar payload"
                raise Exception(msg)
            
            if not await finOlist.baixar_receber(id=dados_conta.get('id'),payload=payload):
                msg = f"Erro ao baixar contas a receber da nota"
                raise Exception(msg)
            
            if not await crudNota.atualizar(cod_pedido=dados_custo.get('n_pedido_ecommerce'),dh_baixa_financeiro=datetime.now(),parcelado=False):
                msg = f"Erro ao atualizar contas a receber da nota"
                raise Exception(msg)
            
            return {"success": True, "__exception__": None}
        except Exception as e:
            logger.error("Erro ao baixar conta: %s",str(e))
            return {"success": False, "__exception__": str(e)}  

    @carrega_dados_ecommerce
    @carrega_dados_empresa
    async def lancar_conta_pagar_olist(self, nunota_nota:int) -> bool:

        def calcula_vcto(dtNeg:str):
            nonlocal dtVcto
            try:
                dtVcto = (datetime.strptime(dtNeg,'%d/%m/%Y') + timedelta(days=30)).strftime('%d/%m/%Y')
            except Exception as e:
                logger.error(f"Erro ao calcular data de vencimento do contas a pagar: {e}")

        def formata_data(data:str) -> str:
            return datetime.strptime(data,'%d/%m/%Y').strftime('%Y-%m-%d')

        notaSnk = NotaSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        finOlist = FinOlist(id_loja=self.id_loja,empresa_id=self.dados_ecommerce.get('empresa_id'))
        parseFin = ParseFin()
        dtVcto:str=''

        try:
            dados_nota_transferencia:dict = await notaSnk.buscar(nunota=nunota_nota)
            if isinstance(dados_nota_transferencia,list):
                dados_nota_transferencia = dados_nota_transferencia[0]
            if not dados_nota_transferencia:
                raise Exception(f"Erro ao buscar dados da nota de transferência {nunota_nota}")                
            
            calcula_vcto(dados_nota_transferencia.get('dtneg'))
            payload:dict={}
            try:
                payload = parseFin.olist_pagar(dtNeg=formata_data(dados_nota_transferencia.get('dtneg')),
                                               dtVcto=formata_data(dtVcto),
                                               valor=dados_nota_transferencia.get('vlrnota'),
                                               numDocumento=dados_nota_transferencia.get('numnota'),
                                               historico=f"Ref. NF nº {dados_nota_transferencia.get('numnota')}. {dados_nota_transferencia.get('observacao')}",
                                               dados_empresa=self.dados_empresa)
            except Exception as e:
                logger.error(f"Erro no parser: {e}")
                print(f"Erro no parser: {e}")
            finally:
                pass
            if not payload:
                raise Exception(f"Erro ao montar payload")

            ack = await finOlist.lancar_pagar(payload=payload)
            if not ack:
                raise Exception(f"Erro ao lançar conta a pagar")

            return True
        except Exception as e:
            logger.error(str(e))
            return False

    @contexto
    @carrega_dados_ecommerce
    async def agrupar_titulos_parcelados(self,**kwargs) -> bool:
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='olist',
                                          contexto=kwargs.get('_contexto'))
        dados_notas_parceladas:list[dict] = await crudNota.buscar_financeiro_parcelado(ecommerce_id=self.dados_ecommerce.get('id'))
        try:
            if dados_notas_parceladas:        
                notas_parceladas:list[dict] = [{"numero":n.get('numero'),"id":n.get('id_nota')} for n in dados_notas_parceladas]
                if not notas_parceladas:
                    raise Exception("Nenhuma nota parcelada encontrada para agrupar títulos.")
                bot = Bot(empresa_id=self.dados_ecommerce.get('empresa_id'))
                if not await bot.rotina_contas_receber(lista_notas=notas_parceladas):
                    raise Exception("Erro ao agrupar títulos parcelados via Bot.")
            await crudLog.atualizar(id=self.log_id,sucesso=True)
            return True                
        except Exception as e:
            logger.error("Erro ao agrupar títulos parcelados: %s",str(e))
            await crudLog.atualizar(id=self.log_id,sucesso=False)
            return False
    
    @contexto
    @carrega_dados_ecommerce
    async def realizar_baixa_contas_receber(self,data_conta:datetime,relatorio_custos:list[dict],**kwargs) -> bool:

        def filtra_loja(contas_dia:list[dict],id_loja:int) -> list[dict]:
            if id_loja in [9227,9265]:
                return [conta for conta in contas_dia if not str.strip(conta['cliente'].get('email'))]
            elif id_loja == 10940:
                return [conta for conta in contas_dia if str.strip(conta['cliente'].get('email'))]
            else:
                return []

        import re
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))        
        REGEX = r"OC nº (\S+)"

        fin_olist = FinOlist(id_loja=self.id_loja,empresa_id=self.dados_ecommerce.get('empresa_id'))
        contas_dia:list[dict] = await fin_olist.buscar_lista_receber_aberto(dt_emissao=data_conta.strftime('%Y-%m-%d'))
        contas_dia = filtra_loja(contas_dia=contas_dia,id_loja=self.id_loja)
        if not contas_dia:
            await crudLog.atualizar(id=self.log_id,sucesso=True)
            return True
        status_log:list[bool] = []
        for conta in contas_dia:            
            try:
                matches = re.search(REGEX, conta.get('historico'))
                codigo_pedido:str=matches.group(1)
                custo:dict = next((r for r in relatorio_custos if r.get("pedido_ecommerce") == codigo_pedido), None)
                status = await self.baixar_conta_liquido(data_baixa=data_conta,dados_conta=conta,dados_custo=custo)
                status_log.append(status.get('success'))
            except Exception as e:
                logger.error("Erro ao baixar conta a receber do pedido %s: %s",codigo_pedido,str(e))
                status_log.append(False)
            finally:
                time.sleep(self.req_time_sleep)
        await crudLog.atualizar(id=self.log_id,sucesso=all(status_log))
        return all(status_log)

    @contexto
    @carrega_dados_ecommerce
    async def baixar_contas_receber_shopee(self,relatorio_recebimentos:list[dict],**kwargs) -> bool:

        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='olist',
                                          contexto=kwargs.get('_contexto'))          
        
        fin_olist = FinOlist(id_loja=self.id_loja,empresa_id=self.dados_ecommerce.get('empresa_id'))
        parse = ParseFin()
        res:list[bool] = []
        dados_nota:dict={}
        dados_conta:dict={}
        payload:dict={}

        for i, recebimento in enumerate(relatorio_recebimentos):
            print(f"{i+1}/{len(relatorio_recebimentos)}: Pedido {recebimento.get('id_do_pedido')}")
            try:
                print("Buscando dados da nota...")
                dados_nota = await crudNota.buscar(cod_pedido=recebimento.get('id_do_pedido'))
                if not dados_nota:
                    msg = f"Erro ao buscar dados da nota"
                    raise ValueError(msg)
                print("Buscando dados da conta...")
                dados_conta = await fin_olist.buscar_receber(id=dados_nota.get('id_financeiro'))
                if not dados_conta:
                    msg = f"Erro ao buscar dados da conta"
                    raise ValueError(msg)
                if dados_conta.get('situacao') == 'pago':
                    print("Conta já foi paga.")
                    res.append(True)
                    continue
                print("Montando payload...")
                payload = parse.olist_receber_shopee(dados_ecommerce=self.dados_ecommerce,dados_conta=dados_conta,dados_recebimento=recebimento)
                if not payload:
                    msg = f"Erro ao montar payload"
                    raise ValueError(msg)
                print("Baixando contas a receber...")
                time.sleep(self.req_time_sleep)
                if not await fin_olist.baixar_receber(id=dados_conta.get('id'),payload=payload):
                    msg = f"Erro ao baixar contas a receber"
                    raise Exception(msg)
                print("Atualizando base...")
                if not await crudNota.atualizar(cod_pedido=recebimento.get('id_do_pedido'),parcelado=False,
                                                dh_baixa_financeiro=datetime.strptime(recebimento.get('data'),'%Y-%m-%d %H:%M:%S')):
                    msg = f"Erro ao atualizar contas a receber da nota"
                    raise Exception(msg)
                res.append(True)
            except Exception as e:
                dados_nota={}
                dados_conta={}
                payload={}                
                res.append(False)
                msg = f"Erro ao baixar contas a receber do pedido {recebimento.get('id_do_pedido')}: {str(e)}"
                logger.error(msg)
            finally:
                print("")
                time.sleep(self.req_time_sleep)
        
        return all(res)
    
    @log_execucao
    async def executar_baixa(self,data:datetime) -> bool:
        parser = ParseFin()
        relatorio_custos:list[dict] = buscar_relatorio_custos()
        if not relatorio_custos:
            logger.error("Relatório de custos não encontrado ou vazio.")
            return False
        relatorio_custos = parser.carregar_relatorio_olist(lista=relatorio_custos)
        if not relatorio_custos:
            await crudLog.atualizar(id=self.log_id,sucesso=True)
            return True        
        sucesso_baixa = await self.realizar_baixa_contas_receber(data_conta=data,relatorio_custos=relatorio_custos)
        return sucesso_baixa