import os
import time
from datetime import datetime
from database.crud import log as crudLog
from database.crud import nota as crudNota
from src.services.bot import Bot
from src.olist.nota import Nota as NotaOlist
from src.parser.conta_receber import ContaReceber
from src.integrador.nota import Nota as IntegradorNota
from src.utils.decorador import contexto, carrega_dados_ecommerce, log_execucao
from src.utils.log import set_logger
from src.utils.load_env import load_env
from src.utils.buscar_arquivo import buscar_relatorio_custos
load_env()
logger = set_logger(__name__)

class Financeiro:

    def __init__(self, id_loja:int):
        self.id_loja = id_loja
        self.log_id = None
        self.contexto = 'financeiro'
        self.dados_empresa:dict = None
        self.dados_ecommerce:dict = None
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))

    @contexto
    @carrega_dados_ecommerce
    async def agrupar_titulos_parcelados(self,**kwargs) -> bool:
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='olist',
                                          contexto=kwargs.get('_contexto'))
        dados_notas_parceladas:list[dict] = await crudNota.buscar_financeiro_parcelado(empresa_id=self.dados_ecommerce.get('empresa_id'))
        try:
            if dados_notas_parceladas:        
                notas_parceladas:list[int] = [n.get('numero') for n in dados_notas_parceladas]
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

        nota_olist = NotaOlist(id_loja=self.id_loja,empresa_id=self.dados_ecommerce.get('empresa_id'))
        integrador_nota = IntegradorNota(id_loja=self.id_loja)
        contas_dia:list[dict] = await nota_olist.buscar_lista_financeiro_aberto(dt_emissao=data_conta.strftime('%Y-%m-%d'))
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
                status = await integrador_nota.baixar_conta_liquido(data_baixa=data_conta,dados_conta=conta,dados_custo=custo)
                status_log.append(status.get('success'))
            except Exception as e:
                logger.error("Erro ao baixar conta a receber do pedido %s: %s",codigo_pedido,str(e))
                status_log.append(False)
            finally:
                time.sleep(self.req_time_sleep)
        await crudLog.atualizar(id=self.log_id,sucesso=all(status_log))
        return all(status_log)
    
    @log_execucao
    async def executar_baixa(self,data:datetime) -> bool:
        parser = ContaReceber()
        relatorio_custos:list[dict] = buscar_relatorio_custos()
        if not relatorio_custos:
            logger.error("Relatório de custos não encontrado ou vazio.")
            return False
        relatorio_custos = parser.carregar_relatorio(lista=relatorio_custos)
        if not relatorio_custos:
            await crudLog.atualizar(id=self.log_id,sucesso=True)
            return True        
        sucesso_baixa = await self.realizar_baixa_contas_receber(data_conta=data,relatorio_custos=relatorio_custos)
        return sucesso_baixa