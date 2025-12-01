import os
import time
from datetime import datetime, timedelta
from src.integrador.nota import Nota as IntegradorNota
from src.olist.nota import Nota as NotaOlist
from database.crud import log as crudLog
from src.services.bot import Bot
from datetime import datetime
from src.utils.decorador import contexto, carrega_dados_ecommerce, log_execucao
from src.utils.log import set_logger
from src.utils.load_env import load_env
from src.utils.buscar_arquivo import buscar_relatorio_custos
load_env()
logger = set_logger(__name__)

class Financeiro:

    def __init__(self, id_loja:int=None):
        self.id_loja = id_loja
        self.log_id = None
        self.contexto = 'financeiro'
        self.dados_empresa:dict = None
        self.dados_ecommerce:dict = None
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))

    @contexto
    @carrega_dados_ecommerce
    async def realizar_baixa_contas_receber(self,data_conta:str,relatorio_custos:dict,**kwargs) -> bool:

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
        nota_olist = NotaOlist(id_loja=self.dados_ecommerce.get('id_loja'),empresa_id=self.dados_ecommerce.get('empresa_id'))
        integrador_nota = IntegradorNota(id_loja=self.dados_ecommerce.get('id_loja'))
        data_conta:datetime = datetime.strptime(data_conta, '%d/%m/%Y').date()
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
    
    @contexto
    @carrega_dados_ecommerce
    async def baixar_relatorio_custos(self,data:str,**kwargs) -> bool:
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))        
        data:datetime = datetime.strptime(data, '%d/%m/%Y').date()
        data_ini:datetime = data - timedelta(days=2)
        bot = Bot()
        try:
            await bot.login()
            await bot.acessa_relatorio_custos()
            await bot.gerar_relatorio_custos(data_inicial=data_ini.strftime('%d/%m/%Y'),
                                             data_final=data.strftime('%d/%m/%Y'))
            await bot.baixar_relatorio_custos()
            await crudLog.atualizar(id=self.log_id,sucesso=True)
            await bot.logout()
            return True            
        except Exception as e:
            logger.error("Erro ao baixar relatorio de custos: %s",str(e))
            await crudLog.atualizar(id=self.log_id,sucesso=False)
            return False
    
    @contexto
    @log_execucao
    async def executar_baixa(self,data:str) -> bool:
        if await self.baixar_relatorio_custos(data=data):
            relatorio_custos:list[dict] = buscar_relatorio_custos()
            if not relatorio_custos:
                logger.error("Relatório de custos não encontrado ou vazio.")
                return False
            sucesso_baixa = await self.realizar_baixa_contas_receber(data_conta=data,relatorio_custos=relatorio_custos)
            return sucesso_baixa