from database.database import AsyncSessionLocal
from database.models import LogPedido
from database.crud import pedido
from sqlalchemy.future import select
from src.utils.db import formatar_retorno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

COLUNAS_CRIPTOGRAFADAS = None

async def criar(
        log_id:int,
        evento:str,
        pedido_id:int=None,
        nunota:int=None,        
        sucesso:bool=True,
        obs:str=None
    ) -> bool:
    async with AsyncSessionLocal() as session:

        if not any([pedido_id, nunota]):
            logger.error("É necessário informar 'pedido_id' ou 'nunota' para criar um log de pedido.")
            return False
        
        if nunota:
            lista = await pedido.buscar(nunota=nunota)
            if lista:
                for ped in lista:
                    novo_log = LogPedido(log_id=log_id,
                                        pedido_id=ped.get('id'),
                                        evento=evento,
                                        sucesso=sucesso,
                                        obs=obs)
                    session.add(novo_log)
        else:
            novo_log = LogPedido(log_id=log_id,
                                pedido_id=pedido_id,
                                evento=evento,
                                sucesso=sucesso,
                                obs=obs)
            session.add(novo_log)
        await session.commit()
        return True

async def buscar_id(log_id: int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(LogPedido)
            .where(LogPedido.log_id == log_id)
        )
        logs = result.scalars().all()
        dados_logs = formatar_retorno(retorno=logs)        
        return dados_logs

async def buscar_falhas(log_id: int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(LogPedido)
            .where(LogPedido.log_id == log_id,
                   LogPedido.sucesso.is_(False))
        )
        logs = result.scalars().all()
        dados_logs = formatar_retorno(retorno=logs,
                                      colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        return dados_logs
