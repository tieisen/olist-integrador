from database.database import AsyncSessionLocal
from database.models import LogEstoque
from sqlalchemy.future import select
from src.utils.db import formatar_retorno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

COLUNAS_CRIPTOGRAFADAS = []

async def criar(log_id:int,codprod:int,idprod:int,qtdmov:int=0,sucesso:bool=True,status_lotes:bool=None,obs:str=None) -> bool:
    async with AsyncSessionLocal() as session:
        novo_log = LogEstoque(log_id=log_id,
                              codprod=codprod,
                              idprod=idprod,
                              qtdmov=qtdmov,
                              sucesso=sucesso,
                              status_lotes=status_lotes,                              
                              obs=obs)
        session.add(novo_log)
        await session.commit()
        return True

async def buscar_falhas(log_id: int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(LogEstoque)
            .where(LogEstoque.log_id == log_id,
                   LogEstoque.sucesso.is_(False))
        )
        logs = result.scalars().all()
        dados_logs = formatar_retorno(retorno=logs, colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        return dados_logs

async def buscar_id(log_id: int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(LogEstoque)
            .where(LogEstoque.log_id == log_id)
        )
        logs = result.scalars().all()
        dados_logs = formatar_retorno(retorno=logs, colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        return dados_logs
