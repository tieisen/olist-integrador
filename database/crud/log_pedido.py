from database.database import AsyncSessionLocal
from database.models import LogPedido
from sqlalchemy.future import select
from src.utils.log import Log
from src.utils.db import formatar_retorno
import os
import logging
from dotenv import load_dotenv

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

async def criar(
        log_id:int,
        pedido_id:int,
        evento:str,
        status:bool=True,
        obs:str=None
    ) -> bool:
    async with AsyncSessionLocal() as session:
        novo_log = LogPedido(log_id=log_id,
                             pedido_id=pedido_id,
                             evento=evento,
                             status=status,
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
        dados_logs = formatar_retorno(retorno=logs)        
        return dados_logs
