from database.database import AsyncSessionLocal
from database.models import LogPedido
from src.utils.log import Log
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

async def criar(log_id:int,pedido_id:int,evento:str,status:bool=True,obs:str=None):
    async with AsyncSessionLocal() as session:
        novo_log = LogPedido(log_id=log_id,
                             pedido_id=pedido_id,
                             evento=evento,
                             status=status,
                             obs=obs)
        session.add(novo_log)
        await session.commit()
        return True

async def buscar_id(log_id: int):
    async with AsyncSessionLocal() as session:
        log = await session.query(LogPedido).filter(LogPedido.log_id == log_id).all()
        return log

async def buscar_status_false(log_id: int):
    async with AsyncSessionLocal() as session:
        log = await session.query(LogPedido).filter(LogPedido.log_id == log_id,
                                                    LogPedido.status.is_(False)).first()
        return log
