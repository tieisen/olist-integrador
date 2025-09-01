from database.database import AsyncSessionLocal
from database.models import LogEstoque
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

async def criar(log_id:int,codprod:int,idprod:int,qtdmov:int=0,status_estoque:bool=True,status_lote:bool=None,obs:str=None):
    async with AsyncSessionLocal() as session:
        novo_log = LogEstoque(log_id=log_id,
                              codprod=codprod,
                              idprod=idprod,
                              qtdmov=qtdmov,
                              status_estoque=status_estoque,
                              status_lote=status_lote,                              
                              obs=obs)
        session.add(novo_log)
        await session.commit()
        await session.refresh(novo_log)
        return novo_log.id

async def buscar_status_false(log_id: int):
    async with AsyncSessionLocal() as session:
        log = await session.query(LogEstoque).filter(LogEstoque.log_id == log_id,
                                                     LogEstoque.status_estoque != 1).first()
        return log

async def buscar_id(log_id: int):
    async with AsyncSessionLocal() as session:
        log = await session.query(LogEstoque).filter(LogEstoque.log_id == log_id).all()
        return log
