from database.database import AsyncSessionLocal
from datetime import datetime, timedelta
from database.models import Log
from src.utils.log import Log as LogEventos
import os
import logging
from dotenv import load_dotenv

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=LogEventos().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

async def criar(empresa_id:int, de:str, para:str, contexto:str):
    async with AsyncSessionLocal() as session:
        novo_log = Log(empresa_id=empresa_id,
                       de=de,
                       para=para,
                       contexto=contexto)
        session.add(novo_log)
        await session.commit()
        await session.refresh(novo_log)
    return novo_log.id

async def atualizar(id:int, sucesso:bool=True):
    async with AsyncSessionLocal() as session:
        log = await session.query(Log).filter(Log.id == id).first()
        if not log:
            return False
        setattr(log, "sucesso", sucesso)
        await session.commit()
        await session.refresh(log)
        return True

async def buscar_falhas(empresa_id:int):
    async with AsyncSessionLocal() as session:
        log = await session.query(Log).filter(Log.sucesso.isnot(1),
                                              Log.dh_execucao >= datetime.now().replace(minute=0, second=0, microsecond=0)-timedelta(hours=1),
                                              Log.empresa_id == empresa_id).order_by(Log.dh_execucao).all()
        return log
  