from database.database import AsyncSessionLocal
from database.models import LogProduto
from database.models import Produto
from src.utils.log import Log
from sqlalchemy.future import select
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
        codprod:int=0,
        idprod:int=0,
        campo:str='',
        valor_old:str=None,
        valor_new:str=None,
        sucesso:bool=True,
        obs:str=None,
        produto_id:int=None
    ):
    async with AsyncSessionLocal() as session:

        if not produto_id and not (codprod == 0 and idprod == 0):
            if codprod :
                produto = await session.execute(
                    select(Produto)
                    .where(Produto.cod_snk == codprod)
                )
                produto = produto.scalar_one_or_none()
                if not produto:
                    return False
                produto_id = produto.id
            elif idprod:
                produto = await session.execute(
                    select(Produto)
                    .where(Produto.cod_olist == idprod)
                )
                produto = produto.scalar_one_or_none()
                if not produto:
                    return False
                produto_id = produto.id
            else:
                pass

        novo_log = LogProduto(log_id=log_id,
                              codprod=codprod,
                              idprod=idprod,
                              campo=campo,
                              sucesso=sucesso,
                              valor_old=str(valor_old),
                              valor_new=str(valor_new),
                              obs=obs,
                              produto_id=produto_id)
        session.add(novo_log)
        await session.commit()
        await session.refresh(novo_log)
        return True

async def buscar_falhas(log_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(LogProduto)
            .where(LogProduto.log_id == log_id,
                   LogProduto.sucesso.is_(False))
        )
        log = result.scalars().all()
        return log

async def buscar_id(log_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(LogProduto)
            .where(LogProduto.log_id == log_id)
        )
        log = result.scalars().all()
        return log