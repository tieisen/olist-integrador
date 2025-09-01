from database.database import AsyncSessionLocal
from datetime import datetime
from database.models import Produto
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

async def criar(cod_snk:int, cod_olist:int):
    async with AsyncSessionLocal() as session:
        produto = await session.execute(Produto.query.where(Produto.cod_snk == cod_snk)).first()
        if produto:
            return False
        novo_produto = Produto(cod_snk=cod_snk,
                               cod_olist=cod_olist)
        session.add(novo_produto)
        await session.commit()
        await session.refresh(novo_produto)
    return True

async def atualizar(cod_snk: int, pendencia: bool):
    async with AsyncSessionLocal() as session:
        produto = await session.execute(Produto.query.where(Produto.cod_snk == cod_snk)).first()
        if not produto:
            return False
        setattr(produto, "pendencia", pendencia)
        if not pendencia:
            setattr(produto, "dh_atualizado", datetime.now())
        await session.commit()
        await session.close()
        return True

async def buscar_pendencias():
    async with AsyncSessionLocal() as session:
        produto = await session.execute(Produto.query.filter(Produto.pendencia.is_(True)).all())
        return produto

async def buscar_olist(cod_olist: int):
    async with AsyncSessionLocal() as session:
        produto = await session.execute(Produto.query.filter(Produto.cod_olist == cod_olist).first())
        return produto

async def buscar_snk(cod_snk: int):
    async with AsyncSessionLocal() as session:
        produto = await session.execute(Produto.query.filter(Produto.cod_snk == cod_snk).first())
        return produto

async def excluir(cod_snk: int):
    async with AsyncSessionLocal() as session:
        produto = await session.execute(Produto.query.filter(Produto.cod_snk == cod_snk).first())
        if not produto:
            return False
        try:
            await session.delete(produto)
            await session.commit()
            return True
        except Exception as e:
            logger.error("Erro ao excluir produtos no banco de dados: %s",e)
            return False
        