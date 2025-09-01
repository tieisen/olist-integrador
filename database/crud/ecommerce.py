from database.database import AsyncSessionLocal
from database.models import Ecommerce
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

async def criar(id_loja:int, nome:str, empresa_id:int):
    async with AsyncSessionLocal() as session:
        ecommerce = await session.query(Ecommerce.id_loja == id_loja,
                                        Ecommerce.empresa_id == empresa_id).first()
        if ecommerce:
            return False
        novo_ecommerce = Ecommerce(id_loja=id_loja,
                                   nome=nome,
                                   empresa_id=empresa_id)
        session.add(novo_ecommerce)
        await session.commit()
        await session.refresh(novo_ecommerce)
        return True

async def atualizar(ecommerce_id:int, **kwargs):
    async with AsyncSessionLocal() as session:
        ecommerce = await session.query(Ecommerce.id == ecommerce_id).first()
        if not ecommerce:
            return False
        for key, value in kwargs.items():
            setattr(ecommerce, key, value)
        await session.commit()
        await session.refresh(ecommerce)
        return True