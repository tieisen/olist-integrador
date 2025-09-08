from database.database import AsyncSessionLocal
from database.models import Ecommerce
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

def valida_colunas_existentes(kwargs):
    colunas_do_banco = [
        'id_loja','nome',
        'ativo','empresa_id'
    ]

    # Verifica se existe coluna no banco para os dados informados
    for _ in kwargs.keys():
        if _ not in colunas_do_banco:
            kwargs.pop(_)
            erro = f"Coluna {_} não encontrada no banco de dados."
            logger.warning(erro)
    
    return kwargs

async def criar(
        id_loja:int,
        nome:str,
        empresa_id:int
    ):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Ecommerce).where(
                Ecommerce.id_loja == id_loja,
                Ecommerce.empresa_id == empresa_id
            )
        )
        ecommerce = result.scalar_one_or_none()

        if ecommerce:
            print("Ecommerce já existe")
            return False
        novo_ecommerce = Ecommerce(id_loja=id_loja,
                                   nome=nome,
                                   empresa_id=empresa_id)
        session.add(novo_ecommerce)
        await session.commit()
        return True

async def buscar_idloja(id_loja:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Ecommerce).where(
                Ecommerce.id_loja == id_loja
            )
        )
        ecommerce = result.scalar_one_or_none()
        if not ecommerce:
            print(f"Ecommerce não encontrado. Parâmetro: {id_loja}")
            return False
        return ecommerce.__dict__
    
async def atualizar(ecommerce_id:int, **kwargs):

    kwargs = valida_colunas_existentes(kwargs)
    if not kwargs:
        print("Colunas informadas não existem no banco de dados.")
        return False

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Ecommerce).where(Ecommerce.id == ecommerce_id)
        )
        ecommerce = result.scalar_one_or_none()
        if not ecommerce:
            print(f"Ecommerce não encontrado. Parâmetro: {ecommerce_id}")
            return False
        for key, value in kwargs.items():
            setattr(ecommerce, key, value)
        await session.commit()
        return True

async def excluir(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Ecommerce).where(Ecommerce.id == ecommerce_id)
        )
        ecommerce = result.scalar_one_or_none()
        if not ecommerce:
            print(f"Ecommerce não encontrado. Parâmetro: {ecommerce_id}")
            return False
        await session.delete(ecommerce)
        await session.commit()
        return True