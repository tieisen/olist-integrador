from database.database import AsyncSessionLocal
from database.models import Ecommerce
from sqlalchemy.future import select
from src.utils.db import validar_dados, formatar_retorno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

COLUNAS_CRIPTOGRAFADAS = None

async def criar(
        id_loja:int,
        nome:str,
        empresa_id:int,
        **kwargs
    ):

    if kwargs:
        kwargs = validar_dados(modelo=Ecommerce,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False

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
                                   empresa_id=empresa_id,
                                   **kwargs)
        session.add(novo_ecommerce)
        await session.commit()
        return True

async def buscar(empresa_id:int=None, id_loja:int=None):
   
    async with AsyncSessionLocal() as session:
        if empresa_id:
            result = await session.execute(
                select(Ecommerce)
                .where(Ecommerce.empresa_id == empresa_id,
                       Ecommerce.ativo.is_(True))
            )
        elif id_loja:
            result = await session.execute(
                select(Ecommerce)
                .where(Ecommerce.id_loja == id_loja)
            )
        else:
            result = await session.execute(
                select(Ecommerce)
                .where(Ecommerce.ativo.is_(True))
            )  
        ecommerce = result.scalars().all()
        dados_ecommerce = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                           retorno=ecommerce)           
        return dados_ecommerce        
    
async def atualizar(ecommerce_id:int, **kwargs):

    if kwargs:
        kwargs = validar_dados(modelo=Ecommerce,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
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