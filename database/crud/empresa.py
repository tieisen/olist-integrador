from database.database import AsyncSessionLocal
from sqlalchemy.ext.asyncio import AsyncSession
from database.models import Empresa
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
        codigo_snk: int,
        nome: str,
        cnpj: str,
        client_id: str,
        client_secret: str,
        admin_email: str,
        admin_senha: str,
        serie_nfe: str
    ):

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Empresa).where(Empresa.codigo_snk == codigo_snk)
        )
        empresa = result.scalar_one_or_none()

        if empresa:
            print("Empresa já existe")
            return False

        nova_empresa = Empresa(
            codigo_snk=codigo_snk,
            nome=nome,
            cnpj=cnpj,
            client_id=client_id,
            client_secret=client_secret,
            admin_email=admin_email,
            admin_senha=admin_senha,
            serie_nfe=serie_nfe,
            ativo=True
        )

        session.add(nova_empresa)
        await session.commit()
        await session.refresh(nova_empresa)
        return True

async def atualizar_id(empresa_id:int,**kwargs):

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Empresa).where(Empresa.id == empresa_id)
        )
        empresa = result.scalar_one_or_none()
        if not empresa:
            return False
        for key, value in kwargs.items():
            setattr(empresa, key, value)
        await session.commit()
        await session.refresh(empresa)
        return True

async def atualizar_codigo(codigo_snk:int,**kwargs):

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Empresa).where(Empresa.codigo_snk == codigo_snk)
        )
        empresa = result.scalar_one_or_none()
        if not empresa:
            return False
        for key, value in kwargs.items():
            setattr(empresa, key, value)
        await session.commit()
        await session.refresh(empresa)
        return True

async def buscar_codigo(codigo_snk:int):

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Empresa).where(Empresa.codigo_snk == codigo_snk)
        )
        empresa = result.scalar_one_or_none()
        if not empresa:
            print(f"Empresa não encontrada. Parâmetro: {codigo_snk}")
            return False
        return empresa.__dict__