from database.database import AsyncSessionLocal
from database.models import Empresa
from src.utils.db import validar_dados, formatar_retorno
from sqlalchemy.future import select
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

COLUNAS_CRIPTOGRAFADAS = [
        'client_secret',
        'olist_admin_senha',
        'snk_token',
        'snk_appkey',
        'snk_admin_senha'
    ]    

async def criar(snk_codemp:int,
                nome:str,
                cnpj:str,
                **kwargs) -> bool:

    if kwargs:
        kwargs = validar_dados(modelo=Empresa,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Empresa)
            .where(Empresa.snk_codemp == snk_codemp)
        )
        empresa = result.scalar_one_or_none()

        if empresa:
            print("Empresa já existe")
            return False

        nova_empresa = Empresa(
            snk_codemp=snk_codemp,
            nome=nome,
            cnpj=cnpj,
            **kwargs
        )

        session.add(nova_empresa)
        await session.commit()
        return True

async def atualizar(
        id:int=None,
        codemp:int=None,
        **kwargs
    ) -> bool:

    if not any([id,codemp]):
        return False

    if kwargs:
        kwargs = validar_dados(modelo=Empresa,
                            kwargs=kwargs,
                            colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False

    async with AsyncSessionLocal() as session:
        if id:
            result = await session.execute(
                select(Empresa).where(Empresa.id == id)
            )

        if codemp:
            result = await session.execute(
                select(Empresa).where(Empresa.snk_codemp == codemp)
            )
        empresa = result.scalar_one_or_none()
        if not empresa:
            return False
        for key, value in kwargs.items():
            setattr(empresa, key, value)
        await session.commit()
        return True

async def buscar(
        id:int=None,
        codemp:int=None
    ) -> dict:

    async with AsyncSessionLocal() as session:
        if id:
            result = await session.execute(
                select(Empresa).where(Empresa.id == id)
            )
        elif codemp:
            result = await session.execute(
                select(Empresa).where(Empresa.snk_codemp == codemp)
            )
        else:
            result = await session.execute(
                select(Empresa).where(Empresa.ativo.is_(True))
            )        
        empresa = result.scalars().all()
        if not empresa:
            return []
        dados_empresa = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                         retorno=empresa)        
        return dados_empresa

async def excluir(
        id:int=None,
        codemp:int=None
    ) -> bool:

    if not any([id,codemp]):
        return False
    async with AsyncSessionLocal() as session:
        if id:
            result = await session.execute(
                select(Empresa).where(Empresa.id == id)
            )
        if codemp:
            result = await session.execute(
                select(Empresa).where(Empresa.snk_codemp == codemp)
            )
        empresa = result.scalar_one_or_none()
        if not empresa:
            print(f"Empresa não encontrada. Parâmetro: {id}")
            return False        
        try:
            await session.delete(empresa)
            await session.commit()
            return True
        except Exception as e:
            await session.rollback()
            print("Erro ao excluir empresa no banco de dados: %s", e)
            logger.error("Erro ao excluir empresa no banco de dados: %s", e)
            return False