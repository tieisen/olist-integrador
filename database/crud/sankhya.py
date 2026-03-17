from database.database import AsyncSessionLocal
from database.models import Sankhya
from sqlalchemy.future import select
from src.utils.db import validar_dados, formatar_retorno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

COLUNAS_CRIPTOGRAFADAS = [ 'token', 'x_token' ]

async def criar(app_id:int, x_token:str, **kwargs) -> bool:
    
    kwargs = validar_dados(modelo=Sankhya,
                           kwargs={'app_id':app_id, 'x_token':x_token, **kwargs},
                           colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)

    async with AsyncSessionLocal() as session:    
        try:
            novo_token = Sankhya(**kwargs)
            session.add(novo_token)
            await session.commit()
            await session.refresh(novo_token)
            return True
        except Exception as e:
            logger.error("Erro ao salvar token no banco de dados: %s",e)
            return False

async def buscar(app_id:int=None) -> dict:
    
    async with AsyncSessionLocal() as session:
        if app_id:
            result = await session.execute(
                select(Sankhya).where(Sankhya.app_id == app_id)
            )
        else:
            result = await session.execute(
                select(Sankhya)
            )
        token = result.scalar_one_or_none()
        if not token:
            return False
                
        dados_token = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                       retorno=token)
        
        return dados_token 

async def atualizar(app_id:int,**kwargs) -> bool:

    if kwargs:
        kwargs = validar_dados(modelo=Sankhya,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Sankhya).where(Sankhya.app_id == app_id)
        )
        token = result.scalar_one_or_none()
        if not token:
            return False
        for key, value in kwargs.items():
            setattr(token, key, value)
        await session.commit()
        return True

async def excluir(id:int) -> bool:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Sankhya).where(Sankhya.id == id)
        )
        token = result.scalar_one_or_none()
        if not token:
            return False
        try:
            await session.delete(token)
            await session.commit()
            return True
        except Exception as e:
            logger.error("Erro ao excluir token do banco de dados: %s", e)
            return False