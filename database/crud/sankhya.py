from database.database import AsyncSessionLocal
from database.models import Sankhya
from datetime import datetime, timedelta
from src.services.criptografia import Criptografia
from sqlalchemy.future import select
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

def valida_criptografia(kwargs):
    colunas_criptografadas = [ 'token' ]    
    # Criptografa os dados sensíveis
    cripto = Criptografia()
    for key, value in kwargs.items():
        if key in colunas_criptografadas:
            kwargs[key] = cripto.criptografar(value).decode()
    
    return kwargs
        
def valida_colunas_existentes(kwargs):
    colunas_do_banco = [
        'dh_solicitacao', 'token',
        'dh_expiracao_token', 'empresa_id'
    ]

    # Verifica se existe coluna no banco para os dados informados
    for _ in kwargs.keys():
        if _ not in colunas_do_banco:
            kwargs.pop(_)
            erro = f"Coluna {_} não encontrada no banco de dados."
            logger.warning(erro)
    
    return kwargs

async def criar(
        empresa_id:int,
        **kwargs
    ):

    kwargs = valida_colunas_existentes(kwargs)
    if not kwargs:
        print("Colunas informadas não existem no banco de dados.")
        return False
    
    kwargs = valida_criptografia(kwargs)

    async with AsyncSessionLocal() as session:    
        try:
            novo_token = Sankhya(empresa_id=empresa_id,
                                 **kwargs)
            session.add(novo_token)
            await session.commit()
            await session.refresh(novo_token)
            return True
        except Exception as e:
            logger.error("Erro ao salvar token no banco de dados: %s",e)
            return False

async def buscar_token(empresa_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Sankhya).where(Sankhya.empresa_id == empresa_id).order_by(Sankhya.id.desc()).fetch(1)
        )
        token = result.scalar_one_or_none()
        try:
            cripto = Criptografia()
            token_literal = cripto.descriptografar(token.token)
            return token_literal
        except Exception as e:
            erro = f"Erro ao descriptografar token: {e}"
            logger.error(erro)
            return False    

async def excluir(id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Sankhya).where(Sankhya.id == id)
        )
        token = result.scalar_one_or_none()
        if not token:
            print("Token não encontrado")
            return False
        try:
            await session.delete(token)
            await session.commit()
            return True
        except Exception as e:
            logger.error("Erro ao excluir token do banco de dados: %s", e)
            return False

async def excluir_cache():

    try:
        dias = os.getenv('DIAS_LIMPA_CACHE',7)
    except Exception as e:
        erro = f"Valor para intervalo de dias do cache não encontrado. {e}"
        logger.error(erro)
        return False

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Sankhya).where(Sankhya.dh_solicitacao < (datetime.now()-timedelta(days=dias)))
        )
        tokens = result.scalars().all()
        if not tokens:
            print("Tokens não encontrados")
            return False
        try:
            for token in tokens:
                await session.delete(token)
            await session.commit()
            return True
        except Exception as e:
            logger.error("Erro ao excluir tokens do banco de dados: %s", e)
            return False
