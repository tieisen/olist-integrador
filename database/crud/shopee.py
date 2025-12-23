import os
from database.database import AsyncSessionLocal
from database.models import Shopee, Ecommerce
from datetime import datetime, timedelta
from sqlalchemy.future import select
from src.utils.db import validar_dados, formatar_retorno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

COLUNAS_CRIPTOGRAFADAS = [
        'access_token', 'refresh_token', 'partner_key'
    ]   

async def criar(ecommerce_id:int,**kwargs):
    if kwargs:
        kwargs = validar_dados(modelo=Shopee, kwargs=kwargs, colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False
    async with AsyncSessionLocal() as session:
        try:
            novo_token = Shopee(ecommerce_id=ecommerce_id,**kwargs)
            session.add(novo_token)
            await session.commit()
            return True
        except Exception as e:
            logger.error("Erro ao salvar token no banco de dados: %s",e)
            return False

async def atualizar(partner_id:int=None,ecommerce_id:int=None,**kwargs):

    if not any([partner_id, ecommerce_id]):
        print("Nenhum parâmetro informado")
        return False

    if kwargs:
        kwargs = validar_dados(modelo=Shopee,kwargs=kwargs,colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False
            
    async with AsyncSessionLocal() as session:
        if partner_id:
            result = await session.execute(
                select(Shopee).where(Shopee.partner_id == partner_id)
            )
        elif ecommerce_id:
            result = await session.execute(
                select(Shopee).where(Shopee.ecommerce_id == ecommerce_id)
            )
        else:
            msg = "Nenhum parâmetro informado"
            logger.warning(msg)
            print(msg)
            return False
                
        results_list = result.scalars().all()
        if not results_list:
            msg = f"Loja Shopee não encontrada. Partner ID: {partner_id}" if partner_id else f"Loja Shopee não encontrada. Ecommerce ID: {ecommerce_id}"
            logger.warning(msg)
            print(msg)
            return False
        
        for results in results_list:                
            for key, value in kwargs.items():
                setattr(results, key, value)
            
        await session.commit()
        return True

async def buscar(ecommerce_id:int=None,empresa_id:int=None):
    if not any([ecommerce_id,empresa_id]):
        return False
    async with AsyncSessionLocal() as session:
        if ecommerce_id:
            result = await session.execute(
                select(Shopee).where(Shopee.ecommerce_id == ecommerce_id).order_by(Shopee.id.desc()).fetch(1)
            )
        if empresa_id:
            result = await session.execute(
                select(Shopee).where(Shopee.ecommerce_.has(Ecommerce.empresa_id == empresa_id)).order_by(Shopee.id.desc()).fetch(1)
            )
        token = result.scalar_one_or_none()
    if not token:
        return False        
    dados_token = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,retorno=token)        
    return dados_token 

async def buscar_idloja(ecommerce_id:int):
    
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Ecommerce).where(Ecommerce.id == ecommerce_id)
        )
        ecommerce = result.scalar_one_or_none()
    if not ecommerce:
        return False        
    else:
        return ecommerce.id_loja

async def excluir(id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Shopee).where(Shopee.id == id)
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
        dias = int(os.getenv('DIAS_LIMPA_CACHE',7))
    except Exception as e:
        erro = f"Valor para intervalo de dias do cache não encontrado. {e}"
        logger.error(erro)
        return False    
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Shopee).where(Shopee.dh_solicitacao < (datetime.now()-timedelta(days=dias)))
        )
        tokens = result.scalars().all()
        if not tokens:
            return None
        try:
            for token in tokens:
                await session.delete(token)
            await session.commit()
            return True
        except Exception as e:
            logger.error("Erro ao excluir tokens do banco de dados: %s", e)
            return False
