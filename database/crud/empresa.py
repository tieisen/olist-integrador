from database.database import AsyncSessionLocal
from database.models import Empresa
from src.services.criptografia import Criptografia
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

def valida_criptografia(kwargs):
    colunas_criptografadas = [
        'client_secret',
        'olist_admin_senha',
        'snk_token',
        'snk_appkey',
        'snk_admin_senha'
    ]    
    # Criptografa os dados sensíveis
    cripto = Criptografia()
    for key, value in kwargs.items():
        if key in colunas_criptografadas:
            kwargs[key] = cripto.criptografar(value)
    
    return kwargs
        
def valida_colunas_existentes(kwargs):
    colunas_do_banco = [
        'serie_nfe','client_id','client_secret',
        'olist_admin_email','olist_admin_senha',
        'olist_idfornecedor_padrao','olist_iddeposito_padrao',
        'olist_dias_busca_pedidos','olist_situacao_busca_pedidos',
        'snk_token','snk_appkey','snk_admin_email',
        'snk_admin_senha','snk_timeout_token_min',
        'snk_top_pedido','snk_top_venda','snk_top_devolucao',
        'snk_codvend','snk_codcencus','snk_codnat',
        'snk_codtipvenda','snk_codusu_integracao','snk_codtab_transf',
        'snk_cod_local_estoque','snk_codparc'
    ]

    # Verifica se existe coluna no banco para os dados informados
    for _ in kwargs.keys():
        if _ not in colunas_do_banco:
            kwargs.pop(_)
            erro = f"Coluna {_} não encontrada no banco de dados."
            logger.warning(erro)
    
    return kwargs

async def criar(snk_codemp:int,
                nome:str,
                cnpj:str,
                **kwargs):

    kwargs = valida_colunas_existentes(kwargs)
    if not kwargs:
        print("Colunas informadas não existem no banco de dados.")
        return False
    
    kwargs = valida_criptografia(kwargs)

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Empresa).where(Empresa.snk_codemp == snk_codemp)
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
        await session.refresh(nova_empresa)
        return True

async def atualizar_id(empresa_id:int,**kwargs):

    kwargs = valida_colunas_existentes(kwargs)
    if not kwargs:
        print("Colunas informadas não existem no banco de dados.")
        return False
    
    kwargs = valida_criptografia(kwargs)

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

async def atualizar_codigo(snk_codemp:int,**kwargs):

    kwargs = valida_colunas_existentes(kwargs)
    if not kwargs:
        print("Colunas informadas não existem no banco de dados.")
        return False
    
    kwargs = valida_criptografia(kwargs)

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Empresa).where(Empresa.snk_codemp == snk_codemp)
        )
        empresa = result.scalar_one_or_none()
        if not empresa:
            return False
        for key, value in kwargs.items():
            setattr(empresa, key, value)
        await session.commit()
        await session.refresh(empresa)
        return True

async def buscar_codigo(snk_codemp:int):

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Empresa).where(Empresa.snk_codemp == snk_codemp)
        )
        empresa = result.scalar_one_or_none()
        if not empresa:
            print(f"Empresa não encontrada. Parâmetro: {snk_codemp}")
            return False
        return empresa.__dict__

async def excluir_id(id:int):

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Empresa).where(Empresa.id == id)
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

async def excluir_codigo(snk_codemp:int):

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Empresa).where(Empresa.snk_codemp == snk_codemp)
        )
        empresa = result.scalar_one_or_none()
        if not empresa:
            print(f"Empresa não encontrada. Parâmetro: {snk_codemp}")
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