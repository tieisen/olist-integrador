from database.database import AsyncSessionLocal
from datetime import datetime
from database.models import Produto
from sqlalchemy.future import select
from src.utils.db import validar_dados, formatar_retorno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

COLUNAS_CRIPTOGRAFADAS = None

async def criar(
        cod_snk:int,
        cod_olist:int,
        empresa_id:int,
        **kwargs
    ):

    if kwargs:
        kwargs = validar_dados(modelo=Produto,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False    

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Produto).where(Produto.cod_snk == cod_snk,
                                  Produto.empresa_id == empresa_id)
        )
        produto = result.scalar_one_or_none()

        if produto:
            print("Produto já existe nesta empresa")
            return False
        
        novo_produto = Produto(
            cod_snk=cod_snk,
            cod_olist=cod_olist,
            empresa_id=empresa_id,
            **kwargs
        )
        
        session.add(novo_produto)
        await session.commit()
        await session.refresh(novo_produto)
    return True

async def atualizar(
        cod_snk:int,
        empresa_id:int,
        pendencia:bool=False,
        **kwargs
    ):

    if kwargs:
        kwargs = validar_dados(modelo=Produto,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False  

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Produto).where(Produto.cod_snk == cod_snk,
                                  Produto.empresa_id == empresa_id)
        )
        produto = result.scalar_one_or_none()

        if not produto:
            print("Produto não encontrado")
            return False
        
        if kwargs:
            for key, value in kwargs.items():
                setattr(produto, key, value)

        setattr(produto, "pendencia", pendencia)
        if not pendencia:
            setattr(produto, "dh_atualizacao", datetime.now())
        await session.commit()
        await session.close()
        return True

async def buscar_pendencias(empresa_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Produto).where(Produto.pendencia.is_(True),
                                  Produto.empresa_id == empresa_id)
        )
        produtos = result.scalars().all()

        dados_produtos = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                         retorno=produtos)
        
        return dados_produtos

async def buscar(empresa_id: int, cod_olist: int=None, cod_snk: int=None):

    if not any([cod_olist, cod_snk]):
        return False

    async with AsyncSessionLocal() as session:
        if cod_olist:
            result = await session.execute(
                select(Produto).where(Produto.cod_olist == cod_olist,
                                      Produto.empresa_id == empresa_id)
            )
        if cod_snk:
            result = await session.execute(
                select(Produto).where(Produto.cod_snk == cod_snk,
                                      Produto.empresa_id == empresa_id)
            )
        produto = result.scalar_one_or_none()

        dados_produto = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                         retorno=produto)

        return dados_produto

async def excluir(cod_snk: int, empresa_id: int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Produto).where(Produto.cod_snk == cod_snk,
                                  Produto.empresa_id == empresa_id)
        )
        produto = result.scalar_one_or_none()
        if not produto:
            print("Produto não encontrado")
            return False
        try:
            await session.delete(produto)
            await session.commit()
            return True
        except Exception as e:
            await session.rollback()
            logger.error("Erro ao excluir produto no banco de dados: %s", e)
            return False
        