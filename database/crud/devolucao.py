from database.database import AsyncSessionLocal
from database.models import Devolucao, Nota, Pedido
from datetime import datetime
from src.utils.log import Log
from sqlalchemy.future import select
from src.utils.db import validar_dados, formatar_retorno
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

COLUNAS_CRIPTOGRAFADAS = None

async def criar(
        chave_referenciada:int,
        id_nota:int,
        numero:int,
        serie:str,
        dh_emissao:str=None,
        **kwargs
    ) -> bool:

    if kwargs:
        kwargs = validar_dados(modelo=Devolucao,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False
    devolucao = await buscar(id_nota=id_nota)
    if devolucao:
        print(f"Nota de devolução {id_nota} já existe")
        return False
    async with AsyncSessionLocal() as session:        
        nota_referenciada = await session.execute(
            select(Nota)
            .where(Nota.chave_acesso == chave_referenciada)
        )
        if not nota_referenciada:
            print(f"Nota referenciada não encontrada para a devolução {id_nota}")
            return False
        id_nota_referenciada = nota_referenciada.scalar_one_or_none().id
        nova_devolucao = Devolucao(nota_id=id_nota_referenciada,
                                   id_nota=id_nota,
                                   numero=numero,
                                   serie=serie,
                                   dh_emissao=datetime.strptime(dh_emissao,'%Y-%m-%d %H:%M:%S') if dh_emissao else datetime.now(),
                                   **kwargs)
        session.add(nova_devolucao)
        await session.commit()
    return True

async def buscar_lancar(ecommerce_id:int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Devolucao)
            .where(Devolucao.nunota.is_(None),
                   Devolucao.nota_.has(
                        Nota.pedido_.has(
                        Pedido.ecommerce_id==ecommerce_id)))
        )
        devolucoes = result.scalars().all()
        dados_devolucoes = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                            retorno=devolucoes)
        return dados_devolucoes

async def buscar_confirmar() -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Devolucao)
            .where(Devolucao.nunota.isnot(None),
                   Devolucao.dh_cancelamento.is_(None),
                   Devolucao.dh_confirmacao.is_(None))
        )
        devolucoes = result.scalars().all()
        dados_devolucoes = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                            retorno=devolucoes)
        return dados_devolucoes

async def buscar(
        id_nota:int=None,
        nunota:int=None,
        chave:str=None,
        numero_ecommerce:dict=None,
        lista_chave:list[str]=None
    ) -> dict:
    if not any([id_nota,nunota,lista_chave]):
        return False
    async with AsyncSessionLocal() as session:
        if nunota:
            result = await session.execute(
                select(Devolucao)
                .where(Devolucao.nunota == nunota)
            )
        elif id_nota:
            result = await session.execute(
                select(Devolucao)
                .where(Devolucao.id_nota == id_nota)
            )
        elif chave:
            result = await session.execute(
                select(Devolucao)
                .where(Devolucao.chave_acesso == chave)
            )
        elif numero_ecommerce:
            result = await session.execute(
                select(Devolucao)
                .where(Devolucao.numero == numero_ecommerce.get('numero'),
                       Devolucao.nota_.has(
                            Nota.pedido_.has(
                          Pedido.ecommerce_id==numero_ecommerce.get('ecommerce'))))
            )
        elif lista_chave:
            result = await session.execute(
                select(Devolucao)
                .where(Devolucao.chave_acesso.in_(lista_chave))
            )
        else:
            return False
        devolucao = result.scalar_one_or_none()
    if not devolucao:
        print(f"Devolução não encontrada. Parâmetro: {nunota or id_nota or chave or numero_ecommerce or lista_chave}")
        return False
    dados_devolucao = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                       retorno=devolucao)        
    return dados_devolucao    

async def atualizar(
        id_nota:int=None,
        numero:int=None,
        nunota:int=None,
        **kwargs
    ) -> bool:

    if not any([id_nota,numero,nunota]):
        return False
    if kwargs:
        kwargs = validar_dados(modelo=Devolucao,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False
    async with AsyncSessionLocal() as session:
        if nunota and not any([id_nota,numero]):
            result = await session.execute(
                select(Devolucao)
                .where(Devolucao.nunota == nunota)
            )
        else:
            kwargs['nunota'] = nunota
            if id_nota:
                result = await session.execute(
                    select(Devolucao)
                    .where(Devolucao.id_nota == id_nota)
                )
            if numero:
                result = await session.execute(
                    select(Devolucao)
                    .where(Devolucao.numero == numero)
                )

        if not result:
            print(f"Devolução não encontrada. Parâmetro: {nunota or id_nota or numero}")
            return False
    
        devolucoes = result.scalars().all()

        for devolucao in devolucoes:
            for key, value in kwargs.items():
                setattr(devolucao, key, value)    

        await session.commit()
        return True