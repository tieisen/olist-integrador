from database.database import AsyncSessionLocal
from database.models import Pedido, Ecommerce
from sqlalchemy.future import select
from src.utils.db import validar_dados, formatar_retorno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

COLUNAS_CRIPTOGRAFADAS = None

async def criar(
        id_loja:int,
        id_pedido:int,
        cod_pedido:str,
        num_pedido:int,
        **kwargs
    ):
    
    if kwargs:
        kwargs = validar_dados(modelo=Pedido,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).where(Pedido.id_pedido == id_pedido)
        )
        pedido = result.scalar_one_or_none()
        if pedido:
            print(f"Pedido {id_pedido} já existe na base")
            return False

        result = await session.execute(
            select(Ecommerce).where(Ecommerce.id_loja == id_loja)
        )
        ecommerce = result.scalar_one_or_none()
        if not ecommerce:
            print(f"Ecommerce não encontrado para o pedido {id_pedido} na loja {id_loja}")
            return False

        try:
            novo_pedido = Pedido(id_pedido=id_pedido,
                                 cod_pedido=cod_pedido,
                                 num_pedido=num_pedido,
                                 ecommerce_id=ecommerce.id,
                                 **kwargs)
            session.add(novo_pedido)
            await session.commit()
            await session.refresh(novo_pedido)            
            return novo_pedido.id
        except Exception as e:
            print(f"Erro ao criar pedido {id_pedido}: {e}")
            return False

async def buscar(
        id_pedido:int=None,
        num_pedido:int=None,
        cod_pedido:str=None,
        nunota:str=None,
        lista:list[int]=None
    ) -> list[dict]:

    if not any([id_pedido, num_pedido, cod_pedido, nunota, lista]):
        print("Nenhum parâmetro informado")
        return False
    
    async with AsyncSessionLocal() as session:
        if id_pedido:
            result = await session.execute(
                select(Pedido).where(Pedido.id_pedido == id_pedido)
            )
        elif num_pedido:
            result = await session.execute(
                select(Pedido).where(Pedido.num_pedido == num_pedido)
            )
        elif cod_pedido:
            result = await session.execute(
                select(Pedido).where(Pedido.cod_pedido == cod_pedido)
            )
        elif nunota:
            result = await session.execute(
                select(Pedido).where(Pedido.nunota == nunota)
            )
        elif lista:
            result = await session.execute(
                select(Pedido).where(Pedido.id_pedido.in_(lista))
            )
        else:
            return False
        
        pedidos = result.scalars().all()
        if not pedidos:
            return []
        dados_pedidos = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                         retorno=pedidos)
        return dados_pedidos

async def atualizar(
        id_pedido:int=None,
        num_pedido:int=None,
        nunota:int=None,
        **kwargs
    ):

    if not any([id_pedido, num_pedido, nunota]):
        print("Nenhum parâmetro informado")
        return False

    if kwargs:
        kwargs = validar_dados(modelo=Pedido,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False
            
    async with AsyncSessionLocal() as session:
        if nunota and not any([id_pedido, num_pedido]):
            result = await session.execute(
                select(Pedido).where(Pedido.nunota == nunota)
            )
        else:
            kwargs['nunota'] = nunota
            if id_pedido:
                result = await session.execute(
                    select(Pedido).where(Pedido.id_pedido == id_pedido)
                )
            if num_pedido:
                result = await session.execute(
                    select(Pedido).where(Pedido.num_pedido == num_pedido)
                )
                
        pedidos = result.scalars().all()
        if not pedidos:
            print(f"Pedido não encontrado. Parâmetro: {id_pedido or num_pedido or nunota}")
            return False
        
        for pedido in pedidos:                
            for key, value in kwargs.items():
                setattr(pedido, key, value)
            
        await session.commit()
        return True

async def cancelar(nunota:int):
            
    async with AsyncSessionLocal() as session:
        try:
            result = await session.execute(
                select(Pedido).where(Pedido.nunota == nunota)
            )                
            pedidos = result.scalars().all()
            if not pedidos:
                print(f"Pedido não encontrado. Parâmetro: {nunota}")
                return False
            
            for pedido in pedidos:
                setattr(pedido, 'nunota', None)
                setattr(pedido, 'dh_importacao', None)
                setattr(pedido, 'dh_confirmacao', None)
                setattr(pedido, 'dh_faturamento', None)
                
            await session.commit()
            return True
        except Exception as e:
            print(f"Erro ao cancelar pedido {nunota}: {e}")
            return False

async def buscar_importar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).where(Pedido.dh_importacao.is_(None),
                                 Pedido.dh_cancelamento.is_(None),
                                 Pedido.id_separacao.isnot(None),
                                 Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido)
        )
        pedidos = result.scalars().all()
        if not pedidos:
            return []
        dados_pedido = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                        retorno=pedidos)
        return dados_pedido

async def buscar_confirmar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).where(Pedido.dh_importacao.isnot(None),
                                 Pedido.dh_confirmacao.is_(None),
                                 Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido)
        )
        pedidos = result.scalars().all()
        if not pedidos:
            return []
        dados_pedido = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                        retorno=pedidos)
        return dados_pedido

async def buscar_checkout(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).where(Pedido.dh_cancelamento.is_(None),
                                 Pedido.dh_faturamento.is_(None),
                                 Pedido.id_separacao.isnot(None),
                                 Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido)
        )
        pedidos = result.scalars().all()
        dados_pedidos = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                         retorno=pedidos)           
        return dados_pedidos
    
async def buscar_faturar(ecommerce_id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).where(Pedido.dh_confirmacao.isnot(None),
                                 Pedido.dh_faturamento.is_(None),
                                 Pedido.ecommerce_id == ecommerce_id).order_by(Pedido.num_pedido)
        )
        pedidos = result.scalars().all()
        dados_pedidos = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                         retorno=pedidos)           
        return dados_pedidos

async def resetar(id_pedido:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido).filter(Pedido.id_pedido == id_pedido)
        )
        pedido = result.scalar_one_or_none()
        if not pedido:
            print(f"Pedido não encontrado. Parâmetro: {id_pedido}")
            return False
        setattr(pedido, "id_separacao", None)
        setattr(pedido, "nunota", None)
        setattr(pedido, "dh_importacao", None)
        setattr(pedido, "dh_confirmacao", None)
        setattr(pedido, "dh_faturamento", None)
        setattr(pedido, "dh_cancelamento", None)
        await session.commit()
        return True
    