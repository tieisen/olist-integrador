from database.database import AsyncSessionLocal
from database.models import Nota, Pedido
from database.crud import pedido
from sqlalchemy.future import select
from src.utils.db import validar_dados, formatar_retorno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

COLUNAS_CRIPTOGRAFADAS = None

async def criar(id_pedido:int,id_nota:int,numero:int,serie:str,**kwargs) -> bool:
    if kwargs:
        kwargs = validar_dados(modelo=Nota,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False   

    # Verifica se a nota já existe
    nota = await buscar(id_nota=id_nota)
    if nota:
        print(f"Nota {id_nota} já existe")
        return False
    
    # Verifica se o pedido existe
    dados_pedido:list[dict] = await pedido.buscar(id_pedido=id_pedido)
    if not dados_pedido:
        print(f"Pedido {id_pedido} não encontrado")
        return False    
    
    # Verifica se o pedido já tem nota
    pedido_atendido = await validar_pedido_atendido(id_pedido=id_pedido)
    if pedido_atendido:
        print(f"Pedido {id_pedido} já foi atendido na nota {pedido_atendido.numero}")
        return False

    async with AsyncSessionLocal() as session:      
        nova_nota = Nota(pedido_id=dados_pedido[0].get('id'),
                         id_nota=id_nota,
                         numero=numero,
                         serie=serie,
                         **kwargs)
        session.add(nova_nota)
        await session.commit()
    return True

async def buscar(id_nota:int=None,nunota:int=None,numero_ecommerce:dict=None,chave_acesso:str=None,cod_pedido:str=None,tudo:bool=False,) -> dict:
    res:dict={}

    try:
        if not any([id_nota,nunota,chave_acesso,cod_pedido,numero_ecommerce]) and not tudo:
            raise ValueError("Parâmetro não informado")
                
        async with AsyncSessionLocal() as session:
            if nunota:
                result = await session.execute(
                    select(Nota)
                    .where(Nota.nunota == nunota)
                )
            elif id_nota:
                result = await session.execute(
                    select(Nota)
                    .where(Nota.id_nota == id_nota)
                )
            elif chave_acesso:
                result = await session.execute(
                    select(Nota)
                    .where(Nota.chave_acesso == chave_acesso)
                )
            elif numero_ecommerce:
                result = await session.execute(
                    select(Nota)
                    .where(Nota.numero == numero_ecommerce.get('numero'),
                        Nota.pedido_
                            .has(Pedido.ecommerce_id==numero_ecommerce.get('ecommerce')))
                )
            elif cod_pedido:
                result = await session.execute(
                    select(Nota)
                    .where(Nota.pedido_
                               .has(Pedido.cod_pedido==cod_pedido))
                )
            elif tudo:
                result = await session.execute(
                    select(Nota)
                )
            else:
                raise ValueError("Parâmetro não informado")            
            nota = result.scalars().all()

        if not nota:
            raise ValueError("Nenhum resultado encontrado")
        
        res = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                               retorno=nota)
    except:
        pass
    finally:
        pass
    return res

async def atualizar(id_nota:int=None,chave_acesso:str=None,nunota_pedido:int=None,nunota_nota:int=None,cod_pedido:int=None,**kwargs) -> bool:

    if not any([id_nota,chave_acesso,nunota_pedido,nunota_nota,cod_pedido]):
        print("Nenhum parâmetro informado")
        return False

    if kwargs:
        kwargs = validar_dados(modelo=Nota,
                               kwargs=kwargs,
                               colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not kwargs:
            return False
            
    async with AsyncSessionLocal() as session:
        if chave_acesso and not any([id_nota,nunota_pedido,cod_pedido]):
            result = await session.execute(
                select(Nota)
                .where(Nota.chave_acesso == chave_acesso)
            )
        else:
            if chave_acesso:
                kwargs['chave_acesso'] = chave_acesso
            if cod_pedido:
                if id_nota:
                    kwargs['id_nota'] = id_nota
                result = await session.execute(
                    select(Nota)
                    .where(Nota.pedido_.has(Pedido.cod_pedido == cod_pedido),
                           Nota.dh_cancelamento.is_(None))
                )
            elif id_nota:
                result = await session.execute(
                    select(Nota)
                    .where(Nota.id_nota == id_nota)
                )
            elif nunota_pedido == -1:
                kwargs['nunota'] = nunota_pedido
                result = await session.execute(
                    select(Nota)
                    .where(Nota.nunota.is_(None),
                           Nota.pedido_.has(Pedido.nunota == nunota_pedido))
                )            
            elif nunota_pedido:
                result = await session.execute(
                    select(Nota)
                    .where(Nota.pedido_.has(Pedido.nunota == nunota_pedido))
                )            
            elif nunota_nota and kwargs.get('baixa_estoque_ecommerce'):
                result = await session.execute(
                    select(Nota)
                    .where(Nota.nunota == nunota_nota,
                           Nota.baixa_estoque_ecommerce.is_(False))
                )
            elif nunota_nota:                
                result = await session.execute(
                    select(Nota)
                    .where(Nota.nunota == nunota_nota)
                )            
            else:
                logger.error("Nenhum parâmetro informado")
                return False

        notas = result.scalars().all()
        if not notas:
            logger.error(f"Nota não encontrada. Parâmetro: {id_nota or chave_acesso or nunota_pedido or nunota_nota or cod_pedido}")
            return False
        
        try:
            for nota in notas:
                for key, value in kwargs.items():
                    setattr(nota, key, value)
        except Exception as e:
            logger.error(f"Erro ao atualizar nota: {e}")
            return False
            
        await session.commit()
        return True  

async def salvar_dados_conta_shopee(cod_pedido:str,dados_conta:dict) -> bool:

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.dh_cancelamento.is_(None),                   
                   Nota.pedido_.has(Pedido.cod_pedido == cod_pedido))
        )
        nota = result.scalar_one_or_none()
        if not nota:
            logger.error(f"Nota do pedido {cod_pedido} não encontrada.")
            return False
        elif nota.dh_baixa_financeiro:
            logger.info(f"Conta do pedido {cod_pedido} já foi baixada.")
            return True
        elif nota.income_data:
            logger.info(f"Conta do pedido {cod_pedido} já foi importada.")
            return True
        else:
            try:
                setattr(nota, 'income_data', dados_conta)
            except Exception as e:
                logger.error(f"Erro ao importar dados da conta do pedido {cod_pedido}: {e}")
                return False            
            await session.commit()
            return True  

async def validar_pedido_atendido(id_pedido:int) -> bool:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.dh_cancelamento.is_(None),
                   Nota.pedido_.has(Pedido.id_pedido == id_pedido))
        )
        pedido_atendido = result.scalar_one_or_none()
    return True if pedido_atendido else False    

async def buscar_criar(ecommerce_id:int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Pedido)
            .where(Pedido.nunota.isnot(None),
                   Pedido.dh_faturamento.isnot(None),
                   Pedido.ecommerce_id == ecommerce_id,
                   ~Pedido.nota_.any())
            .order_by(Pedido.num_pedido)
        )
        notas = result.scalars().all()
    dados_nota = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                  retorno=notas)
    return dados_nota    
    
async def buscar_emitir(ecommerce_id:int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.id.isnot(None),
                   Nota.dh_emissao.is_(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
            .order_by(Nota.id)
        )
        notas = result.scalars().all()
    dados_nota = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                  retorno=notas)
    return dados_nota 

async def buscar_financeiro(ecommerce_id:int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.dh_cancelamento.is_(None),
                   Nota.id_financeiro.is_(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
        )
        notas = result.scalars().all()
    dados_nota = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                  retorno=notas)
    return dados_nota 

async def buscar_financeiro_parcelado(ecommerce_id:int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.dh_cancelamento.is_(None),
                   Nota.dh_baixa_financeiro.is_(None),
                   Nota.parcelado.is_(True),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
        )
        notas = result.scalars().all()
    dados_nota = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                  retorno=notas)
    return dados_nota 

async def buscar_financeiro_baixar(ecommerce_id:int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.dh_cancelamento.is_(None),
                   Nota.id_financeiro.isnot(None),
                   Nota.dh_baixa_financeiro.is_(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
        )
        notas = result.scalars().all()
    dados_nota = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                  retorno=notas)
    return dados_nota 

async def buscar_financeiro_baixar_shopee(ecommerce_id:int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.dh_cancelamento.is_(None),
                   Nota.id_financeiro.isnot(None),
                   Nota.dh_baixa_financeiro.is_(None),
                   Nota.income_data.isnot(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
            .order_by(Nota.numero)
        )
        notas = result.scalars().all()
    dados_nota = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                  retorno=notas)
    return dados_nota 

async def buscar_atualizar_nunota(ecommerce_id:int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.nunota.is_(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
        )
        notas = result.scalars().all()
    dados_nota = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                  retorno=notas)
    return dados_nota 

async def buscar_confirmar(ecommerce_id:int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.nunota.isnot(None),
                   Nota.dh_cancelamento.is_(None),
                   Nota.dh_confirmacao.is_(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
        )
        notas = result.scalars().all()
    dados_nota = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                  retorno=notas)
    return dados_nota 

async def buscar_cancelar(ecommerce_id:int) -> list[dict]:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Nota)
            .where(Nota.cancelado_sankhya.is_(False),
                   Nota.dh_cancelamento.isnot(None),
                   Nota.pedido_.has(Pedido.ecommerce_id == ecommerce_id))
        )
        notas = result.scalars().all()
    dados_nota = formatar_retorno(colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS,
                                  retorno=notas)
    return dados_nota 
