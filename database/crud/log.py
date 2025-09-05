from database.database import AsyncSessionLocal
from datetime import datetime, timedelta
from database.models import Log, LogEstoque, LogPedido, LogProduto
from sqlalchemy.future import select
import os
from dotenv import load_dotenv

load_dotenv('keys/.env')

async def criar(empresa_id:int, de:str, para:str, contexto:str):
    async with AsyncSessionLocal() as session:
        novo_log = Log(empresa_id=empresa_id,
                       de=de,
                       para=para,
                       contexto=contexto)
        session.add(novo_log)
        await session.commit()
        await session.refresh(novo_log)
    return novo_log.id

async def atualizar(id:int, sucesso:bool=None):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Log)
            .where(Log.id == id)
        )
        log = result.scalar_one_or_none()
        if not log:
            print(f"Log não encontrado. Parâmetro: {id}")
            return False
        
        if sucesso is None:
            sucesso = await validar_sucesso_pelo_historico(id)

        setattr(log, "sucesso", sucesso)
        await session.commit()
        await session.refresh(log)
        return True
    
async def validar_sucesso_pelo_historico(id:int):
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Log)
            .where(Log.id == id)
        )
        log = result.scalar_one_or_none()
        if not log:
            print(f"Log não encontrado. Parâmetro: {id}")
            return False
        
        contexto = None
        match log.contexto:
            case c if 'pedido' in c:
                contexto = LogPedido
            case 'produto':
                contexto = LogProduto
            case 'estoque':
                contexto = LogEstoque

        if not contexto:
            print(f"Contexto não encontrado. Parâmetro: {log.contexto}")
            return False

        result = await session.execute(
            select(contexto)
            .where(contexto.log_id == id,
                   contexto.sucesso.is_(False))
        )
        falhas_do_contexto = result.scalars().all()
        return False if falhas_do_contexto else True

async def buscar_falhas(empresa_id:int):    
    try:
        tempo_limite = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(minutes=int(os.getenv('TEMPO_HISTORICO_MINUTOS')))
    except Exception as e:
        print("Erro ao calcular tempo limite para busca do histórico de falhas")
        return False
    finally:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Log)
                .where(Log.sucesso.isnot(True),
                    Log.dh_execucao >= tempo_limite,
                    Log.empresa_id == empresa_id)
                .order_by(Log.dh_execucao)
            )
            log = result.scalars().all()
            return log
        
async def listar_falhas(empresa_id:int=None,logs:list=None):
    lista_falhas = []

    if not any([empresa_id, logs]):
        print("Nenhum parâmetro informado para busca de falhas")
        return False
    
    if not logs:
        logs = await buscar_falhas(empresa_id=empresa_id)
    
    async with AsyncSessionLocal() as session:
        for l in logs:
            match l.contexto:
                case c if 'pedido' in c:
                    contexto = LogPedido
                case 'produto':
                    contexto = LogProduto
                case 'estoque':
                    contexto = LogEstoque

            if not contexto:
                print(f"Contexto não encontrado. Parâmetro: {l.contexto}")
                return False

            result = await session.execute(
                select(contexto)
                .where(contexto.log_id == l.id,
                       contexto.sucesso.is_(False))
            )
            lista_falhas.append({
                "contexto": l.contexto,
                "de": l.de,
                "para": l.para,
                "falhas": result.scalars().all()})
        
        return lista_falhas
  