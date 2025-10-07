from database.database import AsyncSessionLocal
from datetime import datetime, timedelta
from database.models import Log
from database.crud import log_produto, log_estoque, log_pedido
from sqlalchemy.future import select
from src.utils.db import formatar_retorno
from src.utils.log import set_logger
from src.utils.load_env import load_env
import os
load_env()
logger = set_logger(__name__)

COLUNAS_CRIPTOGRAFADAS = None

async def criar(
        empresa_id:int,
        de:str,
        para:str,
        contexto:str
    ) -> int:
    async with AsyncSessionLocal() as session:
        novo_log = Log(empresa_id=empresa_id,
                       de=de,
                       para=para,
                       contexto=contexto)
        session.add(novo_log)
        await session.commit()
        await session.refresh(novo_log)
    return novo_log.id

async def atualizar(
        id:int,
        sucesso:bool=None
    ) -> bool:
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

async def buscar(id:int) -> dict:
    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Log)
            .where(Log.id == id)
        )
        log = result.scalar_one_or_none()
    dados_log = formatar_retorno(retorno=log,
                                 colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
    return dados_log

async def validar_sucesso_pelo_historico(id:int) -> bool:

    dados_log = await buscar(id)
    if not dados_log:
        print(f"Log não encontrado. Parâmetro: {id}")
        return False
    
    contexto = None
    match dados_log.get('contexto'):
        case c if 'pedido' in c:
            contexto = log_pedido
        case 'produto':
            contexto = log_produto
        case 'estoque':
            contexto = log_estoque

    if not contexto:
        print(f"Contexto não encontrado. Parâmetro: {dados_log.get('contexto')}")
        return False
    
    falhas_do_contexto = await contexto.buscar_falhas(log_id=dados_log.get('id'))
    return False if falhas_do_contexto else True

async def buscar_falhas(empresa_id:int) -> list[dict]:    
    try:
        tempo_limite = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(minutes=int(os.getenv('TEMPO_HISTORICO_MINUTOS')))
    except Exception as e:
        print(f"Erro ao calcular tempo limite para busca do histórico de falhas. {e}")
        return []
    finally:
        async with AsyncSessionLocal() as session:
            result = await session.execute(
                select(Log)
                .where(Log.sucesso.isnot(True),
                       Log.dh_execucao >= tempo_limite,
                       Log.empresa_id == empresa_id)
                .order_by(Log.dh_execucao.desc())
            )
            logs = result.scalars().all()
        dados_logs = formatar_retorno(retorno=logs,colunas_criptografadas=COLUNAS_CRIPTOGRAFADAS)
        if not dados_logs:
            dados_logs = []
        elif not isinstance(dados_logs,list):
            dados_logs = [dados_logs]
        else:
            #dados_logs = []
            pass
        return dados_logs
        
async def listar_falhas(
        empresa_id:int=None,
        logs:list=None
    ) -> list[dict]:
    lista_falhas = []

    if not any([empresa_id, logs]):
        print("Nenhum parâmetro informado para busca de falhas")
        return False
    
    if not logs:
        logs = await buscar_falhas(empresa_id=empresa_id)    
    
    l:dict={}
    for l in logs:
        contexto = None
        match l.get('contexto'):
            case c if 'pedido' in c:
                contexto = log_pedido
            case c if 'separacao' in c:
                contexto = log_pedido
            case c if 'produto' in c:
                contexto = log_produto
            case c if 'estoque' in c:
                contexto = log_estoque

        if not contexto:
            print(f"Contexto não encontrado. Parâmetro: {l.get('contexto')}")
            return False
        
        detalhamento_falhas = await contexto.buscar_falhas(log_id=l.get('id'))

        lista_falhas.append({
            "contexto": l.get('contexto'),
            "de": l.get('de'),
            "para": l.get('para'),
            "hora": l.get('dh_execucao'),
            "falhas": detalhamento_falhas})
    
    return lista_falhas

async def excluir_cache():

    try:
        dias = int(os.getenv('DIAS_LIMPA_CACHE',7))*4
    except Exception as e:
        erro = f"Valor para intervalo de dias do cache não encontrado. {e}"
        logger.error(erro)
        return False

    async with AsyncSessionLocal() as session:
        result = await session.execute(
            select(Log).where(Log.dh_execucao < (datetime.now()-timedelta(days=dias)))
        )
        logs = result.scalars().all()
        if not logs:
            return None
        try:
            for log in logs:
                await session.delete(log)
            await session.commit()
            return True
        except Exception as e:
            logger.error("Erro ao excluir logs do banco de dados: %s", e)
            return False
  