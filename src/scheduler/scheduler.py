from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from sqlalchemy import create_engine
from pytz import timezone
from datetime import datetime, timedelta
from src.scheduler.jobs import produtos, estoque, pedidos, devolucoes, notificar, limpar_cache, financeiro
from src.utils.log import set_logger
from src.utils.load_env import load_env
import os
load_env()
logger = set_logger(__name__)

# CONFIGURAÇÃO DO JOBSTORE
DATABASE_URL = os.getenv("ALEMBIC_URL")
DB_NAME = os.getenv("DB_NAME")
if not all([DATABASE_URL,DB_NAME]):
    raise FileNotFoundError("DATABASE_URL e/ou DB_NAME não encontados no arquivo de ambiente")
db_url = DATABASE_URL+DB_NAME

job_engine = create_engine(db_url)

jobstores = {
    "default": SQLAlchemyJobStore(engine=job_engine)
}

scheduler = AsyncIOScheduler(jobstores=jobstores, timezone=timezone("America/Sao_Paulo"))

# DEFINIÇÃO DAS TAREFAS
async def rotina_produtos():
    logger.info("Iniciando sincronização de produtos...")
    try:
        await produtos.integrar_produtos()
        logger.info("Rotina de produtos concluída.")
    except Exception as e:
        logger.exception(f"Erro na rotina produtos: {e}")

async def rotina_estoque():
    logger.info("Iniciando sincronização de estoque...")
    try:
        await estoque.integrar_estoque()
        logger.info("Rotina de estoque concluída.")
    except Exception as e:
        logger.exception(f"Erro na rotina estoque: {e}")

async def rotina_pedidos():
    logger.info("Iniciando sincronização de pedidos...")
    try:
        await pedidos.receber_pedido_lote()
        logger.info("Rotina de pedidos concluída.")
    except Exception as e:
        logger.exception(f"Erro na rotina de pedidos: {e}")

async def rotina_completa():
    await rotina_produtos()
    await rotina_estoque()
    await rotina_pedidos()

async def rotina_financeiro_shopee():
    logger.info("Iniciando busca dos recebimentos da Shopee...")
    try:
        await financeiro.integrar_recebimentos_shopee()
        logger.info("Rotina de recebimentos Shopee concluída.")
    except Exception as e:
        logger.exception(f"Erro na rotina de recebimentos Shopee: {e}")

async def rotina_devolucoes():
    logger.info("Iniciando sincronização de devoluções...")
    try:
        await devolucoes.integrar_devolucoes()
        logger.info("Rotina de devoluções concluída.")
    except Exception as e:
        logger.exception(f"Erro na rotina de devoluções: {e}")

async def rotina_notificacao():
    logger.info("Enviando notificações de erro...")
    try:
        await notificar.enviar_notificacao()
        logger.info("Notificação enviada.")
    except Exception as e:
        logger.exception(f"Erro ao enviar notificação: {e}")

async def rotina_cache():
    logger.info("Limpando cache antigo...")
    try:
        await limpar_cache.excluir_cache()
        logger.info("Cache limpo com sucesso.")
    except Exception as e:
        logger.exception(f"Erro ao limpar cache: {e}")

# LISTENER DE EVENTOS
RETRY_MINUTES = 5

def job_listener(event):
    job = scheduler.get_job(event.job_id)
    if not job:
        return

    if event.exception:
        logger.error(f"Job '{job.id}' falhou às {datetime.now():%H:%M:%S}: {event.exception}")

        retry_id = f"{job.id}_retry"
        # Evita múltiplos retries acumulados
        if scheduler.get_job(retry_id):
            logger.warning(f"Retry já agendado para job '{job.id}', ignorando novo retry.")
            return

        # agenda nova tentativa
        run_time = datetime.now() + timedelta(minutes=RETRY_MINUTES)
        scheduler.add_job(
            job.func,
            trigger="date",
            run_date=run_time,
            id=retry_id,
            replace_existing=True,
            misfire_grace_time=60,
            max_instances=1,
            coalesce=True
        )
        logger.info(f"Reagendado retry de '{job.id}' para {run_time:%H:%M:%S}")
    else:
        logger.info(f"Job '{job.id}' executado com sucesso às {datetime.now():%H:%M:%S}")

scheduler.add_listener(job_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

# INICIALIZAÇÃO DE TAREFAS
async def inicializar_tarefas():
    jobs_existentes = [job.id for job in scheduler.get_jobs()]

    jobs = [
        ("sincronizar_tudo", rotina_completa, "cron", {"minute": "*/10", "hour": "0-5,10-23"}),        
        ("notificar_erros", rotina_notificacao, "cron", {"hour": "12,17"}),
        ("limpar_cache", rotina_cache, "cron", {"day": "1,15", "hour": 23}),
        ("financeiro_shopee", rotina_financeiro_shopee, "cron", {"hour": 7}),
        ("devolucoes", rotina_devolucoes, "cron", {"hour": 12, "minute": 30})
    ]

    for job_id, func, trigger, params in jobs:
        if job_id not in jobs_existentes:
            scheduler.add_job(func, trigger, id=job_id, replace_existing=True, max_instances=1, coalesce=True, **params)
            # logger.info(f"Job registrado: {job_id}")

# CONTROLE DO AGENDADOR
async def iniciar_agendador():
    await inicializar_tarefas()
    scheduler.start()
    logger.info("APScheduler iniciado e monitorando tarefas.")
    print("APScheduler iniciado e monitorando tarefas.")

async def encerrar_agendador():
    scheduler.shutdown(wait=False)
    logger.info("APScheduler encerrado.")    
    print("APScheduler encerrado.")
