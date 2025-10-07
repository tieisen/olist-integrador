from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from sqlalchemy import create_engine
from pytz import timezone
from src.scheduler.jobs import produtos, estoque, pedidos, devolucoes, notificar, limpar_cache
from src.utils.log import set_logger
from src.utils.load_env import load_env
import os
load_env()
logger = set_logger(__name__)


# Engine síncrono apenas para APScheduler
job_engine = create_engine(os.getenv('ALEMBIC_URL'))

jobstores = {
    "default": SQLAlchemyJobStore(engine=job_engine)
}

scheduler = AsyncIOScheduler(jobstores=jobstores, timezone=timezone("America/Sao_Paulo"))

def inicializar_tarefas():
    jobs_existentes = [job.id for job in scheduler.get_jobs()]

    async def rotina_produtos_estoque():
        await produtos.integrar_produtos()
        await estoque.integrar_estoque()

    # if "sincronizar_produtos" not in jobs_existentes:
    #     scheduler.add_job(produtos.integrar_produtos(), "interval", minutes=10, id="sincronizar_produtos")
    
    # if "sincronizar_estoque" not in jobs_existentes:
    #     scheduler.add_job(estoque.integrar_estoque(), "interval", minutes=10, id="sincronizar_estoque")
    
    if "sincronizar_produtos_estoque" not in jobs_existentes:
        scheduler.add_job(estoque.rotina_produtos_estoque(), "interval", minutes=10, id="sincronizar_produtos_estoque")

    if "sincronizar_pedidos" not in jobs_existentes:
        scheduler.add_job(pedidos.receber_pedido_lote(), "interval", minutes=15, id="sincronizar_pedidos")

    if "sincronizar_devolucoes" not in jobs_existentes:
        scheduler.add_job(devolucoes.integrar_devolucoes(), "cron", hour=19, id="sincronizar_devolucoes")

    if "notificar_erros" not in jobs_existentes:
        scheduler.add_job(notificar.enviar_notificacao(), "interval", hour=4, id="notificar_erros")

    if "limpar_cache" not in jobs_existentes:
        scheduler.add_job(limpar_cache.excluir_cache(), "cron", day='1,15', hour=23, id="limpar_cache")

def iniciar_agendador():
    inicializar_tarefas()
    scheduler.start()
    logger.info("✅ APScheduler iniciado")

def encerrar_agendador():
    scheduler.shutdown(wait=False)
    logger.info("🛑 APScheduler encerrado")
