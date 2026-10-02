import os
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from routers import devolucoes, ecommerce, empresas, estoque, financeiro, notas, pedidos, produtos
from src.scheduler.scheduler import encerrar_agendador, iniciar_agendador, scheduler
from src.utils.load_env import load_env

load_env()

api_title: str = os.getenv("API_TITLE")
api_description: str = os.getenv("API_DESCRIPTION")
api_version: str = os.getenv("API_VERSION")
if not any([api_title, api_description, api_version]):
    raise ValueError("API config not found.")


async def startup_event():
    if os.getenv("SCHEDULER_ENABLE", "true").lower() == "true":
        await iniciar_agendador()
    else:
        return


async def shutdown_event():
    if scheduler.running:
        await encerrar_agendador()


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code
    await startup_event()
    yield
    # Shutdown code
    await shutdown_event()


app = FastAPI(title=api_title, description=api_description, version=api_version, lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    # allow_origin_regex=".*",  # Permite qualquer origem
    allow_origins=["*"],  # Permite qualquer origem
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos os métodos HTTP
    allow_headers=["*"],  # Permite todos os headers
)

app.include_router(ecommerce.router, prefix="/ecommerces", tags=["E-commerces"])
app.include_router(empresas.router, prefix="/empresas", tags=["Empresas"])
app.include_router(estoque.router, prefix="/estoque", tags=["Estoque"])
app.include_router(produtos.router, prefix="/produtos", tags=["Produtos"])
app.include_router(pedidos.router, prefix="/pedidos", tags=["Pedidos"])
app.include_router(notas.router, prefix="/notas", tags=["Notas"])
app.include_router(financeiro.router, prefix="/financeiro", tags=["Financeiro"])
app.include_router(devolucoes.router, prefix="/devolucoes", tags=["Devoluções"])


@app.get("/", include_in_schema=False)
def read_root():
    return {"message": f"{api_title}. Version {api_version}."}


print("\n====================================")
print(f"===> START AT: {datetime.now().strftime('%d/%m/%Y, %H:%M:%S')}")
print("====================================\n")
