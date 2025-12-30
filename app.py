import os
from fastapi import FastAPI
from datetime import datetime
from src.utils.load_env import load_env
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from routers import empresas, estoque, pedidos, produtos, notas, devolucoes, financeiro
from src.scheduler.scheduler import iniciar_agendador, encerrar_agendador
load_env()

api_title:str = os.getenv('API_TITLE')
api_description:str = os.getenv('API_DESCRIPTION')
api_version:str = os.getenv('API_VERSION')
if not any([api_title,api_description,api_version]):
    raise ValueError("API config not found.")

async def startup_event():
    await iniciar_agendador()

async def shutdown_event():
    await encerrar_agendador()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup code
    await startup_event()
    yield
    # Shutdown code
    await shutdown_event()

app = FastAPI(title=api_title,
              description=api_description,
              version=api_version,
              lifespan=lifespan)    

app.add_middleware(
    CORSMiddleware,
    # allow_origin_regex=".*",  # Permite qualquer origem
    allow_origins=["*"],  # Permite qualquer origem
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos os métodos HTTP
    allow_headers=["*"],  # Permite todos os headers
)

app.include_router(empresas.router, prefix="/empresas", tags=["Empresas"])
app.include_router(estoque.router, prefix="/estoque", tags=["Estoque"])
app.include_router(produtos.router, prefix="/produtos", tags=["Produtos"])
app.include_router(pedidos.router, prefix="/pedidos", tags=["Pedidos"])
app.include_router(notas.router, prefix="/notas", tags=["Notas"])
app.include_router(financeiro.router, prefix="/financeiro", tags=["Financeiro"])
app.include_router(devolucoes.router, prefix="/devolucoes", tags=["Devoluções"])

@app.get("/",include_in_schema=False)
def read_root():
    return {"message": f"{api_title}. Version {api_version}."}

print(f"\n====================================")
print(f"===> START AT: {datetime.now().strftime("%d/%m/%Y, %H:%M:%S")}")
print(f"====================================\n")