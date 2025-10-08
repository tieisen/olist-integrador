from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
from routers import empresas, estoque, pedidos, produtos, notas, devolucoes
from src.scheduler.scheduler import iniciar_agendador, encerrar_agendador

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

app = FastAPI(title="Integrador Olist x Sankhya",
              description="Integrador Olist x Sankhya",
              version="1.0",
              lifespan=lifespan)    

app.add_middleware(
    CORSMiddleware,
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
app.include_router(devolucoes.router, prefix="/devolucoes", tags=["Devoluções"])

@app.get("/",include_in_schema=False)
def read_root():
    return {"message": "Integrador Olist x Sankhya"}