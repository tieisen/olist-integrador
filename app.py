from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from routers import estoque, pedidos, produtos, notas, devolucoes

app = FastAPI(title="Integrador Olist x Sankhya",
              description="Integrador Olist x Sankhya",
              version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permite qualquer origem
    allow_credentials=True,
    allow_methods=["*"],  # Permite todos os métodos HTTP
    allow_headers=["*"],  # Permite todos os headers
)

app.include_router(produtos.router, prefix="/produtos", tags=["Produtos"])
app.include_router(estoque.router, prefix="/estoque", tags=["Estoque"])
app.include_router(pedidos.router, prefix="/pedidos", tags=["Pedidos"])
app.include_router(notas.router, prefix="/notas", tags=["Notas"])
app.include_router(devolucoes.router, prefix="/devolucoes", tags=["Devoluções"])

@app.get("/",include_in_schema=False)
def read_root():
    return {"message": "Integrador Olist x Sankhya"}