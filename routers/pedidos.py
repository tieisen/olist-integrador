from fastapi import APIRouter, HTTPException, status
from src.integrador.pedido import Pedido
from src.integrador.faturamento import Faturamento
from src.integrador.nota import Nota
from src.integrador.separacao import Separacao
import asyncio

router = APIRouter()
pedido = Pedido()
faturamento = Faturamento()
separacao = Separacao()
nota = Nota()

@router.get("/")
def default():
    return {"message": "Pedidos"}

@router.get("/buscar")
def buscar_pedidos_olist():    
    asyncio.run(pedido.receber())
    asyncio.run(pedido.validar_cancelamentos())
    return True

@router.get("/integrar")
def integrar_pedidos_olist():
    asyncio.run(pedido.importar())
    asyncio.run(separacao.receber())
    asyncio.run(pedido.confirmar())
    return True

@router.get("/faturar")
def faturar_pedidos_snk():    
    if not asyncio.run(faturamento.faturar_lote()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos")
    return True