from fastapi import APIRouter
from src.integrador.pedido import Pedido
from src.integrador.faturamento import Faturamento
import asyncio

router = APIRouter()

@router.get("/")
def default():
    return {"message": "Pedidos"}

@router.get("/buscar")
def buscar_pedidos_olist():
    pedido = Pedido()
    asyncio.run(pedido.receber_pedidos())
    asyncio.run(pedido.validar_cancelamentos())
    return True

@router.get("/integrar")
def integrar_pedidos_olist():
    pedido = Pedido()
    asyncio.run(pedido.importar_novos())
    asyncio.run(pedido.confirmar())
    return True

@router.get("/faturar")
def faturar_pedidos_snk():
    pedido = Pedido()
    faturamento = Faturamento()
    ack = asyncio.run(faturamento.venda_entre_empresas())
    if ack:
        asyncio.run(pedido.faturar())
        return True
    else:
        return False