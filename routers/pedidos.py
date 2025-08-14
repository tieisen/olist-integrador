from fastapi import APIRouter
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
    if not asyncio.run(faturamento.venda_entre_empresas()):
        return False
    if not asyncio.run(pedido.faturar()):
        return False
    if not asyncio.run(nota.emitir()):
        return False
    if not asyncio.run(separacao.checkout()):
        return False
    if not asyncio.run(nota.confirmar()):
        return False
    if not asyncio.run(nota.baixar_financeiro()):
        return False
    return True