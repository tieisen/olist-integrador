from fastapi import APIRouter, HTTPException, status
from src.integrador.pedido import Pedido
from src.integrador.faturamento import Faturamento
from src.integrador.separacao import Separacao
import asyncio

router = APIRouter()
pedido = Pedido()
faturamento = Faturamento()
separacao = Separacao()

@router.get("/")
def default():
    return {"message": "Pedidos"}

@router.get("/integrar")
def integrar_pedidos():
    """
    Busca os pedidos novos do Olist que estão com status Preparando Envio.
    Valida pedidos cancelados.
    Importa os pedidos pendentes do Olist em um único pedido no Sankhya.
    Confirma o pedido no Sankhya.
    """
    if not asyncio.run(pedido.receber()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao receber pedidos")
    if not asyncio.run(pedido.validar_cancelamentos()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao validar cancelamentos")
    if not asyncio.run(separacao.receber()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao buscar separação dos pedidos")
    if not asyncio.run(pedido.importar_lote()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao importar pedidos")
    if not asyncio.run(pedido.confirmar_lote()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao confirmar pedidos")
    return True

@router.get("/faturar")
def faturar_pedidos():
    """
    Emite e autoriza as NFs dos pedidos no Olist.
    Envia os pedidos para separação no Olist.
    Cria e confirma a nota de transferência no Sankhya.
    Fatura o pedido no Sankhya.
    Confirma a nota gerada no Sankhya.
    """    
    if not asyncio.run(faturamento.faturar_lote()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos")
    return True

@router.get("/receber/{numero}")
def receber_pedido(numero:int):
    """
    Busca um pedido específico do Olist
    """
    if not asyncio.run(pedido.receber(num_pedido=numero)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro ao buscar pedido {numero}")
    return True