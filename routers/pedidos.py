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
    Importa os pedidos pendentes do Olist.
    Confirma o pedido no Sankhya.
    """
    if not asyncio.run(pedido.receber_novos()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao receber pedidos")
    if not asyncio.run(separacao.receber()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao buscar separação dos pedidos")
    if not asyncio.run(pedido.integrar_novos()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao importar pedidos")
    if not asyncio.run(pedido.integrar_confirmacao()):
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
    if not asyncio.run(faturamento.integrar_olist()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos no Olist")
    if not asyncio.run(faturamento.integrar_snk()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos no Sankhya")
    return True

@router.get("/faturar/olist")
def faturar_olist():
    """
    Emite e autoriza as NFs dos pedidos no Olist.
    Envia os pedidos para separação no Olist.
    """    
    if not asyncio.run(faturamento.integrar_olist()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos no Olist")
    return True

@router.get("/faturar/sankhya")
def faturar_sankhya():
    """
    Cria e confirma a nota de transferência no Sankhya.
    Fatura o pedido no Sankhya.
    Confirma a nota gerada no Sankhya.
    """    
    if not asyncio.run(faturamento.integrar_snk()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos no Sankhya")
    return True

@router.get("/faturar/venda-interna")
def faturar_venda_interna():
    """
    Cria e confirma a nota de transferência no Sankhya.
    """    
    if not asyncio.run(faturamento.realizar_venda_interna()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao criar venda interna")
    return True

@router.get("/receber/{numero}")
def receber_pedido(numero:int):
    """
    Busca um pedido específico do Olist
    """
    if not asyncio.run(pedido.receber(num_pedido=numero)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro ao buscar pedido {numero}")
    return True

@router.get("/separacao/buscar")
def buscar_separacao():
    """
    Busca um pedido específico do Olist
    """
    if not asyncio.run(separacao.receber()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao buscar separação dos pedidos")
    return True

@router.get("/anular/{nunota}")
def anular_pedidos(nunota: int):
    """
    Exclui pedido não faturado do Sankhya
    """
    if not asyncio.run(pedido.integrar_cancelamento(nunota=nunota)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao anular pedido")
    return True