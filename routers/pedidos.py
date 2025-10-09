from fastapi import APIRouter, HTTPException, status
from src.integrador.pedido import Pedido
from src.scheduler.jobs.pedidos import integrar_pedidos, receber_pedido_lote, receber_pedido_unico, integrar_separacoes
from src.scheduler.jobs.faturamento import integrar_faturamento, integrar_faturamento_olist, integrar_faturamento_snk, integrar_venda_interna
import asyncio
from pydantic import BaseModel

router = APIRouter()

class PedidoModel(BaseModel):
    codemp:int
    id_loja:int
    numero:int
    nunota:int

@router.post("/integrar-lote")
def integrar(pedido:PedidoModel):
    """
    Busca os pedidos novos do Olist que estão com status Preparando Envio.
    Importa os pedidos pendentes do Olist.
    Confirma o pedido no Sankhya.
    """
    res:dict={}
    res = asyncio.run(receber_pedido_lote(codemp=pedido.codemp))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar pedidos")
    res = asyncio.run(integrar_pedidos(codemp=pedido.codemp))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar pedidos")
    return True

@router.post("/integrar-loja")
def integrar_loja(pedido:PedidoModel):
    """
    Busca os pedidos novos do Olist, por E-commerce, que estão com status Preparando Envio.
    Importa os pedidos pendentes do Olist.
    Confirma o pedido no Sankhya.
    """
    res:dict={}
    res = asyncio.run(receber_pedido_lote(id_loja=pedido.id_loja))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar pedidos")
    res = asyncio.run(integrar_pedidos(id_loja=pedido.id_loja))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar pedidos")
    return True

@router.post("/faturar-lote")
def faturar(pedido:PedidoModel):
    """
    Emite e autoriza as NFs dos pedidos no Olist.
    Envia os pedidos para separação no Olist.
    Cria e confirma a nota de transferência no Sankhya.
    Fatura o pedido no Sankhya.
    Confirma a nota gerada no Sankhya.
    """
    res:dict={}
    res = asyncio.run(integrar_faturamento(codemp=pedido.codemp))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos")
    return True

@router.post("/faturar-loja")
def faturar_loja(pedido:PedidoModel):
    """
    Emite e autoriza as NFs dos pedidos no Olist, por E-commerce.
    Envia os pedidos para separação no Olist.
    Cria e confirma a nota de transferência no Sankhya.
    Fatura o pedido no Sankhya.
    Confirma a nota gerada no Sankhya.
    """
    res:dict={}
    res = asyncio.run(integrar_faturamento(id_loja=pedido.id_loja))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos")
    return True

@router.post("/faturar/olist")
def faturar_olist(pedido:PedidoModel):
    """
    Emite e autoriza as NFs dos pedidos no Olist.
    Envia os pedidos para separação no Olist.
    """
    res:dict={}
    res = asyncio.run(integrar_faturamento_olist(codemp=pedido.codemp))    
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos no Olist")
    return True

@router.post("/faturar-loja/olist")
def faturar_olist_loja(pedido:PedidoModel):
    """
    Emite e autoriza as NFs dos pedidos no Olist, por E-commerce.
    Envia os pedidos para separação no Olist.
    """
    res:dict={}
    res = asyncio.run(integrar_faturamento_olist(id_loja=pedido.id_loja))    
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos no Olist")
    return True

@router.post("/faturar/snk")
def faturar_sankhya(pedido:PedidoModel):
    """
    Cria e confirma a nota de transferência no Sankhya.
    Fatura o pedido no Sankhya.
    Confirma a nota gerada no Sankhya.
    """    
    res:dict={}
    res = asyncio.run(integrar_faturamento_snk(codemp=pedido.codemp))    
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos no Sankhya")
    return True

@router.post("/faturar/venda-interna")
def faturar_venda_interna(pedido:PedidoModel):
    """
    Cria e confirma a nota de transferência no Sankhya.
    """
    res:dict={}
    res = asyncio.run(integrar_venda_interna(codemp=pedido.codemp))    
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao criar venda interna")
    return True

@router.post("/receber")
def receber_pedido(pedido:PedidoModel):
    """
    Busca um pedido específico do Olist
    """
    res:dict={}
    res = asyncio.run(receber_pedido_unico(id_loja=pedido.id_loja,numero=pedido.numero)) 
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro ao buscar pedido {pedido.numero}")
    return True

@router.post("/separacao/buscar")
def buscar_separacao(pedido:PedidoModel):
    """
    Busca as separações pendentes no Olist
    """
    res:dict={}
    res = asyncio.run(integrar_separacoes(codemp=pedido.codemp)) 
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao buscar separação dos pedidos")
    return True

@router.post("/anular")
def anular_pedidos(pedido:PedidoModel):
    """
    Exclui pedido não faturado do Sankhya
    """
    _pedido = Pedido(codemp=pedido.codemp)
    if not asyncio.run(_pedido.anular_pedido_importado(nunota=pedido.nunota)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao anular pedido")
    return True