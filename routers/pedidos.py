from fastapi import APIRouter, HTTPException, status
from src.integrador.pedido import Pedido
from src.scheduler.jobs.pedidos import integrar_pedidos, receber_pedido_lote, receber_pedido_unico, integrar_separacoes
from src.scheduler.jobs.faturamento import integrar_faturamento, integrar_faturamento_olist, integrar_faturamento_snk, integrar_venda_interna
import asyncio

router = APIRouter()

@router.get("/")
def default():
    return {"message": "Pedidos"}

@router.get("/{codemp}/integrar-lote")
def integrar(codemp:int):
    """
    Busca os pedidos novos do Olist que estão com status Preparando Envio.
    Importa os pedidos pendentes do Olist.
    Confirma o pedido no Sankhya.
    """
    res:dict={}
    res = asyncio.run(receber_pedido_lote(codemp=codemp))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar pedidos")
    res = asyncio.run(integrar_pedidos(codemp=codemp))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar pedidos")
    return True

@router.get("/{id_loja}/integrar-loja")
def integrar_loja(id_loja:int):
    """
    Busca os pedidos novos do Olist, por E-commerce, que estão com status Preparando Envio.
    Importa os pedidos pendentes do Olist.
    Confirma o pedido no Sankhya.
    """
    res:dict={}
    res = asyncio.run(receber_pedido_lote(id_loja=id_loja))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar pedidos")
    res = asyncio.run(integrar_pedidos(id_loja=id_loja))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar pedidos")
    return True

@router.get("/{codemp}/faturar-lote")
def faturar(codemp:int):
    """
    Emite e autoriza as NFs dos pedidos no Olist.
    Envia os pedidos para separação no Olist.
    Cria e confirma a nota de transferência no Sankhya.
    Fatura o pedido no Sankhya.
    Confirma a nota gerada no Sankhya.
    """
    res:dict={}
    res = asyncio.run(integrar_faturamento(codemp=codemp))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos")
    return True

@router.get("/{id_loja}/faturar-loja")
def faturar_loja(id_loja:int):
    """
    Emite e autoriza as NFs dos pedidos no Olist, por E-commerce.
    Envia os pedidos para separação no Olist.
    Cria e confirma a nota de transferência no Sankhya.
    Fatura o pedido no Sankhya.
    Confirma a nota gerada no Sankhya.
    """
    res:dict={}
    res = asyncio.run(integrar_faturamento(id_loja=id_loja))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos")
    return True

@router.get("/{codemp}/faturar/olist")
def faturar_olist(codemp:int):
    """
    Emite e autoriza as NFs dos pedidos no Olist.
    Envia os pedidos para separação no Olist.
    """
    res:dict={}
    res = asyncio.run(integrar_faturamento_olist(codemp=codemp))    
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos no Olist")
    return True

@router.get("/{id_loja}/faturar-loja/olist")
def faturar_olist_loja(id_loja:int):
    """
    Emite e autoriza as NFs dos pedidos no Olist, por E-commerce.
    Envia os pedidos para separação no Olist.
    """
    res:dict={}
    res = asyncio.run(integrar_faturamento_olist(id_loja=id_loja))    
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos no Olist")
    return True

@router.get("/{codemp}/faturar/snk")
def faturar_sankhya(codemp:int):
    """
    Cria e confirma a nota de transferência no Sankhya.
    Fatura o pedido no Sankhya.
    Confirma a nota gerada no Sankhya.
    """    
    res:dict={}
    res = asyncio.run(integrar_faturamento_snk(codemp=codemp))    
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao faturar pedidos no Sankhya")
    return True

@router.get("/{codemp}/faturar/venda-interna")
def faturar_venda_interna(codemp:int):
    """
    Cria e confirma a nota de transferência no Sankhya.
    """
    res:dict={}
    res = asyncio.run(integrar_venda_interna(codemp=codemp))    
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao criar venda interna")
    return True

@router.get("/{idloja}/receber/{numero}")
def receber_pedido(idloja:int,numero:int):
    """
    Busca um pedido específico do Olist
    """
    res:dict={}
    res = asyncio.run(receber_pedido_unico(id_loja=idloja,numero=numero)) 
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro ao buscar pedido {numero}")
    return True

@router.get("/{codemp}/separacao/buscar")
def buscar_separacao(codemp:int):
    """
    Busca as separações pendentes no Olist
    """
    res:dict={}
    res = asyncio.run(integrar_separacoes(codemp=codemp)) 
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao buscar separação dos pedidos")
    return True

@router.get("/{codemp}/anular/{nunota}")
def anular_pedidos(codemp:int,nunota: int):
    """
    Exclui pedido não faturado do Sankhya
    """
    pedido = Pedido(codemp=codemp)
    if not asyncio.run(pedido.integrar_cancelamento(nunota=nunota)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao anular pedido")
    return True