from fastapi import APIRouter, HTTPException, status
from src.integrador.pedido import Pedido
from src.scheduler.jobs.pedidos import integrar_pedidos, receber_pedido_lote, receber_pedido_unico, integrar_separacoes
from src.scheduler.jobs.faturamento import integrar_faturamento, integrar_faturamento_olist, integrar_faturamento_snk, integrar_venda_interna
from pydantic import BaseModel

router = APIRouter()

class PedidoEmpresa(BaseModel):
    codemp:int

class PedidoLoja(BaseModel):
    id_loja:int

class PedidoReceber(BaseModel):
    id_loja:int
    numero:int

class PedidoAnular(BaseModel):
    codemp:int
    nunota:int

@router.post("/integrar-lote")
async def integrar(body:PedidoEmpresa) -> bool:
    """
    Busca os pedidos novos do Olist que estão com status Preparando Envio.
    Importa os pedidos pendentes do Olist.
    Confirma o pedido no Sankhya.
    """
    res:dict={}
    res = await receber_pedido_lote(codemp=body.codemp)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    
    res = await integrar_pedidos(codemp=body.codemp)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

@router.post("/integrar-loja")
async def integrar_loja(body:PedidoLoja) -> bool:
    """
    Busca os pedidos novos do Olist, por E-commerce, que estão com status Preparando Envio.
    Importa os pedidos pendentes do Olist.
    Confirma o pedido no Sankhya.
    """
    res:dict={}
    res = await receber_pedido_lote(id_loja=body.id_loja)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    res = await integrar_pedidos(id_loja=body.id_loja)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

@router.post("/faturar-lote")
async def faturar(body:PedidoEmpresa) -> bool:
    """
    Emite e autoriza as NFs dos pedidos no Olist.
    Envia os pedidos para separação no Olist.
    Cria e confirma a nota de transferência no Sankhya.
    Fatura o pedido no Sankhya.
    Confirma a nota gerada no Sankhya.
    """
    res:dict={}
    res = await integrar_faturamento(codemp=body.codemp)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

@router.post("/faturar-loja")
async def faturar_loja(body:PedidoLoja) -> bool:
    """
    Emite e autoriza as NFs dos pedidos no Olist, por E-commerce.
    Envia os pedidos para separação no Olist.
    Cria e confirma a nota de transferência no Sankhya.
    Fatura o pedido no Sankhya.
    Confirma a nota gerada no Sankhya.
    """
    res:dict={}
    res = await integrar_faturamento(id_loja=body.id_loja)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

@router.post("/faturar/olist")
async def faturar_olist(body:PedidoEmpresa) -> bool:
    """
    Emite e autoriza as NFs dos pedidos no Olist.
    Envia os pedidos para separação no Olist.
    """
    res:dict={}
    res = await integrar_faturamento_olist(codemp=body.codemp)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

@router.post("/faturar-loja/olist")
async def faturar_olist_loja(body:PedidoLoja) -> bool:
    """
    Emite e autoriza as NFs dos pedidos no Olist, por E-commerce.
    Envia os pedidos para separação no Olist.
    """
    res:dict={}
    res = await integrar_faturamento_olist(id_loja=body.id_loja)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

@router.post("/faturar/snk")
async def faturar_sankhya(body:PedidoEmpresa) -> bool:
    """
    Cria e confirma a nota de transferência no Sankhya.
    Fatura o pedido no Sankhya.
    Confirma a nota gerada no Sankhya.
    """    
    res:dict={}
    res = await integrar_faturamento_snk(codemp=body.codemp)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

@router.post("/faturar/venda-interna")
async def faturar_venda_interna(body:PedidoEmpresa) -> bool:
    """
    Cria e confirma a nota de transferência no Sankhya.
    """
    res:dict={}
    res = await integrar_venda_interna(codemp=body.codemp)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

@router.post("/receber")
async def receber_pedido(body:PedidoReceber) -> bool:
    """
    Busca um pedido específico do Olist
    """
    res:dict={}
    res = await receber_pedido_unico(id_loja=body.id_loja,numero=body.numero)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

@router.post("/separacao/buscar")
async def buscar_separacao(body:PedidoEmpresa) -> bool:
    """
    Busca as separações pendentes no Olist
    """
    res:dict={}
    res = await integrar_separacoes(codemp=body.codemp)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

@router.post("/anular")
async def anular_pedidos(body:PedidoAnular) -> bool:
    """
    Exclui pedido não faturado do Sankhya
    """
    res:dict={}    
    _pedido = Pedido(codemp=body.codemp)
    res = await _pedido.anular_pedido_importado(nunota=body.nunota)
    if not res.get('sucesso'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('__exception__'))
    return True
