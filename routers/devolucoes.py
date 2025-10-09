from fastapi import APIRouter, HTTPException, status
from src.integrador.devolucao import Devolucao
from src.scheduler.jobs.devolucoes import integrar_devolucoes
import asyncio
from pydantic import BaseModel

router = APIRouter()

class DevolucaoModel(BaseModel):
    codemp:int
    id_loja:int
    numero:int

@router.get("/integrar")
def integrar(devolucao:DevolucaoModel):
    """
    Busca as notas de devolução no Olist e lança no Sankhya
    """
    if not asyncio.run(integrar_devolucoes(codemp=devolucao.codemp)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar devoluções")
    return True

@router.get("/devolver")
def devolver_nota(devolucao:DevolucaoModel):
    """
    Lança de devolução de uma nota específica
    """
    _devolucao = Devolucao(id_loja=devolucao.id_loja)
    if not asyncio.run(_devolucao.devolver_unico(numero_nota=devolucao.numero)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao lançar devolução no Sankhya")
    return True

@router.get("/cancelar")
def cancelar_devolucao(devolucao:DevolucaoModel):
    """
    Cancela nota de devolução
    """
    _devolucao = Devolucao(id_loja=devolucao.id_loja)
    if not asyncio.run(_devolucao.integrar_cancelamento(numero=devolucao.numero)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao cancelar nota de devolução")
    return True

