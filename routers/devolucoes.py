from fastapi import APIRouter, HTTPException, status
from src.integrador.devolucao import Devolucao
import asyncio

router = APIRouter()
devolucao = Devolucao()

@router.get("/")
def default():
    return {"message": "Devoluções"}

@router.get("/receber")
def receber_devolucoes():
    """
    Busca as notas de devolução no Olist
    """
    if not asyncio.run(devolucao.integrar_receber()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao receber notas de devolução")
    return True

@router.get("/integrar")
def integrar_devolucoes():
    """
    Busca as notas de devolução no Olist e lança no Sankhya
    """
    if not asyncio.run(devolucao.integrar_receber()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao receber notas de devolução")
    if not asyncio.run(devolucao.integrar_devolucoes()):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao lançar devoluções no Sankhya")
    return True

@router.get("/devolver/{numero}")
def devolver_nota(numero:int):
    """
    Lança de devolução de uma nota específica
    """
    if not asyncio.run(devolucao.devolver_unico(numero_nota=numero)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao lançar devolução no Sankhya")
    return True

@router.get("/cancelar/{numero}")
def cancelar_devolucao(numero:int):
    """
    Cancela nota de devolução
    """
    if not asyncio.run(devolucao.integrar_cancelamento(numero=numero)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao cancelar nota de devolução")
    return True

