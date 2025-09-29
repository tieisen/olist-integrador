from fastapi import APIRouter, HTTPException, status
from src.integrador.devolucao import Devolucao
from routines.devolucoes import integrar_devolucoes
import asyncio

router = APIRouter()
devolucao = Devolucao()

@router.get("/")
def default():
    return {"message": "Devoluções"}

@router.get("{codemp}/integrar")
def integrar_devolucoes(codemp:int):
    """
    Busca as notas de devolução no Olist e lança no Sankhya
    """
    res:dict={}
    res = asyncio.run(integrar_devolucoes(codemp=codemp))
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar devoluções")
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

