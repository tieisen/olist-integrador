from fastapi import APIRouter, HTTPException, status
from src.integrador.devolucao import Devolucao
from src.scheduler.jobs.devolucoes import integrar_devolucoes
import asyncio

router = APIRouter()

@router.get("/")
def default():
    return {"message": "Devoluções"}

@router.get("/{codemp}/integrar")
def integrar(codemp:int):
    """
    Busca as notas de devolução no Olist e lança no Sankhya
    """
    if not asyncio.run(integrar_devolucoes(codemp=codemp)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar devoluções")
    return True

@router.get("/{id_loja}/devolver/{numero}")
def devolver_nota(id_loja:int,numero:int):
    """
    Lança de devolução de uma nota específica
    """
    devolucao = Devolucao(id_loja=id_loja)
    if not asyncio.run(devolucao.devolver_unico(numero_nota=numero)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao lançar devolução no Sankhya")
    return True

@router.get("/{id_loja}/cancelar/{numero}")
def cancelar_devolucao(id_loja:int,numero:int):
    """
    Cancela nota de devolução
    """
    devolucao = Devolucao(id_loja=id_loja)
    if not asyncio.run(devolucao.integrar_cancelamento(numero=numero)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao cancelar nota de devolução")
    return True

