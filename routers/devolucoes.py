from fastapi import APIRouter, HTTPException, status
from src.integrador.devolucao import Devolucao
from src.scheduler.jobs.devolucoes import integrar_devolucoes
from pydantic import BaseModel

router = APIRouter()

class DevolucaoEmpresa(BaseModel):
    codemp:int

class DevolucaoNota(BaseModel):
    codemp:int
    numero:int

@router.get("/integrar")
async def integrar(devolucao:DevolucaoEmpresa):
    """
    Busca as notas de devolução no Olist e lança no Sankhya
    """
    res:dict={}
    res = await integrar_devolucoes(codemp=devolucao.codemp)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

@router.get("/devolver")
async def devolver_nota(devolucao:DevolucaoNota):
    """
    Lança de devolução de uma nota específica
    """
    _devolucao = Devolucao(codemp=devolucao.codemp)
    res:dict={}
    res = await _devolucao.devolver_unico(numero=devolucao.numero)    
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

@router.get("/cancelar")
async def cancelar_devolucao(devolucao:DevolucaoNota):
    """
    Cancela nota de devolução
    """
    _devolucao = Devolucao(codemp=devolucao.codemp)
    res:dict={}
    res = await _devolucao.integrar_cancelamento(numero=devolucao.numero)
    if not res.get('status'):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=res.get('exception'))
    return True

