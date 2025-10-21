from fastapi import APIRouter, HTTPException, status
from src.sankhya.empresa import Empresa
from database.schemas import EmpresaCreate
from database.crud import empresa as crud
import asyncio

router = APIRouter()
empresa = Empresa()

@router.get("/buscar")
def buscar_todas():
    """
    Busca todas as empresas
    """
    dados = asyncio.run(empresa.buscar())
    if not dados:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao buscar lista de empresas")
    return dados

@router.get("/buscar/{codemp}")
def buscar_codemp(codemp:int):
    """
    Busca empresa
    """
    dados = asyncio.run(empresa.buscar(codemp=codemp))
    if not dados:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Erro ao buscar dados da empresa {codemp}")
    return dados

@router.post("")
async def criar(empresa: EmpresaCreate):
    """
    Cadastra empresa
    """
    try:
        sucesso = await crud.criar(**empresa.model_dump())
        if not sucesso:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Erro ao criar empresa"
            )
        return sucesso
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(e)
        )