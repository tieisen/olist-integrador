from fastapi import APIRouter, HTTPException, status
from src.scheduler.jobs.estoque import integrar_estoque
from pydantic import BaseModel

router = APIRouter()

class EstoqueModel(BaseModel):
    codemp:int

@router.post("/integrar")
async def integrar(estoque:EstoqueModel):
    """
    Atualiza o saldo de estoque no Olist dos produtos que tiveram alteração no saldo de estoque do Sankhya.
    """
    res:bool=None
    res = await integrar_estoque(codemp=estoque.codemp)
    if not res:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao integrar estoque")
    return res