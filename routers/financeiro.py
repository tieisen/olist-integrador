from fastapi import APIRouter, HTTPException, status
from src.scheduler.jobs.financeiro import integrar_financeiro
from datetime import datetime
from pydantic import BaseModel

router = APIRouter()

class FinanceiroModel(BaseModel):
    codemp:int
    data:str

@router.post("/baixar")
async def baixar_contas(financeiro:FinanceiroModel) -> bool:
    """
    Baixa contas a receber com base no relatório de custos do e-commerce.
    """
    try:
        financeiro.data = datetime.strptime(financeiro.data, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Formato de data inválido. Use 'YYYY-MM-DD'. Data informada: {financeiro.data}")
    if not await integrar_financeiro(data=financeiro.data,codemp=financeiro.codemp):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao baixar contas a receber")
    return True
