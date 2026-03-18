from fastapi import APIRouter, HTTPException, status
from src.scheduler.jobs.financeiro import consultarRecebimentosShopee, integrar
from pydantic import BaseModel

router = APIRouter()

class FinanceiroModel(BaseModel):
    codemp:int
    idLoja:int | None = None
    data:str | None = None

class FinanceiroShopeeModel(BaseModel):
    codemp:int

@router.post("/processar-shopee")
async def processar_shopee(financeiro:FinanceiroShopeeModel) -> bool:    
    """
    Busca os recebimentos na API da Shopee.
    """
    if not await consultarRecebimentosShopee(codemp=financeiro.codemp):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao processar recebimentos da Shopee")
    return True

@router.post("/integrar")
async def integrar_financeiro(financeiro:FinanceiroModel) -> bool:
    """
    Processa e lança títulos a receber e taxas no Olist.
    """
    if not await integrar(dtFim=financeiro.data,codemp=financeiro.codemp,idLoja=financeiro.idLoja):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao processar títulos")
    return True
