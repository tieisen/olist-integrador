from fastapi import APIRouter, HTTPException, status
from src.scheduler.jobs.financeiro import integrar_financeiro, baixar_shopee, integrar_recebimentos_shopee
from datetime import datetime
from pydantic import BaseModel

router = APIRouter()

class FinanceiroModel(BaseModel):
    codemp:int
    data:str

class FinanceiroShopeeModel(BaseModel):
    codemp:int

class RecebimentoShopeeModel(BaseModel):
    data:str
    tipo_de_transacao:str=None
    descricao:str=None
    id_do_pedido:str
    direcao_do_dinheiro:str=None
    valor:float
    status:str=None
    balanca_apos_as_transacoes:float=None
    valor_a_ser_ajustado:float=None

class FinanceiroShopeeModel(BaseModel):
    codemp:int
    dados:list[RecebimentoShopeeModel]    

@router.post("/processar-shopee")
async def processar_shopee(financeiro:FinanceiroShopeeModel) -> bool:    
    """
    Busca os recebimentos na API da Shopee.
    Baixa títulos pendentes.
    """
    if not await integrar_recebimentos_shopee(codemp=financeiro.codemp):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro ao processar recebimentos da Shopee")
    return True

@router.post("/baixar/shopee")
async def baixar_titulos_shopee(financeiro:FinanceiroShopeeModel) -> bool:    
    """
    Baixa títulos pendentes dos pedidos da Shopee.
    """

    try:
        codemp:int = financeiro.codemp
        data:list[dict] = financeiro.model_dump()
        if not data:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Dados inválidos")
        data = data.get('dados')
        res = await baixar_shopee(codemp=codemp,data=data)
        if not res.get('status'):
            raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=res.get('exception'))
        return res.get('status')
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,detail=str(e))

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
