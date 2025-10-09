from fastapi import APIRouter, HTTPException, status
from src.scheduler.jobs.produtos import integrar_produtos
import asyncio
from pydantic import BaseModel

router = APIRouter()

class ProdutoModel(BaseModel):
    codemp:int

@router.post("/integrar")
def integrar_produtos(produto:ProdutoModel):
    """
    Cria o cadastro de produtos novos no Olist.
    Atualiza no Olist os dados dos produtos que tiveram alteração no cadastro do Sankhya.
    Atualiza no Sankhya os dados dos produtos que tiveram alteração no cadastro do Olist.
    """
    if not asyncio.run(integrar_produtos(codemp=produto.codemp)):
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Erro integrar produtos")
    return True