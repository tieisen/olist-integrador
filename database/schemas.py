from pydantic import BaseModel
from datetime import datetime
from typing import Optional

class EmpresaBase(BaseModel):
    ativo: Optional[bool] = True
    snk_codemp: int
    nome: str
    cnpj: str
    serie_nfe: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    olist_admin_email: Optional[str] = None
    olist_admin_senha: Optional[str] = None
    olist_id_fornecedor_padrao: Optional[int] = None
    olist_id_deposito_padrao: Optional[int] = None
    olist_dias_busca_pedidos: Optional[int] = None
    olist_situacao_busca_pedidos: Optional[int] = None
    olist_id_conta_destino: Optional[int] = None
    olist_id_categoria_padrao: Optional[int] = None
    olist_id_marca_padrao: Optional[int] = None
    snk_token: Optional[str] = None
    snk_appkey: Optional[str] = None
    snk_admin_email: Optional[str] = None
    snk_admin_senha: Optional[str] = None
    snk_timeout_token_min: Optional[int] = None
    snk_top_pedido: Optional[int] = None
    snk_top_venda: Optional[int] = None
    snk_top_transferencia: Optional[int] = None
    snk_top_devolucao: Optional[int] = None
    snk_codvend: Optional[int] = None
    snk_codcencus: Optional[int] = None
    snk_codnat: Optional[int] = None
    snk_codnat_transferencia: Optional[int] = None
    snk_codtipvenda: Optional[int] = None
    snk_codusu_integracao: Optional[int] = None
    snk_codtab_transf: Optional[int] = None
    snk_codlocal_estoque: Optional[str] = None
    snk_codlocal_venda: Optional[int] = None
    snk_codparc: Optional[int] = None
    snk_codemp_fornecedor: int
    snk_obs_transferencia: Optional[str] = None

class EmpresaCreate(EmpresaBase):
    pass

class EmpresaDB(EmpresaBase):
    id: int
    dh_criacao: datetime
    dh_atualizacao: Optional[datetime] = None

    class Config:
        orm_mode = True
