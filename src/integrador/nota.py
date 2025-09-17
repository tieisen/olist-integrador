import logging
import os
import time
from datetime import datetime
from src.sankhya.nota import Nota as NotaSnk
from src.olist.nota   import Nota as NotaOlist
from src.olist.pedido import Pedido as PedidoOlist
from database.crud                     import nota       as crudNota
from database.crud                     import log_pedido as crudLogPed
from database.crud                     import log        as crudLog
from dotenv import load_dotenv
from src.utils.log import Log
from src.utils.decorador.contexto      import contexto
from src.utils.decorador.ecommerce     import ensure_dados_ecommerce
from src.utils.decorador.log           import log_execucao
from src.utils.decorador.internal_only import internal_only

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Nota:

    def __init__(self, id_loja:int):
        self.id_loja:int=id_loja
        self.log_id:int=None
        self.contexto:str='nota'
        self.dados_ecommerce:dict=None
        self.req_time_sleep:float=float(os.getenv('REQ_TIME_SLEEP', 1.5))

    @contexto
    @internal_only
    @ensure_dados_ecommerce
    async def gerar(
            self,
            dados_pedido:dict,
            **kwargs
        ) -> dict:

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='olist',
                                              para='olist',
                                              contexto=kwargs.get('_contexto'))  

        pedido_olist = PedidoOlist(id_loja=self.id_loja)
        try:
            # Gera NF no Olist
            print("Gerando NF no Olist...")
            dados_nota_olist = await pedido_olist.gerar_nf(id=dados_pedido.get('id_pedido'))
            if not dados_nota_olist:
                msg = f"Erro ao gerar NF"
                raise Exception(msg)
            
            # Atualiza nota
            print("Atualizando status da nota...")
            ack = await crudNota.criar(id_pedido=dados_pedido.get('id_pedido'),
                                       id_nota=dados_nota_olist.get('id'),
                                       numero=dados_nota_olist.get('numero'),
                                       serie=dados_nota_olist.get('serie'))
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                raise Exception(msg)            
            return {"success": True, "dados_nota":dados_nota_olist}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    @contexto
    @internal_only
    @ensure_dados_ecommerce
    async def emitir(
            self,
            dados_nota:dict,
            **kwargs
        ) -> dict:

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='olist',
                                              para='olist',
                                              contexto=kwargs.get('_contexto'))  

        nota_olist = NotaOlist(id_loja=self.id_loja)
        try:
            # Emite a nota
            print("Emitindo nota...")
            dados_emissao = await nota_olist.emitir(id=dados_nota.get('id'))
            if not dados_emissao:
                msg = f"Erro ao emitir nota"
                raise Exception(msg)
            
            # Atualiza a nota no banco de dados
            print("Atualizando status da nota...")
            ack = await crudNota.atualizar(id_nota=dados_nota.get('id'),
                                           chave_acesso=dados_emissao.get('chaveAcesso'),
                                           dh_emissao=datetime.now())
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                raise Exception(msg)
                        
            print(f"Nota emitida com sucesso!")
            return {"success": True, "chave_acesso":dados_emissao.get('chaveAcesso')}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}        

    @internal_only
    @ensure_dados_ecommerce
    async def receber_conta(
            self,
            dados_nota:dict,
            **kwargs
        ) -> dict:

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='olist',
                                              para='olist',
                                              contexto=kwargs.get('_contexto'))  

        try:            
            # Busca contas a receber no Olist
            print("Buscando contas a receber no Olist...")
            nota_olist = NotaOlist(id_loja=self.id_loja)
            dados_financeiro = await nota_olist.buscar_financeiro(serie=dados_nota.get('serie'),
                                                                  numero=dados_nota.get('numero'))
            if not dados_financeiro:
                msg = f"Erro ao buscar contas a receber da nota"
                raise Exception(msg)
            
            if dados_financeiro.get('dataLiquidacao'):
                print(f"Contas a receber da nota já está liquidado")
            
            # Atualiza a nota no banco de dados
            print("Atualizando status da nota...")
            ack = await crudNota.atualizar(id_nota=dados_nota.get('id'),
                                           id_financeiro=dados_financeiro.get('id'),
                                           dh_baixa_financeiro=dados_financeiro.get('dataLiquidacao',None))
            if not ack:
                msg = f"Erro ao atualizar contas a receber da nota"
                raise Exception(msg)            
            print(f"Contas a receber da nota vinculado com sucesso!")
            return {"success": True, "dados_financeiro":dados_financeiro}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    @internal_only
    @ensure_dados_ecommerce
    async def baixar_conta(
            self,
            id_nota:int,
            dados_financeiro:dict=None,
            **kwargs
        ) -> dict:

        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='olist',
                                              para='olist',
                                              contexto=kwargs.get('_contexto'))
        try:
            if not dados_financeiro:
                # Busca dados do contas a receber no Olist
                print("Buscando dados do contas a receber no Olist...")
                dados_nota = await crudNota.buscar(id_nota=id_nota)
                if not dados_nota:
                    msg = f"Erro ao buscar dados da nota"
                    raise Exception(msg)
                nota_olist = NotaOlist(id_loja=self.id_loja)
                dados_financeiro = await nota_olist.buscar_financeiro(numero=dados_nota.get('numero'),
                                                                            serie=dados_nota.get('serie'))
                if not dados_financeiro:
                    msg = f"Erro ao buscar contas a receber da nota"
                    raise Exception(msg)
            
            # Lança recebimento do contas a receber
            print("Lançando baixa do contas a receber...")
            ack = await nota_olist.baixar_financeiro(id=dados_financeiro.get('id'),
                                                     valor=dados_financeiro.get('valor'))
            if not ack:
                msg = f"Erro ao baixar contas a receber da nota"
                raise Exception(msg)
            
            # Atualiza a nota no banco de dados
            ack = await crudNota.atualizar(id_nota=id_nota,
                                           dh_baixa_financeiro=datetime.now())
            if not ack:
                msg = f"Erro ao atualizar contas a receber da nota"
                raise Exception(msg)            
            print(f"Contas a receber da nota vinculado com sucesso!")
            return {"success": True}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}
