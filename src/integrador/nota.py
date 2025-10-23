import os
from datetime import datetime
from src.olist.nota import Nota as NotaOlist
from src.sankhya.nota import Nota as NotaSnk
from src.olist.pedido import Pedido as PedidoOlist
from database.crud import nota as crudNota
from database.crud import log as crudLog
from database.crud import log_pedido as crudLogPed
from src.utils.decorador import contexto, carrega_dados_ecommerce, interno, log_execucao
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Nota:

    def __init__(self, id_loja:int):
        self.id_loja:int=id_loja
        self.log_id:int=None
        self.contexto:str='nota'
        self.dados_ecommerce:dict=None
        self.req_time_sleep:float=float(os.getenv('REQ_TIME_SLEEP', 1.5))

    @contexto
    @carrega_dados_ecommerce
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

        pedido_olist = PedidoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
        try:
            # Gera NF no Olist
            print("Gerando NF no Olist...")
            dados_nota_olist = await pedido_olist.gerar_nf(id=dados_pedido.get('id_pedido'))
            if not dados_nota_olist:
                msg = f"Erro ao gerar NF"
                raise Exception(msg)
            
            if dados_nota_olist.get('mensagem'):
                print(dados_nota_olist.get('mensagem'))
                nota_olist = NotaOlist(id_loja=self.id_loja)
                dados_nota_olist = await nota_olist.buscar(cod_pedido=dados_pedido.get('cod_pedido'))
                print(f"#{int(dados_nota_olist.get('numero'))}")                
            
            # Atualiza nota
            print("Atualizando status da nota...")
            ack = await crudNota.criar(id_pedido=dados_pedido.get('id_pedido'),
                                       id_nota=dados_nota_olist.get('id'),
                                       numero=int(dados_nota_olist.get('numero')),
                                       serie=str(dados_nota_olist.get('serie')))
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                print(msg)
                raise Exception(msg)            
            return {"success": True, "dados_nota":dados_nota_olist}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    @contexto
    @carrega_dados_ecommerce
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

    @carrega_dados_ecommerce
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
            dados_financeiro = await nota_olist.buscar_financeiro(serie=str(dados_nota.get('serie')),                                                                  
                                                                  numero=str(dados_nota.get('numero')).zfill(6))
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

    @carrega_dados_ecommerce
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
                dados_financeiro = await nota_olist.buscar_financeiro(numero=str(dados_nota.get('numero')).zfill(6),
                                                                      serie=str(dados_nota.get('serie')))
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
    
    async def registrar_cancelamento(self,dados_nota:dict) -> dict:
        try:
            # Atualiza a nota no banco de dados
            print("Atualizando status da nota...")
            ack = await crudNota.atualizar(id_nota=dados_nota.get('id'),
                                           dh_cancelamento=datetime.now()) 
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                raise Exception(msg)                        
            print(f"Nota cancelada com sucesso!")
            return {"success": True}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}
    
    @carrega_dados_ecommerce
    async def cancelar(self,id:int=None,chave:str=None,numero:int=None) -> dict:
        nota_snk = NotaSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        try:
            # Busca nota
            print("Buscando nota...")
            numero_ecommerce:dict={}
            if numero:
                numero_ecommerce = {
                    "numero":numero,
                    "ecommerce":self.dados_ecommerce.get('id')
                }            
            dados_nota = await crudNota.buscar(id_nota=id,
                                               chave_acesso=chave,
                                               numero_ecommerce=numero_ecommerce)
            if not dados_nota:
                msg = f"Nota não encontrada"
                raise Exception(msg)            
            # Excluir nota
            print(f"Excluindo nota...")
            ack = await nota_snk.excluir(nunota=dados_nota.get('nunota'))
            if not ack:
                msg = "Erro ao excluir nota."
                raise Exception(msg)                        
            print(f"Nota excluída com sucesso!")
            return {"success": True, "dados_nota":dados_nota}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}

    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def integrar_cancelamento(
            self,
            id:int=None,
            chave:str=None,
            numero:int=None,
            **kwargs
        ) -> bool:

        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))

        dados_nota:dict={}
        try:
            ack = await self.cancelar(id=id,
                                      chave=chave,
                                      numero=numero)
            if not ack.get('success'):
                raise Exception
            
            dados_nota = ack.get('dados_nota')
            if not dados_nota:
                raise Exception                
            
            ack = await self.registrar_cancelamento(dados_nota=dados_nota)
            if not ack.get('success'):
                raise Exception          
        except:
            pass
        finally:
            await crudLogPed.criar(log_id=self.log_id,
                                   pedido_id=ack.get('pedido_id'),
                                   evento='N',
                                   sucesso=ack.get('success'),
                                   obs=ack.get('__exception__',None))             

        # Atualiza log
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        return status_log
        