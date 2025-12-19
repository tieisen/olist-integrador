import os
from datetime import datetime
from src.olist.nota import Nota as NotaOlist
from src.olist.financeiro import Financeiro as FinOlist
from src.olist.pedido import Pedido as PedidoOlist
from src.sankhya.nota import Nota as NotaSnk
from database.crud import nota as crudNota
from database.crud import log as crudLog
from database.crud import log_pedido as crudLogPed
from src.utils.decorador import contexto, carrega_dados_ecommerce, log_execucao
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
    async def gerar(self,dados_pedido:dict,**kwargs) -> dict:
        """
        Gera NF no Olist
            :param dados_pedido: dicionário com os dados do pedido no Olist
            :return dict: dicionário com status, dados da NF gerada e erro
        """
        
        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='olist',
                                              para='olist',
                                              contexto=kwargs.get('_contexto'))  

        pedido_olist = PedidoOlist(empresa_id=self.dados_ecommerce.get('empresa_id'))
        try:
            # Gera NF no Olist
            dados_nota_olist = await pedido_olist.gerar_nf(id=dados_pedido.get('id_pedido'))
            if not dados_nota_olist:
                msg = f"Erro ao gerar NF"
                raise Exception(msg)
            
            if dados_nota_olist.get('mensagem'):
                print(dados_nota_olist.get('mensagem'))
                nota_olist = NotaOlist(id_loja=self.id_loja,empresa_id=self.dados_ecommerce.get('empresa_id'))
                dados_nota_olist = await nota_olist.buscar(cod_pedido=dados_pedido.get('cod_pedido'))
                print(f"#{int(dados_nota_olist.get('numero'))}")                

            # eh_parcelado:bool = True if len(dados_nota_olist.get('parcelas',[])) > 1 else False
            eh_parcelado:bool = True if len(dados_pedido['dados_pedido']['pagamento'].get('parcelas',[])) > 1 else False
            status_estoque:bool = True if self.dados_ecommerce.get('empresa_id') == 1 else False

            # Atualiza nota
            ack = await crudNota.criar(id_pedido=dados_pedido.get('id_pedido'),
                                       id_nota=dados_nota_olist.get('id'),
                                       numero=int(dados_nota_olist.get('numero')),
                                       serie=str(dados_nota_olist.get('serie')),
                                       parcelado=eh_parcelado,
                                       baixa_estoque_ecommerce=status_estoque)
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                raise Exception(msg)            
            return {"success": True, "dados_nota":dados_nota_olist, "__exception__": None}
        except Exception as e:
            logger.error(f"Erro ao gerar NF: {e}")
            return {"success": False, "dados_nota": None, "__exception__": str(e)}

    @contexto
    @carrega_dados_ecommerce
    async def emitir(self,dados_nota:dict,**kwargs) -> dict:
        """
        Autoriza NF do Olist na Sefaz
            :param dados_nota: dicionário com os dados da NF
            :return dict: dicionário com status, chave de acesso da NF autorizada e erro
        """
        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='olist',
                                              para='olist',
                                              contexto=kwargs.get('_contexto'))  

        nota_olist = NotaOlist(id_loja=self.id_loja,empresa_id=self.dados_ecommerce.get('empresa_id'))
        try:
            # Emite a nota
            dados_emissao = await nota_olist.emitir(id=dados_nota.get('id'))
            if not dados_emissao:
                msg = f"Erro ao emitir nota"
                raise Exception(msg)
            
            # Atualiza a nota no banco de dados
            ack = await crudNota.atualizar(id_nota=dados_nota.get('id'),
                                           chave_acesso=dados_emissao.get('chaveAcesso'),
                                           dh_emissao=datetime.now())
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                raise Exception(msg)
                        
            return {"success": True, "chave_acesso":dados_emissao.get('chaveAcesso'), "__exception__": None}
        except Exception as e:
            return {"success": False, "chave_acesso": None, "__exception__": str(e)}        

    @carrega_dados_ecommerce
    async def receber_conta(self,dados_nota:dict,**kwargs) -> dict:
        """
        Busca o lançamento de contas a receber referente à NF no Olist
            :param dados_nota: dicionário com os dados da NF
            :return dict: dicionário com status, dados do lançamento e erro
        """
        if not self.log_id:
            self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                              de='olist',
                                              para='olist',
                                              contexto=kwargs.get('_contexto'))  

        try:            
            # Busca contas a receber no Olist
            # print("Buscando contas a receber no Olist...")
            nota_olist = NotaOlist(id_loja=self.id_loja,empresa_id=self.dados_ecommerce.get('empresa_id'))
            dados_financeiro = await nota_olist.buscar_financeiro(serie=str(dados_nota.get('serie')),                                                                  
                                                                  numero=str(dados_nota.get('numero')).zfill(6))
            if not dados_financeiro:
                msg = f"Erro ao buscar contas a receber da nota"
                raise Exception(msg)
            
            # Atualiza a nota no banco de dados
            ack = await crudNota.atualizar(id_nota=dados_nota.get('id'),
                                           id_financeiro=dados_financeiro.get('id'))
            if not ack:
                msg = f"Erro ao atualizar contas a receber da nota"
                raise Exception(msg)            
            return {"success": True, "dados_financeiro":dados_financeiro, "__exception__": None}
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
            nota_olist = NotaOlist(id_loja=self.id_loja,empresa_id=self.dados_ecommerce.get('empresa_id'))
            if not dados_financeiro:
                # Busca dados do contas a receber no Olist
                # print("Buscando dados do contas a receber no Olist...")
                dados_nota = await crudNota.buscar(id_nota=id_nota)
                if not dados_nota:
                    msg = f"Erro ao buscar dados da nota"
                    raise Exception(msg)                
                dados_financeiro = await nota_olist.buscar_financeiro(numero=str(dados_nota.get('numero')).zfill(6),
                                                                      serie=str(dados_nota.get('serie')))
                if not dados_financeiro:
                    msg = f"Erro ao buscar contas a receber da nota"
                    raise Exception(msg)
            
            # Lança recebimento do contas a receber
            # print("Lançando baixa do contas a receber...")
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
        """
        Regitra o cancelamento de uma NF            
            :param dados_nota: dicionário com os dados da NF
            :return dict: dicionário com status e erro
        """
        try:
            # Atualiza a nota no banco de dados
            ack = await crudNota.atualizar(id_nota=dados_nota.get('id'),dh_cancelamento=datetime.now()) 
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                raise Exception(msg)                        
            return {"success": True, "__exception__": None}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}
    
    @carrega_dados_ecommerce
    async def cancelar(self,id:int=None,chave:str=None,numero:int=None) -> dict:
        """
        Exclui uma nota de venda no Sankhya. Somente para importação por pedido.
            :param id: ID da NF no Olist
            :param chave: chave de acesso da NF
            :param numero: número da NF
            :return dict: dicionário com status, dados da nota e erro
        """        
        nota_snk = NotaSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        try:
            # Busca nota
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
            ack = await nota_snk.excluir(nunota=dados_nota.get('nunota'))
            if not ack:
                msg = "Erro ao excluir nota."
                raise Exception(msg)                        
            return {"success": True, "dados_nota":dados_nota, "__exception__": None}
        except Exception as e:
            return {"success": False, "dados_nota":None, "__exception__": str(e)}

    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def integrar_cancelamento(self,id:int=None,chave:str=None,numero:int=None,**kwargs) -> bool:
        """
        Rotina para excluir uma nota de venda no Sankhya. Somente para importação por pedido.
            :param id: ID da NF no Olist
            :param chave: chave de acesso da NF
            :param numero: número da NF
            :return bool: status da operação
        """ 
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
        