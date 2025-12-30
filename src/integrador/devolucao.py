import os, re, time
from datetime import datetime
from database.crud import log as crudLog
from database.crud import log_pedido as crudLogPed
from database.crud import nota as crudNota
from database.crud import devolucao as crudDev
from src.olist.nota import Nota as NotaOlist
from src.sankhya.nota import Nota as NotaSnk
from src.parser.devolucao import Devolucao as parser
from src.utils.decorador import contexto, carrega_dados_ecommerce, log_execucao, interno, carrega_dados_empresa
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

REGEX_CHAVE_ACESSO = r'\d{44}'

class Devolucao:

    def __init__(self, id_loja:int=None, codemp:int=None):
        self.id_loja:int=id_loja
        self.codemp:int=codemp
        self.empresa_id:int=None
        self.log_id:int=None
        self.contexto:str='devolucao'
        self.dados_ecommerce:dict={}
        self.dados_empresa:dict={}
        self.req_time_sleep:float=float(os.getenv('REQ_TIME_SLEEP', 1.5))   

    @interno
    async def validar_existentes(self,lista_notas: list) -> list[dict]:
        """
        Verifica quais NFD já foram mapeadas
            :param lista_notas: lista de dicionários com os dados das NFD
            :return list[dict]: lista de dicionários com os dados das NFD que ainda não foram mapeadas
        """     
        lista_chaves = [n.get('chaveAcesso') for n in lista_notas]
        devolucoes_existentes = await crudDev.buscar(lista_chave=lista_chaves)
        lista_devolucoes_existentes = [d.get('id_nota') for d in devolucoes_existentes]
        devolucoes_pendentes = [d for d in lista_notas if d.get('id') not in lista_devolucoes_existentes]
        return devolucoes_pendentes

    async def receber(self,id:int=None,numero:int=None) -> dict:
        """
        Recebe novas NFD
            :param id: ID da NFD
            :param numero: número da NFD
            :return dict: dicionário com status, dados da NFD, dados da nota referenciada e erro
        """
        nota_olist = NotaOlist(id_loja=self.id_loja,codemp=self.codemp)

        try:
            # Busca nota de devolução
            dados_nota_devolucao = await nota_olist.buscar(id=id,numero=numero)
            if not dados_nota_devolucao:
                msg = f"Nota de devolução não encontrada"
                raise Exception(msg)
            if dados_nota_devolucao.get('tipo') != 'E':
                msg = f"Número informado não é de uma nota de devolução"
                raise Exception(msg)                
            
            # Valida nota de venda referenciada
            chave_referenciada = re.search(REGEX_CHAVE_ACESSO,
                                           dados_nota_devolucao.get('observacoes','')).group(0)
            if not chave_referenciada:
                msg = "Não foi possível extraír a chave referenciada das observações da nota fiscal"
                raise Exception(msg)
            dados_nota_referenciada = await crudNota.buscar(chave_acesso=chave_referenciada)
            if not dados_nota_referenciada:
                msg = "Nota de venda referenciada não encontrada"
                raise Exception(msg)
            
            # Registra no banco de dados
            ack = await crudDev.criar(chave_referenciada=chave_referenciada,
                                      id_nota=dados_nota_devolucao.get('id'),
                                      numero=int(dados_nota_devolucao.get('numero')),
                                      serie=dados_nota_devolucao.get('serie'),
                                      chave_acesso=dados_nota_devolucao.get('chaveAcesso'),
                                      dh_emissao=dados_nota_devolucao.get('dataInclusao'))
            if not ack:
                msg = "Erro ao registrar nota de devolução"
                raise Exception(msg)
            return {"success": True, "dados_nota":dados_nota_devolucao, "dados_nota_referenciada":dados_nota_referenciada, "__exception__": None}
        except Exception as e:
            logger.error(f"Erro ao receber nota de devolução: {e}")
            return {"success": False, "dados_nota":None, "dados_nota_referenciada": None, "__exception__": str(e)}
        
    @carrega_dados_ecommerce
    @carrega_dados_empresa
    async def lancar(self,dados_devolucao:dict) -> dict:
        """
        Registra NFDs no Sankhya
            :param dados_devolucao: dicionário com os dados da NFD
            :return dict: dicionário com status, número único da devolução de venda no Sankhya, ID do pedido no Olist e erro
        """

        nota_snk = NotaSnk(empresa_id=self.dados_ecommerce.get('empresa_id'),codemp=self.codemp)     

        try:
            if not isinstance(dados_devolucao,dict):
                dados_devolucao = dados_devolucao[0]

            # Converte para o formato da API do Sankhya
            parse = parser()
            dados_cabecalho, dados_itens = parse.to_sankhya_(dados_empresa=self.dados_empresa,dados_nfd=dados_devolucao)
            if not all([dados_cabecalho, dados_itens]):
                msg = "Erro ao converter dados da devolucao para o formato da API do Sankhya"
                raise Exception(msg)
            
            # Lança devolução
            nunota_devolucao = await nota_snk.devolver_sem_lote(dados_cabecalho=dados_cabecalho,dados_itens=dados_itens)
            if not isinstance(nunota_devolucao,int) or nunota_devolucao==0:
                msg = f"Erro ao lançar NFD {dados_devolucao.get('numero')}. Nenhum número de nota retornado. {nunota_devolucao}"
                raise Exception(msg)
            
            # Atualiza a nota no banco de dados            
            ack = await crudDev.atualizar(id_nota=dados_devolucao.get('id'),nunota=nunota_devolucao,dh_confirmacao=datetime.now())
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                print(msg)
                raise Exception(msg)

            return {"success": True, "nunota":nunota_devolucao, "__exception__": None}
        except Exception as e:
            logger.error(f"Erro ao lancar NFD {dados_devolucao.get('numero')}: {e}")
            return {"success": False, "nunota":None, "__exception__": str(e)}   
        
    @carrega_dados_ecommerce
    async def lancar_unico(self,dados_devolucao:dict,dados_nota_referenciada:dict) -> dict:
        """
        Registra NFDs no Sankhya
            :param dados_devolucao: dicionário com os dados da NFD
            :param dados_nota_referenciada: dicionário com os dados da NF referenciada
            :return dict: dicionário com status, número único da devolução de venda no Sankhya, ID do pedido no Olist e erro
        """

        nota_snk = NotaSnk(empresa_id=self.dados_ecommerce.get('empresa_id'),
                           codemp=self.codemp)     

        try:
            if not isinstance(dados_devolucao,dict):
                dados_devolucao = dados_devolucao[0]

            if not isinstance(dados_nota_referenciada,dict):
                dados_nota_referenciada = dados_nota_referenciada[0]

            if not all([isinstance(dados_devolucao,dict),isinstance(dados_nota_referenciada,dict)]):
                msg = "Dados da nota de devolução ou da nota referenciada inválidos"
                raise Exception(msg)

            dados_venda_snk = await nota_snk.buscar(nunota=dados_nota_referenciada.get('nunota'),itens=True)
            if not dados_venda_snk:
                msg = "Nota de venda não encontrada no Sankhya"
                raise Exception(msg)

            # Converte para o formato da API do Sankhya
            parse = parser()
            dados_formatados = parse.to_sankhya(itens_olist=dados_devolucao.get('itens'),
                                                itens_snk=dados_venda_snk.get('itens'))
            if not dados_formatados:
                msg = "Erro ao converter dados da devolucao para o formato da API do Sankhya"
                raise Exception(msg)
            
            # Lança devolução
            nunota_devolucao = await nota_snk.devolver(nunota=dados_venda_snk.get('nunota'),
                                                       itens=dados_formatados)
            if not isinstance(nunota_devolucao,int):
                msg = f"Erro ao lançar devolução do pedido. Nenhum número de nota retornado. {nunota_devolucao}"
                raise Exception(msg)
            
            # Atualiza a nota no banco de dados
            ack = await crudDev.atualizar(id_nota=dados_devolucao.get('id'),
                                          nunota=nunota_devolucao)
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                raise Exception(msg)            
            
            # Informa observação
            observacao = f"NFD {dados_devolucao.get('numero')} de {dados_devolucao.get('dataEmissao')}\n"+dados_devolucao.get('observacoes')
            ack = await nota_snk.alterar_observacao(nunota=nunota_devolucao,
                                                    observacao=observacao)
            if not ack:
                msg = "Erro ao informar observação da nota"
                raise Exception(msg)
            
            # Confirma a devolução
            ack = await nota_snk.confirmar(nunota=nunota_devolucao)
            if not ack:
                msg = "Erro ao confirmar devolução."
                raise Exception(msg)
        
            # Atualiza a nota no banco de dados
            ack = await crudDev.atualizar(id_nota=dados_devolucao.get('id'),
                                          dh_confirmacao=datetime.now())
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                raise Exception(msg)

            return {"success": True, "nunota":nunota_devolucao, "pedido_id":dados_nota_referenciada.get('pedido_id'), "__exception__": None}
        except Exception as e:
            return {"success": False, "nunota":None, "pedido_id": None, "__exception__": str(e)}   
        
    async def registrar_cancelamento(self,dados_devolucao:dict) -> dict:
        """
        Regitra o cancelamento de uma NFD
            :param dados_devolucao: dicionário com os dados da NFD
            :return dict: dicionário com status e erro
        """   
        try:
            # Atualiza a nota no banco de dados
            ack = await crudDev.atualizar(id_nota=dados_devolucao.get('id'),
                                          dh_cancelamento=datetime.now()) 
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                raise Exception(msg)                        
            return {"success": True, "__exception__": None}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}   
    
    @carrega_dados_ecommerce
    async def cancelar(self,id:int=None,chave:str=None,numero:int=None) -> dict:
        """
        Exclui uma nota de devolução no Sankhya.
            :param id: ID da NFD no Olist
            :param chave: chave de acesso da NFD
            :param numero: número da NFD
            :return dict: dicionário com status, dados da NFD e erro
        """           
        nota_snk = NotaSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        try:
            # Busca nota de devolução
            numero_ecommerce:dict={}
            if numero:
                numero_ecommerce = {
                    "numero":numero,
                    "ecommerce":self.dados_ecommerce.get('id')
                }
            dados_devolucao = await crudDev.buscar(id_nota=id,
                                                   chave=chave,
                                                   numero_ecommerce=numero_ecommerce)
            if not dados_devolucao:
                msg = f"Nota de devolução não encontrada"
                raise Exception(msg)            
            # Excluir devolução
            ack = await nota_snk.excluir(nunota=dados_devolucao.get('nunota'))
            if not ack:
                msg = "Erro ao excluir nota de devolução."
                raise Exception(msg)                        
            return {"success": True, "dados_devolucao":dados_devolucao, "__exception__": None}
        except Exception as e:
            return {"success": False, "dados_devolucao":dados_devolucao, "__exception__": str(e)}
        
    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def integrar_receber(self,data:str=None,**kwargs) -> bool:
        """
        Rotina para mapear novas NFD.
            :param data: data da emissão da NFD
            :return bool: status da operação
        """
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='base',
                                          contexto=kwargs.get('_contexto'))

        lista_devolucoes:list[dict]=[]
        nota_olist = NotaOlist(id_loja=self.id_loja)
        # Busca devoluções
        lista_devolucoes = await nota_olist.buscar_devolucoes(data=data)
        if not lista_devolucoes:
            # Nenhuma devolução para receber
            await crudLog.atualizar(id=self.log_id)
            return True

        # Valida devoluções já recebidas
        lista_devolucoes = await self.validar_existentes(lista_devolucoes)
        if not lista_devolucoes:
            # Todas as notas de devolução já foram recebidas
            await crudLog.atualizar(id=self.log_id)
            return True

        print(f"Devoluções para processar: {len(lista_devolucoes)}")

        for i, devolucao in enumerate(lista_devolucoes):
            time.sleep(self.req_time_sleep)
            ack = await self.receber(id=devolucao.get("id"))
            # Registra no log        
            if not ack.get('success'):
                msg = f"Erro ao receber nota de devolução {devolucao.get('numero')}: {ack.get('__exception__',None)}"
                logger.error(msg)
            await crudLogPed.criar(log_id=self.log_id,
                                    pedido_id=ack.get('pedido_id'),
                                    evento='D',
                                    sucesso=ack.get('success'),
                                    obs=ack.get('__exception__',None))

        # Atualiza log
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        return status_log
    
    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def integrar_devolucoes(self,**kwargs) -> bool:
        """
        Rotina para registrar novas NFD no Sankhya.
            :return bool: status da operação
        """

        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))

        # Busca devoluções pendentes
        devolucoes_pendentes = await crudDev.buscar_lancar(ecommerce_id=self.dados_ecommerce.get('id'))
        if not devolucoes_pendentes:
            # Nenhuma devolução para registrar
            await crudLog.atualizar(id=self.log_id)
            return True

        nota_olist = NotaOlist(id_loja=self.id_loja)
        msg = ''
        for i, devolucao in enumerate(devolucoes_pendentes):
            time.sleep(self.req_time_sleep)
            print(f"Processando NFD {devolucao.get('numero')} :: {i+1}/{len(devolucoes_pendentes)}")
            dados_nfd:dict = await nota_olist.buscar(id=devolucao.get('id_nota'))
            if not dados_nfd:
                msg = f"Erro ao buscar nota de devolução {devolucao.get('numero')}"
                logger.error(msg)
                continue
            
            # Lança devolução
            print("Lançando nota devolução")
            ack_devolucao = await self.lancar(dados_devolucao=dados_nfd)
            # Registra no log
            if not ack_devolucao.get('success'):
                msg = f"Erro ao integrar nota de devolução {devolucao.get('numero')}: {ack_devolucao.get('__exception__',None)}"
                logger.error(msg)
                continue

        # Atualiza log
        status_log = False if msg else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        return status_log
    
    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def integrar_cancelamento(self,id:int=None,chave:str=None,numero:int=None,**kwargs) -> bool:
        """
        Rotina para excluir uma NFD no Sankhya.
            :param id: ID da NFD no Olist
            :param chave: chave de acesso da NFD
            :param numero: número da NFD
            :return bool: status da operação
        """ 

        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))

        dados_devolucao:dict={}
        try:
            ack = await self.cancelar(id=id,
                                      chave=chave,
                                      numero=numero)
            if not ack.get('success'):
                raise Exception
            
            dados_devolucao = ack.get('dados_devolucao')
            if not dados_devolucao:
                raise Exception                
            
            ack = await self.registrar_cancelamento(dados_devolucao=dados_devolucao)
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

    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def devolver_unico(self,numero_nota:int,**kwargs) -> dict:
        """
        Rotina mapear e lançar uma NFD.
            :param numero_nota: número da NFD
            :return dict: dicionário com status e erro
        """
                
        retorno:dict={}
        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))

        dados_nota_devolucao:dict={}
        try:
            ack = await self.receber(numero=numero_nota)
            if not ack.get('success'):
                msg = f"Erro ao receber nota de devolução {numero_nota}: {ack.get('__exception__',None)}"                
                raise Exception(msg)
            
            dados_nota_devolucao = ack.get('dados_nota')
            dados_nota_referenciada = ack.get('dados_nota_referenciada')
            if not all([dados_nota_devolucao,dados_nota_referenciada]):
                msg = f"Erro ao receber nota de devolução {numero_nota}: dados da nota não encontrados"
                raise Exception(msg)
            
            ack = await self.lancar_unico(dados_devolucao=dados_nota_devolucao,
                                    dados_nota_referenciada=dados_nota_referenciada)
            if not ack.get('success'):
                msg = f"Erro ao lançar nota de devolução {numero_nota}: {ack.get('__exception__',None)}"
                raise Exception(msg)
            
            retorno = {
                "status": True,
                "exception": None
            }
        except Exception as e:
            logger.error(f"{e}")
            retorno = {
                "status": False,
                "exception": f"{e}"
            }
        finally:
            await crudLogPed.criar(log_id=self.log_id,
                                   pedido_id=ack.get('pedido_id'),
                                   evento='D',
                                   sucesso=ack.get('success'),
                                   obs=ack.get('__exception__',None))             

        # Atualiza log
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        return retorno