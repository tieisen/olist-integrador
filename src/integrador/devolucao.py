import os
import re
import time
import logging
from datetime import datetime
from dotenv import load_dotenv
from database.crud                 import log         as crudLog
from database.crud                 import log_pedido  as crudLogPed
from database.crud                 import nota        as crudNota
from database.crud                 import devolucao   as crudDev
from src.olist.nota                import Nota        as NotaOlist
from src.sankhya.nota              import Nota        as NotaSnk
from src.parser.devolucao          import Devolucao as parser
#from src.utils.decorador.contexto  import contexto
#from src.utils.decorador.ecommerce import carrega_dados_ecommerce
#from src.utils.decorador.log       import log_execucao
from src.utils.decorador import contexto, carrega_dados_ecommerce, log_execucao
from src.utils.log                 import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

REGEX_CHAVE_ACESSO = r'\d{44}'

class Devolucao:

    def __init__(self, id_loja:int):
        self.id_loja:int=id_loja
        self.log_id:int=None
        self.contexto:str='devolucao'
        self.dados_ecommerce:dict={}
        self.req_time_sleep:float=float(os.getenv('REQ_TIME_SLEEP', 1.5))   

    @carrega_dados_ecommerce
    async def receber(self,numero:int) -> dict:

        nota_olist = NotaOlist(id_loja=self.id_loja)

        try:
            # Busca nota de devolução
            print("Buscando nota de devolução...")
            dados_nota_devolucao = await nota_olist.buscar(numero=numero)
            if not dados_nota_devolucao:
                msg = f"Nota de devolução não encontrada"
                raise Exception(msg)
            
            # Valida nota de venda referenciada
            print("Validando nota de venda referenciada...")
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
            print("Registrando nota de devolução...")
            ack = crudDev.criar(chave_referenciada=chave_referenciada,
                                id_nota=dados_nota_devolucao.get('id'),
                                numero=int(dados_nota_devolucao.get('numero')),
                                serie=dados_nota_devolucao.get('serie'),
                                dh_emissao=dados_nota_devolucao.get('dataInclusao'))
            if not ack:
                msg = "Erro ao registrar nota de devolução"
                raise Exception(msg)
            print("Nota de devolução recebida com sucesso!")
            return {"success": True}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}   
        
    @carrega_dados_ecommerce
    async def lancar(self,dados_devolucao:dict) -> dict:

        nota_olist = NotaOlist(id_loja=self.id_loja)
        nota_snk = NotaSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))

        try:
            # Busca nota de devolução
            print("Buscando nota de devolução...")
            dados_nota_devolucao = await nota_olist.buscar(id=dados_devolucao.get('id_nota'))
            if not dados_nota_devolucao:
                msg = f"Nota de devolução não encontrada"
                raise Exception(msg)            

            # Busca nota de venda no Sankhya
            print("Buscando nota de venda no Sankhya...")
            dados_nota_referenciada = await crudNota.buscar(chave_acesso=dados_devolucao.get('chave_referenciada'))
            if not dados_nota_referenciada:
                msg = "Nota de venda referenciada não encontrada"
                raise Exception(msg)            
            dados_venda_snk = await nota_snk.buscar(nunota=dados_nota_referenciada.get('nunota'),itens=True)
            if not dados_venda_snk:
                msg = "Nota de venda não encontrada no Sankhya"
                raise Exception(msg)

            # Converte para o formato da API do Sankhya
            print("Convertendo dados para o formato da API do Sankhya...")
            dados_formatados = parser.to_sankhya(itens_olist=dados_nota_devolucao.get('itens'),
                                                itens_snk=dados_venda_snk.get('itens'))
            if not dados_formatados:
                msg = "Erro ao converter dados da devolucao para o formato da API do Sankhya"
                raise Exception(msg)
            
            # Lança devolução
            print(f"Lançando devolução...")
            nunota_devolucao = await nota_snk.devolver(nunota=dados_venda_snk.get('nunota'),
                                                       itens=dados_formatados)
            if not nunota_devolucao:
                msg = "Erro ao lançar devolução do pedido."
                raise Exception(msg)
            
            # Atualiza a nota no banco de dados
            print("Atualizando status da nota...")
            ack = await crudDev.atualizar(id_nota=dados_devolucao.get('id'),
                                          nunota=nunota_devolucao)
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                raise Exception(msg)            
            
            # Informa observação
            print("Informando observação...")
            ack = await nota_snk.alterar_observacao(nunota=nunota_devolucao,
                                                    observacao=dados_nota_devolucao.get('observacao'))
            if not ack:
                msg = "Erro ao informar observação da nota"
                raise Exception(msg)
            
            # Confirma a devolução
            print("Confirmando devolução...")
            ack = await nota_snk.confirmar(nunota=nunota_devolucao)
            if not ack:
                msg = "Erro ao confirmar devolução."
                raise Exception(msg)
        
            # Atualiza a nota no banco de dados
            print("Atualizando status da nota...")
            ack = await crudDev.atualizar(id_nota=dados_devolucao.get('id'),
                                          dh_confirmacao=datetime.now())
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                raise Exception(msg)
                        
            print(f"Nota de devolução lançada com sucesso!")
            return {"success": True, "nunota":nunota_devolucao, "pedido_id":dados_nota_referenciada.get('pedido_id')}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}   
        
    async def registrar_cancelamento(self,dados_devolucao:dict) -> dict:
        try:
            # Atualiza a nota no banco de dados
            print("Atualizando status da nota...")
            ack = await crudDev.atualizar(id_nota=dados_devolucao.get('id'),
                                          dh_cancelamento=datetime.now()) 
            if not ack:
                msg = f"Erro ao atualizar status da nota"
                raise Exception(msg)                        
            print(f"Nota de devolução cancelada com sucesso!")
            return {"success": True}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}   
        
    async def cancelar(self,dados_devolucao:dict) -> dict:
        nota_snk = NotaSnk(empresa_id=self.dados_ecommerce.get('empresa_id'))
        try:
            # Busca nota de devolução
            print("Buscando nota de devolução...")
            dados_devolucao = await crudDev.buscar(id_nota=dados_devolucao.get('id_nota'))
            if not dados_devolucao:
                msg = f"Nota de devolução não encontrada"
                raise Exception(msg)            
            # Excluir devolução
            print(f"Excluindo devolução...")
            ack = await nota_snk.excluir(nunota=dados_devolucao.get('nunota'))
            if not ack:
                msg = "Erro ao excluir nota de devolução."
                raise Exception(msg)                        
            print(f"Nota de devolução excluída com sucesso!")
            return {"success": True}
        except Exception as e:
            return {"success": False, "__exception__": str(e)}   
    
    @contexto
    @log_execucao
    @carrega_dados_ecommerce
    async def integrar_devolucoes(self,**kwargs):

        self.log_id = await crudLog.criar(empresa_id=self.dados_ecommerce.get('empresa_id'),
                                          de='olist',
                                          para='sankhya',
                                          contexto=kwargs.get('_contexto'))

        # Busca devoluções pendentes
        print("-> Buscando devoluções pendentes...")
        devolucoes_pendentes = await crudDev.buscar_lancar(ecommerce_id=self.dados_ecommerce.get('id'))
        if not devolucoes_pendentes:
            print("Nenhuma devolução para integrar.")
            await crudLog.atualizar(id=self.log_id)
            return True

        print(f"{len(devolucoes_pendentes)} devoluções para integrar")

        for i, devolucao in enumerate(devolucoes_pendentes):
            time.sleep(self.req_time_sleep)
            print(f"-> Nota {i + 1}/{len(devolucoes_pendentes)}: {devolucao.get("numero")}")
            ack_devolucao = await self.lancar(dados_devolucao=devolucao)
            # Registra no log
            print("-> Registrando log...")
            for ack in ack_devolucao:
                if not ack.get('success'):
                    msg = f"Erro ao integrar nota de devolução {devolucao.get('numero')}: {ack.get('__exception__',None)}"
                    logger.error(msg)
                    print(msg)
                await crudLogPed.criar(log_id=self.log_id,
                                       pedido_id=ack.get('pedido_id'),
                                       evento='D',
                                       sucesso=ack.get('success'),
                                       obs=ack.get('__exception__',None))              

        # Atualiza log
        status_log = False if await crudLogPed.buscar_falhas(self.log_id) else True
        await crudLog.atualizar(id=self.log_id,sucesso=status_log)
        return True