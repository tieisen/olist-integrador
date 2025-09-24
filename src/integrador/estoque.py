import os
import logging
from dotenv import load_dotenv
from database.crud                 import log         as crudLog
from database.crud                 import log_estoque as crudLogEst
from src.sankhya.estoque           import Estoque     as EstoqueSnk
from src.olist.estoque             import Estoque     as EstoqueOlist
# from src.utils.decorador.contexto  import contexto
# from src.utils.decorador.empresa   import carrega_dados_empresa
# from src.utils.decorador.ecommerce import carrega_dados_ecommerce
# from src.utils.decorador.log       import log_execucao

from src.utils.decorador import contexto, carrega_dados_empresa, carrega_dados_ecommerce, log_execucao
from src.utils.log                 import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Estoque:

    def __init__(self, codemp:int=None, id_loja:int=None):
        self.codemp = codemp
        self.id_loja = id_loja
        self.contexto = 'estoque'
        self.dados_empresa = None
        self.dados_ecommerce = None

    @contexto
    @carrega_dados_ecommerce
    def calcular_variacao(
            self,
            estoque_snk:dict,
            estoque_olist:dict=None
        ) -> dict:

        resultado = {}

        if not estoque_olist:
            # Produto recém cadastrado
            variacao = int(estoque_snk.get('disponivel'))
            ajuste_estoque = {
                    "id": int(estoque_snk.get('ad_mkp_idprod')),
                    "codprod": int(estoque_snk.get('codprod')),
                    "deposito": self.dados_ecommerce.get('id_deposito'),
                    "quantidade": variacao,
                    "tipo": "B"
                }
            resultado = {
                "variacao": variacao,
                "ajuste_estoque": ajuste_estoque
            }
            return resultado
        
        # Se o produto é Kit ou qualquer tipo que não pode ser diretamente movimentado
        # o Olist não informa código de depósito
        if not estoque_olist.get('depositos'):
            logger.error("Não é possível movimentar estoque desse tipo de produto %s",estoque_olist.get('id'))
            print(f"Não é possível movimentar estoque desse tipo de produto {estoque_olist.get('id')}")
            return False

        # Verifica se existe diferença entre o estoque disponível do Sankhya e do Olist
        if int(estoque_olist.get('disponivel')) == int(estoque_snk.get('disponivel')):
            resultado = {
                "variacao": 0,
                "ajuste_estoque": {
                    "id": int(estoque_olist.get('id')),
                    "codprod": int(estoque_snk.get('codprod')),
                    "deposito": 0,
                    "quantidade": 0
                }    
            }
            return resultado

        # Recupera o código do depósito ou busca do banco
        try:
            id_deposito = int(estoque_olist.get('depositos')[0].get('id'))
        except:
            id_deposito = self.dados_ecommerce.get('id_deposito')

        # Calcula a variação do saldo do estoque
        variacao = int(estoque_snk.get('disponivel')) - estoque_olist.get('disponivel')
        ajuste_estoque = {
                "id": int(estoque_olist.get('id')),
                "codprod": int(estoque_snk.get('codprod')),
                "deposito": id_deposito,
                "quantidade":abs(variacao)
            }    
        
        # Define se é movimento de Saída ou Entrada
        if variacao < 0:
            ajuste_estoque["tipo"] = "S"
        if variacao > 0:
            ajuste_estoque["tipo"] = "E"

        resultado = {
            "variacao": variacao,
            "ajuste_estoque": ajuste_estoque
        }

        return resultado

    async def atualizar_log_lote(self, log_id:int, lista_estoque:list):
        try:
            for l in lista_estoque:            
                await crudLogEst.criar(log_id=log_id,
                                       codprod=l['ajuste_estoque'].get('codprod'),
                                       idprod=l['ajuste_estoque'].get('id'),
                                       qtdmov=l.get('variacao'),
                                       sucesso=l.get('sucesso'))
            return True
        except Exception as e:
            logger.error("Erro ao atualizar log de estoques em lote: %s",e)            
            return False

    @contexto
    @log_execucao
    @carrega_dados_empresa
    async def atualizar_olist(self, **kwargs):
        log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                     de='sankhya',
                                     para='olist',
                                     contexto=kwargs.get('_contexto'))        
        # Busca lista de produtos com alterações de estoque no Sankhya
        print("-> Buscando de produtos com alterações de estoque no Sankhya...")
        estoque_snk = EstoqueSnk()
        alteracoes_pendentes = await estoque_snk.buscar_alteracoes()
        if not alteracoes_pendentes:
            print("Sem alterações pendentes")
            await crudLog.atualizar(id=log_id,
                                    sucesso=True)
            return True
        print(f"{len(alteracoes_pendentes)} produtos com alteracoes de estoque")

        # Extrai lista dos produtos
        print("-> Extraindo lista dos produtos...")
        lista_codprod = [int(produto.get('codprod')) for produto in alteracoes_pendentes]
        lista_id = [int(produto.get('idprod')) for produto in alteracoes_pendentes]     

        try:
            # Busca estoque dos produtos no Sankhya e no Olist
            print("-> Buscando estoque dos produtos no Sankhya...")
            dados_estoque_snk = await estoque_snk.buscar(lista_produtos=lista_codprod)
            if not dados_estoque_snk:
                logger.error("Erro ao buscar estoque no Sankhya")                
                raise Exception("Erro ao buscar estoque no Sankhya")

            # Busca estoque dos produtos no Olist        
            print("-> Buscando estoque dos produtos no Olist...")
            estoque_olist = EstoqueOlist()
            dados_estoque_olist = await estoque_olist.buscar(lista_produtos=lista_id)
            if not dados_estoque_olist:
                logger.error("Erro ao buscar estoque no Olist")
                raise Exception("Erro ao buscar estoque no Olist")

            # Calcula variação dos produtos
            print("-> Calculando variação dos produtos...")
            dados_update = self.calcular_variacao(estoque_olist=dados_estoque_olist,
                                                estoque_snk=dados_estoque_snk)
            if not dados_update:
                logger.error("Erro ao calcular variação dos produtos")
                raise Exception("Erro ao calcular variação dos produtos")

            # Envia modificações para Olist
            print("-> Enviando modificações para Olist...")
            res_estoque = await estoque_olist.enviar_saldo(lista_dados=dados_update)
            if not res_estoque:
                logger.error("Erro ao enviar modificações de estoque")
                raise Exception("Erro ao enviar modificações de estoque")

            # Limpa tabela de alterações pendentes
            print("-> Limpando tabela de alterações...")
            ack = await estoque_snk.remover_alteracoes(lista_produtos=res_estoque)
            if not ack:
                logger.error("Erro ao remover alterações pendentes")
                raise Exception("Erro ao remover alterações pendentes")

            # Atualiza o log de eventos
            print("-> Atualizando logs...")
            if not await self.atualizar_log_lote(log_id=log_id,
                                                 lista_estoque=res_estoque):
                raise Exception("Erro ao atualizar log")

            print(f"-> Estoque sincronizado com sucesso!")

            # Registro no log
            status_log = False if await crudLogEst.buscar_falhas(log_id) else True
            await crudLog.atualizar(id=log_id,sucesso=status_log)
            print("--> INTEGRAÇÃO DE ESTOQUE CONCLUÍDA!")
            return True
        except Exception as e:
            obs = f"{e}"
            await crudLogEst.criar(log_id=log_id,
                                   codprod=0,
                                   idprod=0,
                                   qtdmov=0,
                                   obs=obs,
                                   sucesso=False)
            await crudLog.atualizar(id=log_id,
                                    sucesso=False)
            return False