import os
import time
from database.crud import log as crudLog
from database.crud import log_estoque as crudLogEst
from src.sankhya.estoque import Estoque as EstoqueSnk
from src.olist.estoque import Estoque as EstoqueOlist
from src.parser.estoque import Estoque as Parser
from src.utils.decorador import contexto, carrega_dados_empresa, log_execucao, interno
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Estoque:

    def __init__(self, codemp:int):
        self.codemp = codemp
        self.empresa_id = None
        self.contexto = 'estoque'
        self.dados_empresa:dict = {}
        self.req_time_sleep:float=float(os.getenv('REQ_TIME_SLEEP', 1.5))

    @interno
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
                    "id": int(estoque_snk.get('idprod')),
                    "codprod": int(estoque_snk.get('codprod')),
                    "deposito": self.dados_empresa.get('olist_id_deposito_padrao'),
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
            id_deposito = self.dados_empresa.get('olist_id_deposito_padrao')

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

    @interno
    async def atualizar_log_lote(
            self,
            log_id:int,
            lista_estoque:list
        ) -> bool:
        try:
            l:dict={}
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

        estoque_snk = EstoqueSnk(codemp=self.codemp)
        estoque_olist = EstoqueOlist(codemp=self.codemp)
        parser = Parser()
        
        log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                     de='sankhya',
                                     para='olist',
                                     contexto=kwargs.get('_contexto'))        
        # Busca lista de produtos com alterações de estoque no Sankhya
        print("-> Buscando de produtos com alterações de estoque no Sankhya...")        
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
                msg = f"Erro ao buscar estoque no Sankhya. Parametro: {lista_codprod}"
                raise Exception(msg)

            # Busca estoque dos produtos no Olist        
            print("-> Buscando estoque dos produtos no Olist...")            
            dados_estoque_olist = await estoque_olist.buscar(lista_produtos=lista_id)
            if not dados_estoque_olist:
                msg = f"Erro ao buscar estoque no Olist. Parametro: {lista_id}"
                raise Exception(msg)

            # Compara os estoques e calcula as variações
            for i, produto in enumerate(alteracoes_pendentes):
                dados_update:dict = {}
                res_estoque:dict = {}
                time.sleep(self.req_time_sleep)
                print(f"-> Produto {i + 1}/{len(alteracoes_pendentes)}: {produto.get('codprod')}")
                
                # Calcula variação do produto
                print("Calculando variação do produto...")
                dados_update = self.calcular_variacao(estoque_olist=dados_estoque_olist[i],
                                                      estoque_snk=dados_estoque_snk[i])
                if not dados_update:
                    msg = f"Erro ao calcular variação do produto {produto.get('codprod')}"
                    raise Exception(msg)                
                dados_update = parser.to_olist(dados_estoque=dados_update.get('ajuste_estoque'))

                # Envia modificações para Olist
                print("Enviando modificações para Olist...")
                res_estoque = await estoque_olist.enviar_saldo(id=dados_update[0],
                                                               data=dados_update[1])
                if not res_estoque:
                    msg = f"Erro ao enviar modificações de estoque para o Olist. Produto {produto.get('codprod')}"
                    raise Exception(msg)

                # Limpa tabela de alterações pendentes
                print("Limpando tabela de alterações...")
                ack = await estoque_snk.remover_alteracoes(codprod=produto.get('codprod'))
                if not ack:
                    msg = f"Erro ao remover alterações pendentes. Produto {produto.get('codprod')}"
                    raise Exception(msg)

                # Atualiza o log de eventos
                print("Atualizando logs...")
                ack = await crudLogEst.criar(log_id=log_id,
                                             codprod=int(produto.get('codprod')),
                                             idprod=int(produto.get('idprod')),
                                             qtdmov=int(produto.get('variacao')),
                                             sucesso=bool(produto.get('sucesso')))
                if not ack:
                    msg = f"Erro ao atualizar log. Produto {produto.get('codprod')}"
                    raise Exception(msg)

            print(f"Estoque sincronizado com sucesso!")

        except Exception as e:
            obs = f"{e}"
            print(obs)
            await crudLogEst.criar(log_id=log_id,
                                   codprod=0,
                                   idprod=0,
                                   qtdmov=0,
                                   obs=obs,
                                   sucesso=False)
        finally:
            # Registro no log
            status_log = False if await crudLogEst.buscar_falhas(log_id) else True
            await crudLog.atualizar(id=log_id,sucesso=status_log)
            return status_log            