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
    def calcular_variacao(self,estoque_snk:dict,estoque_olist:dict=None) -> dict:
        """
        Calcula a diferença do saldo de estoque entre Sankhya e Olist
            :param estoque_snk: dicionário com os dados de estoque do Sankhya
            :param estoque_olist: dicionário com os dados de estoque do Olist
            :return dict: dicionário com a variação de estoque e os dados para enviar a atualização para o Olist
        """
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
        
        if isinstance(estoque_olist,list):
            estoque_olist = estoque_olist[0]

        # Se o produto é Kit ou qualquer tipo que não pode ser diretamente movimentado
        # o Olist não informa código de depósito
        if not estoque_olist.get('depositos'):
            logger.error("Não é possível movimentar estoque desse tipo de produto %s",estoque_olist.get('id'))
            return False

        # Verifica se existe diferença entre o estoque disponível do Sankhya e do Olist
        if int(estoque_olist.get('saldo')) == int(estoque_snk.get('disponivel')):
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
        variacao = int(estoque_snk.get('disponivel')) - estoque_olist.get('saldo')
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

    @contexto
    @log_execucao
    @carrega_dados_empresa
    async def atualizar_olist(self, **kwargs) -> bool:
        """
        Rotina de atualização de estoque no Olist
            :return bool: status da operação
        """
        
        def buscar_produto(codprod:int,lista_produtos:list[dict]) -> dict:
            dict_res:dict={}
            for produto in lista_produtos:
                if produto.get('codprod') == int(codprod):
                    dict_res = produto
                    break
            return dict_res

        estoque_snk = EstoqueSnk(codemp=self.codemp)
        estoque_olist = EstoqueOlist(codemp=self.codemp)
        parser = Parser()
        
        log_id = await crudLog.criar(empresa_id=self.dados_empresa.get('id'),
                                     de='sankhya',
                                     para='olist',
                                     contexto=kwargs.get('_contexto'))
        
        # Busca lista de produtos com alterações de estoque no Sankhya
        alteracoes_pendentes = await estoque_snk.buscar_alteracoes(codemp=self.dados_empresa.get('snk_codemp_fornecedor'))
        if not alteracoes_pendentes:
            await crudLog.atualizar(id=log_id,sucesso=True)
            return True
        
        # Extrai lista dos produtos
        lista_codprod = [int(produto.get('codprod')) for produto in alteracoes_pendentes]
        produtos_buscar:list[int]=[]
        limite_lista:int=300        
        if len(lista_codprod) > limite_lista:
            produtos_buscar = lista_codprod[:limite_lista]
            logger.warning("A lista de produtos excedeu o limite de %s itens. Apenas os %s primeiros serão considerados na consulta.",limite_lista,limite_lista)
        else:                
            produtos_buscar = lista_codprod.copy()

        try:
            # Busca estoque dos produtos no Sankhya e no Olist
            lista_dados_estoque_snk = await estoque_snk.buscar(lista_produtos=produtos_buscar)
            if not lista_dados_estoque_snk:
                msg = f"Erro ao buscar estoque no Sankhya. Parametro: {produtos_buscar}"
                raise Exception(msg)

            # Compara os estoques e calcula as variações
            alteracoes_pendentes = alteracoes_pendentes[:limite_lista]
            for i, produto in enumerate(alteracoes_pendentes):
                dados_update:dict = {}
                res_estoque:dict = {}
                time.sleep(self.req_time_sleep)

                # Busca estoque dos produtos no Olist        
                dados_estoque_olist = await estoque_olist.buscar(id=produto.get('idprod'))
                if not dados_estoque_olist:
                    msg = f"Erro ao buscar estoque no Olist. Parametro: {produto.get('idprod')}"
                    logger.error(msg)                                     
                    ack = await estoque_snk.remover_alteracoes(codprod=produto.get('codprod'))                  
                    continue
                
                if isinstance(dados_estoque_olist,list):
                    dados_estoque_olist = dados_estoque_olist[0]

                # Calcula variação do produto
                dados_estoque_snk:dict = {}
                dados_estoque_snk = buscar_produto(codprod=produto.get('codprod'),lista_produtos=lista_dados_estoque_snk)
                if not dados_estoque_snk:
                    msg = f"Não foi possível encontrar o produto. \nProduto procurado: {produto.get('codprod')}"
                    raise Exception(msg)
                if int(dados_estoque_olist.get('id')) != int(dados_estoque_snk.get('idprod')):
                    msg = f"Erro ao buscar produto correto no Sankhya\ndados_estoque_olist:{dados_estoque_olist}\ndados_estoque_snk:{dados_estoque_snk}"
                    raise Exception(msg)
                
                dados_update = self.calcular_variacao(estoque_olist=dados_estoque_olist,estoque_snk=dados_estoque_snk)
                if not dados_update:
                    msg = f"Erro ao calcular variação do produto {produto.get('codprod')}"
                    raise Exception(msg)
                if dados_update.get('variacao') == 0:
                    # Limpa tabela de alterações pendentes
                    await estoque_snk.remover_alteracoes(codprod=produto.get('codprod'))

                # Converte para o formato da API
                id_produto:int=None
                dicionario_mvto_estoque:dict={}
                id_produto, dicionario_mvto_estoque = parser.to_olist(dados_estoque=dados_update.get('ajuste_estoque'))
                if not all([id_produto,dicionario_mvto_estoque]):
                    msg = f"Erro ao converter para o formato da API.\nid_produto: {id_produto}\ndicionario_mvto_estoque:{dicionario_mvto_estoque}"
                    raise Exception(msg)

                # Envia modificações para Olist
                res_estoque = await estoque_olist.enviar_saldo(id=id_produto,data=dicionario_mvto_estoque)
                if not res_estoque:
                    msg = f"Erro ao enviar modificações de estoque para o Olist. Produto {produto.get('codprod')}"
                    raise Exception(msg)

                # Limpa tabela de alterações pendentes
                await estoque_snk.remover_alteracoes(codprod=produto.get('codprod'))

                # Atualiza o log de eventos
                ack = await crudLogEst.criar(log_id=log_id,
                                             codprod=int(produto.get('codprod')),
                                             idprod=int(produto.get('idprod')),
                                             qtdmov=int(dados_update.get('variacao')),
                                             sucesso=bool(produto.get('sucesso')))
                if not ack:
                    msg = f"Erro ao atualizar log. Produto {produto.get('codprod')}"
                    raise Exception(msg)

        except Exception as e:
            obs = f"{e}"
            print(obs)
            logger.error("Erro ao atualizar estoque no Olist: %s",obs)            
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