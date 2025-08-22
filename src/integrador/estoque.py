import logging
import os
from database.crud import log, log_estoque
from src.sankhya.estoque import Estoque as EstoqueSnk
from src.olist.estoque import Estoque as EstoqueOlist
from dotenv import load_dotenv
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

CONTEXTO = 'estoque'

class Estoque:

    def __init__(self):
        self.log_id = None

    def calcular_variacao(self, estoque_snk:list, estoque_olist:list=None) -> list:        
        result = []
        if not estoque_olist:
            try:
                for produto_snk in estoque_snk:
                    variacao = int(produto_snk.get('disponivel'))
                    ajuste_estoque = {
                            "id": int(produto_snk.get('ad_mkp_idprod')),
                            "codprod": int(produto_snk.get('codprod')),
                            "deposito": int(os.getenv('OLIST_ID_DEPOSITO_PADRAO')),
                            "quantidade": variacao,
                            "tipo": "B"
                        }  
                    result.append({
                        "variacao": variacao,
                        "ajuste_estoque": ajuste_estoque
                    })
            except Exception as e:
                print(f"Erro ao calcular variação do balanço. {e}")
                return []

        if estoque_olist:

            # Verifica se existe diferença entre o estoque disponível do Sankhya e do Olist
            for produto_snk in estoque_snk:
                for produto_olist in estoque_olist:
                    if int(produto_snk.get('ad_mkp_idprod')) == produto_olist.get('id'):                        
                        break
                
                if int(produto_olist.get('disponivel')) == int(produto_snk.get('disponivel')):
                    result.append({
                        "variacao": 0,
                        "ajuste_estoque": {
                            "id": int(produto_olist.get('id')),
                            "codprod": int(produto_snk.get('codprod')),
                            "deposito": 0,
                            "quantidade": 0
                        }    
                    })                    
                    continue

                variacao = int(produto_snk.get('disponivel')) - produto_olist.get('disponivel')
                ajuste_estoque = {
                        "id": int(produto_olist.get('id')),
                        "codprod": int(produto_snk.get('codprod')),
                        "deposito": int(produto_olist.get('depositos')[0].get('id')),
                        "quantidade":abs(variacao)
                    }    
                if variacao < 0:
                    ajuste_estoque["tipo"] = "S"
                if variacao > 0:
                    ajuste_estoque["tipo"] = "E"

                result.append({
                    "variacao": variacao,
                    "ajuste_estoque": ajuste_estoque
                })

        return result

    def atualizar_log_lote(self, lista_estoque:list):
        try:
            for l in lista_estoque:            
                log_estoque.criar(log_id=self.log_id,
                                  codprod=l['ajuste_estoque'].get('codprod'),
                                  idprod=l['ajuste_estoque'].get('id'),
                                  qtdmov=l.get('variacao'),
                                  status_estoque=l.get('sucesso'))
            return True
        except Exception as e:
            logger.error("Erro ao atualizar log de estoques em lote: %s",e)
            print(f"Erro: {e}")
            return False

    async def atualizar_olist(self, balanco:bool=False):
        # Registro no log
        self.log_id = log.criar(de='sankhya', para='olist', contexto=CONTEXTO)

        estoque_snk = EstoqueSnk()
        estoque_olist = EstoqueOlist()
        
        # Busca lista de produtos com alterações no Sankhya
        print("Busca lista de produtos com alterações de estoque")
        alteracoes_pendentes = await estoque_snk.buscar_alteracoes()

        print(f"{len(alteracoes_pendentes)} produtos com alteracoes de estoque")
        
        if len(alteracoes_pendentes) == 0:
            print("Sem alterações pendentes")
            # Registro no log
            log_estoque.criar(log_id=self.log_id,
                              codprod=0,
                              idprod=0,
                              obs = "Sem alterações pendentes")
            log.atualizar(id=self.log_id)
            return True

        # Extrai lista dos produtos
        print("Extraindo lista dos produtos...")
        lista_codprod = [int(produto.get('codprod')) for produto in alteracoes_pendentes]
        lista_id = [int(produto.get('idprod')) for produto in alteracoes_pendentes]

        print(f"{len(lista_codprod)} produtos para sincronizar.")

        try:
            if balanco:
                # Busca estoque dos produtos no Sankhya
                print("Buscando estoque dos produtos no Sankhya...")
                dados_estoque_snk = await estoque_snk.buscar(lista_produtos=lista_codprod)

                # Calcula variação dos produtos
                print("Calculando variação dos produtos...")
                dados_update = self.calcular_variacao(estoque_snk=dados_estoque_snk)
            else: 
                # Busca estoque dos produtos no Sankhya e no Olist
                print("Buscando estoque dos produtos no Sankhya...")
                dados_estoque_snk = await estoque_snk.buscar(lista_produtos=lista_codprod)
                print("Buscando estoque dos produtos no Olist...")
                dados_estoque_olist = await estoque_olist.buscar(lista_produtos=lista_id)

                # Calcula variação dos produtos
                print("Calculando variação dos produtos...")
                dados_update = self.calcular_variacao(estoque_olist=dados_estoque_olist,
                                                      estoque_snk=dados_estoque_snk)

            # Envia modificações para Olist
            print("Enviando modificações para Olist...")
            res_estoque = await estoque_olist.enviar_saldo(lista_dados=dados_update)
            if not res_estoque:
                print("Erro ao enviar modificações de estoque")
                logger.error("Erro ao enviar modificações de estoque")
                return False

            # Limpa tabela de alterações pendentes
            print("Limpando tabela de alterações")
            ack = await estoque_snk.remover_alteracoes(lista_produtos=res_estoque)
            if not ack:
                print("Erro ao remover alterações pendentes")
                logger.error("Erro ao remover alterações pendentes")

            print("Atualizando log")
            if not self.atualizar_log_lote(lista_estoque=res_estoque):
                print("Erro ao atualizar log")
                logger.error("Erro ao atualizar log")

            print(f"Estoque sincronizado com sucesso!")

            # Registro no log
            status_log = False if log_estoque.buscar_status_false(self.log_id) else True
            log.atualizar(id=self.log_id, sucesso=status_log)

            return True
        except Exception as e:
            obs = f"Erro ao atualizar estoque no olist: {e}"
            log_estoque.criar(log_id=self.log_id,
                              codprod=0,
                              idprod=0,
                              qtdmov=0,
                              obs=obs,
                              status_estoque=False)            
            log.atualizar(id=self.log_id, sucesso=False)
            logger.error(obs)
            return False