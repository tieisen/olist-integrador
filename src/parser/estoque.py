import os
import json
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Estoque:

    def __init__(self):
        pass

    def to_olist(self,dados_estoque:dict) -> tuple[int,dict]:
        """
        Converte os dados da movimentação de estoque no formato da API do Olist.
            :param dados_estoque: dados da movimentação de estoque
            :return int: ID do produto no Olist
            :return dict: dicionário com os dados do estoque do produto
        """        

        int_res:int=None
        dict_res:dict={}        
        
        try:
            if not isinstance(dados_estoque, dict):
                msg = "Dados inválidos, deve ser um dicionário"
                raise Exception(msg)
            
            with open(os.getenv('OBJECT_ESTOQUE',"src/json/estoque.json"), "r", encoding="utf-8") as f:
                modelo_api = json.load(f)

            if not modelo_api:
                msg = "Erro ao carregar o modelo de dados do estoque"
                raise Exception(msg)

            try:
                dict_res = modelo_api.get('post')
                dict_res['deposito']['id'] = dados_estoque.get('deposito')
                dict_res['tipo'] = dados_estoque.get('tipo')
                dict_res['data'] = None
                dict_res['quantidade'] = dados_estoque.get('quantidade')
                dict_res['precoUnitario'] = 0
                dict_res['observacoes'] = os.getenv('OLIST_OBS_MVTO_ESTOQUE','Ajuste de estoque Sankhya')
                int_res = dados_estoque.get('id')
            except Exception as e:
                msg = f"Erro ao atribuir valores ao dicionário. {e}"
            finally:
                pass
        except Exception as e:
            logger.error(e)
        finally:
            return int_res,dict_res