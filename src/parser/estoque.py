import os
import json
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Estoque:

    def __init__(self):
        pass

    def to_olist(self, dados_estoque:dict) -> tuple[int,dict]:
        
        if not isinstance(dados_estoque, dict):
            logger.error("Dados inválidos, deve ser um dicionário.")
            print("Dados inválidos, deve ser um dicionário.")
            return False, None
        
        with open(os.getenv('OBJECT_ESTOQUE',"src/json/estoque.json"), "r", encoding="utf-8") as f:
            modelo_api = json.load(f)

        if not modelo_api:
            logger.error("Erro ao carregar o modelo de dados do estoque.")
            print("Erro ao carregar o modelo de dados do estoque.")
            return False, None

        try:
            new_data = modelo_api.get('post')
            new_data['deposito']['id'] = dados_estoque.get('deposito')
            new_data['tipo'] = dados_estoque.get('tipo')
            new_data['data'] = None
            new_data['quantidade'] = dados_estoque.get('quantidade')
            new_data['precoUnitario'] = 0
            new_data['observacoes'] = os.getenv('OLIST_OBS_MVTO_ESTOQUE','Ajuste de estoque Sankhya')
            return dados_estoque.get('id'), new_data
        except Exception as e:
            logger.error("Erro ao atribuir valores ao dicionário. %s",e)
            print("Erro ao atribuir valores ao dicionário.",e)
            return False, None