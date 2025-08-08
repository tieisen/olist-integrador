import os
import json
import logging
from dotenv import load_dotenv
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Estoque:

    def __init__(self):
        pass

    def to_olist(self, data:dict=None) -> tuple[int,dict]:

        if not data:
            logger.error("Dados não informados.")
            print("Dados não informados.")
            return False, None
        
        if not isinstance(data, dict):
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
            new_data['deposito']['id'] = data.get('deposito')
            new_data['tipo'] = data.get('tipo')
            new_data['data'] = None
            new_data['quantidade'] = data.get('quantidade')
            new_data['precoUnitario'] = 0
            new_data['observacoes'] = os.getenv('OLIST_OBS_MVTO_ESTOQUE','Ajuste de estoque Sankhya')
            return data.get('id'), new_data
        except Exception as e:
            logger.error("Erro ao atribuir valores ao dicionário. %s",e)
            print("Erro ao atribuir valores ao dicionário.",e)
            return False, None