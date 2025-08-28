import os
import logging
from dotenv import load_dotenv
from datetime import datetime
from typing import List, Dict, Any
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Formatter:

    def __init__(self):
        pass    

    def limpar_json(self, dados:dict=None, modelo:dict=None):
        if isinstance(modelo, dict) and isinstance(dados, dict):
            resultado = {}
            for chave in modelo:
                if chave in dados:
                    resultado[chave] = self.limpar_json(dados[chave], modelo[chave])
            return resultado
        elif isinstance(modelo, list) and isinstance(dados, list):
            if modelo:  # usar o primeiro item como modelo se a lista tiver exemplo
                return [self.limpar_json(item, modelo[0]) for item in dados]
            else:
                return []
        else:
            return dados
        
    def return_format_estoque(self, dados: List[Dict[str, Any]]) -> Dict[str, Any]:
        if not dados:
            return {}

        def parse_data(data_str: str) -> datetime:
            return datetime.strptime(data_str, "%d/%m/%Y %H:%M:%S")

        dados = dados['responseBody']['records']['record']
        if not isinstance(dados, list):
            dados = [dados]
        resultado = {
            'codprod': int(dados[0]['CODPROD']['$']),
            'ad_mkp_idprod': int(dados[0]['AD_MKP_IDPROD']['$']),
            'estoque_total': int(dados[0]['ESTOQUE_TOTAL'].get('$',0)),
            'reservado': int(dados[0]['RESERVADO'].get('$',0)),
            'disponivel': int(dados[0]['DISPONIVEL'].get('$',0)),
            'lotes': []
        }

        for item in dados:
            lote = {
                'controle': item['CONTROLE']['$'],
                'dtfabricacao': parse_data(item['DTFABRICACAO']['$']),
                'dtval': parse_data(item['DTVAL']['$']),
                'estoque': int(item['ESTOQUE'].get('$',0))
            }
            resultado['lotes'].append(lote)

        return resultado        

    def return_format(self, res) -> list:

        if res.get('serviceName') == 'DbExplorerSP.executeQuery':
            field_names = [field['name'].lower() for field in res['responseBody']['fieldsMetadata']]
            result = [dict(zip(field_names, row)) for row in res['responseBody']['rows']]
            return result
        
        if res.get('serviceName') == 'CRUDServiceProvider.loadView':
            result = []
            aux = res['responseBody']['records']['record']
            if isinstance(aux, list):
                for item in res['responseBody']['records']['record']:
                    novo_dict = {str.lower(chave): valor['$'] for chave, valor in item.items()}
                    result.append(novo_dict)
            if isinstance(aux, dict):
                result.append({str.lower(chave): valor['$'] for chave, valor in aux.items()})
            return result

        if res['responseBody']['entities']['total'] == '0':
            return [404]

        res_formatted = {}        
        columns = res['responseBody']['entities']['metadata']['fields']['field']
        if isinstance(columns, dict):
            columns = [columns]

        if res['responseBody']['entities']['total'] == '1':
            rows = [res['responseBody']['entities']['entity']]
            try:            
                for row in rows:
                    for i, column in enumerate(columns):                        
                        res_formatted[str.lower(column['name'])] = row.get(f'f{i}').get('$',None)
            except Exception as e:
                logger.error("Erro ao formatar dados da resposta. %s",e)
            finally:
                return [res_formatted]
        else:
            new_res = []
            rows = res['responseBody']['entities']['entity']

            if isinstance(columns, list):
                try:
                    for row in rows:
                        for i, column in enumerate(columns):
                            res_formatted[str.lower(column['name'])] = row.get(f'f{i}').get('$',None)  
                        new_res.append(res_formatted)
                        res_formatted = {}                        
                except Exception as e:
                    logger.error("Erro ao formatar dados da resposta. %s",e)
                finally:
                    return new_res
                
            if isinstance(columns, dict):
                values = []
                try:            
                    for row in rows:
                        values.append(row.get('f0').get('$',None)) 
                    new_res = [{str.lower(columns['name']) : values}]
                except Exception as e:
                    logger.error("Erro ao formatar dados da resposta. %s",e)
                finally:
                    return new_res
