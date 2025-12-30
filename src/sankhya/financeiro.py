import os, requests
from src.utils.autenticador import token_snk
from src.utils.formatter import Formatter
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Financeiro:

    def __init__(self, codemp:int=None, empresa_id:int=None):
        self.token:int=None
        self.codemp = codemp
        self.dados_empresa:dict={}
        self.empresa_id = empresa_id
        self.formatter = Formatter()
        self.req_time_sleep = float(os.getenv('REQ_TIME_SLEEP', 1.5))        

    @token_snk
    async def buscar(self,nufin:int=None,nunota:int=None,numnota:int=None) -> list[dict]:
        """
        Busca um registro financeiro ou todos os registros financeiros de uma nota.
            :param nufin: número único do registro financeiro
            :param nunota: número único da nota de venda
            :param numnota: número da NF (Sankhya)
            :return list[dict]: lista de dicionários com os dados dos registros financeiros
        """

        def monta_expressao(nufin:int=None,nunota:int=None,numnota:int=None):
            nonlocal criteria

            if not any([nufin,nunota,numnota]):
                return False
            
            try:
                if nufin:
                    criteria = {
                        "expression": {
                            "$": "this.NUFIN = ?"
                        },
                        "parameter": [
                            {
                                "$": f"{nufin}",
                                "type": "I"
                            }
                        ]
                    }
                elif nunota:
                    criteria = {
                        "expression": {
                            "$": "this.NUNOTA = ?"
                        },
                        "parameter": [
                            {
                                "$": f"{nunota}",
                                "type": "I"
                            }
                        ]
                    }
                elif numnota:
                    criteria = {
                        "expression": {
                            "$": "this.NUMNOTA = ?"
                        },
                        "parameter": [
                            {
                                "$": f"{numnota}",
                                "type": "I"
                            }
                        ]
                    }
                else:
                    pass
                return True
            except Exception as e:
                logger.error(f"Erro ao montar expressão: {e}")
                return False

        url:str = ''
        fieldset_list:str = ''
        criteria:dict={}

        if not any([nufin,nunota,numnota]):            
            return False        
        url = os.getenv('SANKHYA_URL_LOAD_RECORDS') 
        if not url:
            logger.error("Erro relacionado à url. %s",url)
            return False
        fieldset_list:str = ','.join(self.campos)
        if not fieldset_list:
            logger.error("Erro relacionado aos campos da busca. %s",fieldset_list)
            return False            
        if not monta_expressao(nufin=nufin,nunota=nunota,numnota=numnota):
            return False

        res = requests.get(
            url=url,
            headers={ "Authorization":f"Bearer {self.token}" },
            json={
                "serviceName": "CRUDServiceProvider.loadRecords",
                "requestBody": {
                    "dataSet": {
                        "rootEntity": "Financeiro",
                        "includePresentationFields": "N",
                        "offsetPage": "0",
                        "criteria": criteria,
                        "entity": {
                            "fieldset": {
                                "list": fieldset_list
                            }
                        }
                    }
                }
            })
        
        if res.ok:
            return self.formatter.return_format(res.json())
        else:
            if nunota:
                logger.error("Erro ao buscar financeiro. Nunota %s. %s",nunota,res.json())
            elif numnota:
                logger.error("Erro ao buscar financeiro. Numnota %s. %s",numnota,res.json())
            elif nufin:
                logger.error("Erro ao buscar financeiro. Nufin %s. %s",nufin,res.json())
            else:
                logger.error("Erro ao buscar financeiro. %s",res.json())
            return False
        
    @token_snk
    async def lancar(self,dados:dict) -> int:
        """
        Cria um registro de financeiro.
            :param dados: dicionário com os dados do registro financeiro
            :return int: número único do registro financeiro
        """        
        
        pass