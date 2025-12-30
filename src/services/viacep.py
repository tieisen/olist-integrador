import re, requests
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Viacep:

    def __init__(self):
        self.regex = r"\D"

    async def busca_ibge_pelo_cep(self,cep:str) -> int:
        """
        Busca o código IBGE do município pelo CEP
            :param cep: CEP
        """
        codigo_ibge:int=None

        try:
            cep = re.sub(self.regex,"",cep)
            if len(cep) != 8:
                raise ValueError(f"CEP inválido: {cep}")
            
            res = requests.get(f"https://viacep.com.br/ws/{cep}/json/")
            codigo_ibge = int(res.json().get('ibge')) if res.ok else 0
        except Exception as e:
            codigo_ibge = 0
            logger.error("Erro ao buscar código IBGE pelo CEP: %s",e)
        finally:
            pass
        return codigo_ibge
