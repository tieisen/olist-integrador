from database.crud import empresa
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

class Empresa:

    def __init__(self):
        pass

    async def buscar(self,id:int=None,codemp:int=None) -> list[dict]:
        """
        Busca dados da empresa.
            :param id: id da empresa
            :param codemp: código da empresa
            :return list[dict]: lista com os dados da empresa
        """

        dados_empresas:list[dict]=[]
        dados_empresas = await empresa.buscar(id=id,codemp=codemp)
        return dados_empresas