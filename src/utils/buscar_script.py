import os
from src.utils.log import set_logger
from src.utils.load_env import load_env
load_env()
logger = set_logger(__name__)

def buscar_script(parametro:str) -> str:
    """
    Busca no diretório um script SQL com base na variável de ambiente.
        :param parametro: nome da variável de ambiente
    """
    script:str=''
    try:
        path:str = os.getenv(parametro)
        if not path:
            erro = f"Parâmetro do diretório do script não informado. param: {path}"
            raise ValueError(erro)
    
        try:
            with open(path, "r") as file:
                script = " ".join(line.strip() for line in file)
        except Exception as e:
            erro = f"Falha ao abrir arquivo do script em {path}. {e}"
            raise FileNotFoundError(erro)
    
        if not script.strip():
            erro = f"Arquivo carregado de {path} sem conteúdo"
            raise ValueError(erro)
    except Exception as e:
        logger.error(f"{e}")
    finally:
        pass

    return script
