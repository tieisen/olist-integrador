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

def buscar_relatorio_custos() -> list[dict]:
    """
    Busca o relatório de custos em um arquivo Excel.
        :return list[dict]: lista com os dados do relatório de custos
    """
    import pandas as pd

    def path_valido() -> bool:
        from pathlib import Path
        pasta = Path.home() / "Downloads"
        filtro = "custos-ecommerce"
        arquivos = [f for f in pasta.iterdir() if filtro.lower() in f.name.lower()]
        if not arquivos:
            return False
        
        if os.path.isfile(arquivos[-1]) and os.access(arquivos[-1], os.R_OK):
            return arquivos[-1]
        return False

    relatorio_custos:list[dict] = []
    try:
        path_arquivo = path_valido()
        if not path_arquivo:
            raise FileNotFoundError("Arquivo de relatório de custos não encontrado.")
        df = pd.read_excel(path_arquivo, engine='calamine', usecols='A:L')
        relatorio_custos = df.to_dict(orient='records')
    except Exception as e:
        logger.error(f"Falha ao carregar relatório de custos do arquivo {path_arquivo}. {e}")
    finally:
        return relatorio_custos