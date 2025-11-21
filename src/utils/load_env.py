from dotenv import load_dotenv

def load_env() -> load_dotenv:
    """ Carrega o arquivo com as variáveis de ambiente """
    return load_dotenv('keys/.env')