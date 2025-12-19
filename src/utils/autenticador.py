from functools import wraps
from src.olist.autenticacao import Autenticacao as AuthOlist
from src.sankhya.autenticacao import Autenticacao as AuthSnk

async def buscar_token_olist(self):
    token = await AuthOlist(codemp=self.codemp,empresa_id=self.empresa_id).autenticar()
    self.token = token.strip('"')

def token_olist(func):
    """
    Busca o token da API do Olist
        :param func: função que recebe o decorador
    """    
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        await buscar_token_olist(self)
        return await func(self, *args, **kwargs)
    return wrapper    

async def buscar_token_snk(self):
    token = await AuthSnk(codemp=self.codemp,empresa_id=self.empresa_id).autenticar()
    self.token = token.strip('"')

def token_snk(func):
    """
    Busca o token da API do Sankhya
        :param func: função que recebe o decorador
    """        
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        await buscar_token_snk(self)
        return await func(self, *args, **kwargs)
    return wrapper

