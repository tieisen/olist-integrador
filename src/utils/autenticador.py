from functools import wraps
from src.olist.autenticacao import Autenticacao as AuthOlist
from src.sankhya.autenticacao import Autenticacao as AuthSnk

# BUSCA TOKEN OLIST
async def buscar_token_olist(self):
    if not self.token:
        token = await AuthOlist(codemp=self.codemp,empresa_id=self.empresa_id).autenticar()
        self.token = token.strip('"')

def token_olist(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        # Garante que os dados da empresa estão carregados
        if not self.token:
            await buscar_token_olist(self)
        return await func(self, *args, **kwargs)
    return wrapper    

# BUSCA TOKEN SANKHYA
async def buscar_token_snk(self):
    if not self.token:
        token = await AuthSnk(codemp=self.codemp,empresa_id=self.empresa_id).autenticar()
        self.token = token.strip('"')

def token_snk(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        # Garante que os dados da empresa estão carregados
        if not self.token:
            await buscar_token_snk(self)
        return await func(self, *args, **kwargs)
    return wrapper

