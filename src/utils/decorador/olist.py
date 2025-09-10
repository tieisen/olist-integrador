from src.olist.autenticacao import Autenticacao
from functools import wraps

async def buscar_token(self):
    if not self.token:
        self.token = await Autenticacao(codemp=self.codemp).autenticar()

def ensure_token(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        # Garante que os dados da empresa estão carregados
        if not self.token:
            await buscar_token(self)
        return await func(self, *args, **kwargs)
    return wrapper        
