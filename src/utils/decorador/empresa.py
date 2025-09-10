from database.crud import empresa
from functools import wraps

async def buscar_dados_empresa(self):
    if not self.dados_empresa:
        self.dados_empresa = await empresa.buscar(codemp=self.codemp)

def ensure_dados_empresa(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        # Garante que os dados da empresa estão carregados
        if not self.dados_empresa:
            await buscar_dados_empresa(self)
        return await func(self, *args, **kwargs)
    return wrapper