from database.crud import ecommerce
from functools import wraps

async def buscar_dados_ecommerce(self):
    if not self.dados_ecommerce:
        self.dados_ecommerce = await ecommerce.buscar(id_loja=self.id_loja)

def ensure_dados_ecommerce(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        # Garante que os dados da empresa estão carregados
        if not self.dados_ecommerce:
            await buscar_dados_ecommerce(self)
        return await func(self, *args, **kwargs)
    return wrapper