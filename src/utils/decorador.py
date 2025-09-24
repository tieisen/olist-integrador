import inspect
import asyncio
import time
from functools import wraps
from database.crud import ecommerce
from database.crud import empresa
from src.olist.autenticacao import Autenticacao as AuthOlist
from src.sankhya.autenticacao import Autenticacao as AuthSnk

# EXTRAI CONTEXTO 
def contexto(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        # injeta o nome da função em kwargs
        kwargs["_contexto"] = self.contexto+':'+func.__name__
        return func(self,*args, **kwargs)
    return wrapper

# DESABILITA BLOCO DE CÓDIGO
def desabilitado(func):
    def wrapper(*args, **kwargs):
        print(f"Função {func.__name__} está desativada!")
    return wrapper

# DADOS DO ECOMMERCE
async def buscar_dados_ecommerce(self):
    if not self.dados_ecommerce:
        self.dados_ecommerce = await ecommerce.buscar(id_loja=self.id_loja)

def carrega_dados_ecommerce(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        # Garante que os dados da empresa estão carregados
        if not self.dados_ecommerce:
            await buscar_dados_ecommerce(self)
        return await func(self, *args, **kwargs)
    return wrapper

# DADOS DA EMPRESA
async def buscar_dados_empresa(self):
    if not self.dados_empresa:
        self.dados_empresa = await empresa.buscar(id=self.empresa_id, codemp=self.codemp)

def carrega_dados_empresa(func):
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        # Garante que os dados da empresa estão carregados
        if not self.dados_empresa:
            await buscar_dados_empresa(self)
        return await func(self, *args, **kwargs)
    return wrapper

# BLOQUEIA CHAMADA DIRETA DA FUNCAO
def interno(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        # Pega a pilha de chamadas
        stack = inspect.stack()
        # Verifica a função chamadora (logo acima na pilha)
        caller = stack[1].function

        # Lista de métodos da classe
        metodos_classe = dir(self.__class__)
        if metodos_classe and 'wrapper' not in metodos_classe:
            metodos_classe.append('wrapper')

        if caller not in metodos_classe:
            raise PermissionError(
                f"O método '{func.__name__}' só pode ser chamado internamente pela classe '{self.__class__.__name__}'."
            )

        return func(self, *args, **kwargs)
    return wrapper

# IMPRIME LOG DE EXECUCAO
def log_execucao(func):
    @wraps(func)
    async def async_wrapper(*args, **kwargs):
        nome_funcao = func.__name__.replace("_", " ").upper()

        # se for método de classe, adiciona o nome da classe
        if args and hasattr(args[0], "__class__"):
            nome_classe = args[0].__class__.__name__
            nome_funcao = f"{nome_classe}.{nome_funcao}"

        # Cabeçalho
        print(nome_funcao)
        print("=" * 60)
        # print("ARGS:", args[1:] if len(args) > 1 else args)  # ignorar self
        # print("KWARGS:", kwargs)
        
        inicio = time.perf_counter()
        try:
            resultado = await func(*args, **kwargs)
            return resultado
        finally:
            fim = time.perf_counter()
            duracao = fim - inicio
            print("=" * 60)
            print(f"--> ROTINA CONCLUÍDA! (Tempo: {duracao:.4f}s)\n")

    @wraps(func)
    def sync_wrapper(*args, **kwargs):
        nome_funcao = func.__name__.replace("_", " ").upper()

        # se for método de classe, adiciona o nome da classe
        if args and hasattr(args[0], "__class__"):
            nome_classe = args[0].__class__.__name__
            nome_funcao = f"{nome_classe}.{nome_funcao}"

        # Cabeçalho
        print(nome_funcao)
        print("=" * 60)
        # print("ARGS:", args[1:] if len(args) > 1 else args)
        # print("KWARGS:", kwargs)

        inicio = time.perf_counter()
        try:
            resultado = func(*args, **kwargs)
            return resultado
        finally:
            fim = time.perf_counter()
            duracao = fim - inicio
            print("=" * 60)
            print(f"--> ROTINA CONCLUÍDA! (Tempo: {duracao:.4f}s)\n")

    return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

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

