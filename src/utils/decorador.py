import inspect, asyncio, time
from functools import wraps
from database.crud import ecommerce, empresa, shopee

# EXTRAI CONTEXTO 
def contexto(func):
    """
    Extrai o nome da função do contexto que está em execução.
        :param func: função que recebe o decorador
    """
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
    res = await ecommerce.buscar(id_loja=self.id_loja)
    self.dados_ecommerce = res[0]

def carrega_dados_ecommerce(func):
    """
    Carrega os dados do ecommerce na memória.
        :param func: função que recebe o decorador
    """
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        # Garante que os dados da empresa estão carregados
        if not self.dados_ecommerce:
            await buscar_dados_ecommerce(self)
        return await func(self, *args, **kwargs)
    return wrapper

# DADOS DA EMPRESA
async def buscar_dados_empresa(self):
    res = await empresa.buscar(id=self.empresa_id, codemp=self.codemp)
    self.dados_empresa = res[0]

def carrega_dados_empresa(func):
    """
    Carrega os dados da empresa na memória.
        :param func: função que recebe o decorador
    """
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        # Garante que os dados da empresa estão carregados
        if not self.dados_empresa:
            await buscar_dados_empresa(self)
        return await func(self, *args, **kwargs)
    return wrapper

# DADOS DA SHOPEE
async def buscar_dados_shopee(self):
    res = await shopee.buscar(ecommerce_id=self.ecommerce_id, empresa_id=self.empresa_id)
    if isinstance(res, list):
        self.dados_shopee = res[0]
    else:
        self.dados_shopee = res

def carrega_dados_shopee(func):
    """
    Carrega os dados da loja Shopee na memória.
        :param func: função que recebe o decorador
    """
    @wraps(func)
    async def wrapper(self, *args, **kwargs):
        # Garante que os dados da empresa estão carregados
        if not self.dados_shopee:
            await buscar_dados_shopee(self)
        return await func(self, *args, **kwargs)
    return wrapper

# BLOQUEIA CHAMADA DIRETA DA FUNCAO
def interno(func):
    """
    Bloqueia chamada direta da função.
        :param func: função que recebe o decorador
    """
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
    """
    Imprime a função que está sendo executada.
        :param func: função que recebe o decorador
    """
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
