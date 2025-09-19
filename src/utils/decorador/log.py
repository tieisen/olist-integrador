import functools
import asyncio
import time

def log_execucao(func):
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        nome_funcao = func.__name__.replace("_", " ").upper()

        # se for método de classe, adiciona o nome da classe
        if args and hasattr(args[0], "__class__"):
            nome_classe = args[0].__class__.__name__
            nome_funcao = f"{nome_classe}.{nome_funcao}"

        # Cabeçalho
        print("\n" + nome_funcao)
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
            print("\n" + "=" * 60)
            print(f"--> ROTINA CONCLUÍDA! (Tempo: {duracao:.4f}s)\n")

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        nome_funcao = func.__name__.replace("_", " ").upper()

        # se for método de classe, adiciona o nome da classe
        if args and hasattr(args[0], "__class__"):
            nome_classe = args[0].__class__.__name__
            nome_funcao = f"{nome_classe}.{nome_funcao}"

        # Cabeçalho
        print("\n" + nome_funcao)
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
            print("\n" + "=" * 60)
            print(f"--> ROTINA CONCLUÍDA! (Tempo: {duracao:.4f}s)\n")

    return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
