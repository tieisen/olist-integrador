import inspect
import functools
import asyncio

def log_execucao(func):
    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        nome_funcao = func.__name__.replace("_", " ").upper()

        # se for método de classe, adiciona o nome da classe
        if args and hasattr(args[0], "__class__"):
            nome_classe = args[0].__class__.__name__
            nome_funcao = f"{nome_classe}.{nome_funcao}"

        print("\n" + nome_funcao)
        print("=" * 60)
        return await func(*args, **kwargs)

    @functools.wraps(func)
    def sync_wrapper(*args, **kwargs):
        nome_funcao = func.__name__.replace("_", " ").upper()

        # se for método de classe, adiciona o nome da classe
        if args and hasattr(args[0], "__class__"):
            nome_classe = args[0].__class__.__name__
            nome_funcao = f"{nome_classe}.{nome_funcao}"

        print("\n" + nome_funcao)
        print("=" * 60)
        return func(*args, **kwargs)

    # decide se é função async ou normal
    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    else:
        return sync_wrapper
