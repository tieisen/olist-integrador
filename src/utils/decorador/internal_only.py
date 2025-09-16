import inspect
import functools

def internal_only(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # Pega a pilha de chamadas
        stack = inspect.stack()
        # Verifica a função chamadora (logo acima na pilha)
        caller = stack[1].function

        # Lista de métodos da classe
        metodos_classe = dir(self.__class__)

        if caller not in metodos_classe:
            raise PermissionError(
                f"O método '{func.__name__}' só pode ser chamado internamente pela classe '{self.__class__.__name__}'."
            )

        return func(self, *args, **kwargs)
    return wrapper
