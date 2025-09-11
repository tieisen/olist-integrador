from functools import wraps

def contexto(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        # injeta o nome da função em kwargs
        kwargs["_contexto"] = self.contexto+':'+func.__name__
        return func(self,*args, **kwargs)
    return wrapper
