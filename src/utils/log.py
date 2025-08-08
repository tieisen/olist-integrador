import os
from datetime import datetime

class Log:

    def __init__(self):
        pass

    def buscar_path(self):
        mes_atual = datetime.now().strftime('%Y%m')
        if not os.path.exists(f"./logs/{mes_atual}.log"):
            with open(f"./logs/{mes_atual}.log", "w") as f:
                pass
        return f"./logs/{mes_atual}.log"