import asyncio
from src.services.smtp import Email

# ROTINA A SER EXECUTADA DIARIAMENTE, A CADA 1H

if __name__=="__main__":    
    email = Email()
    asyncio.run(email.notificar())