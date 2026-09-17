import json
from pathlib import Path

from src.services.smtp import Email
from src.utils.log import set_logger

logger = set_logger(__name__)
EMAILS_PATH = Path(__file__).parent / "emails.json"


def carregar_emails() -> list[str]:
    try:
        with open(EMAILS_PATH, encoding="utf-8") as file:
            return json.load(file)
    except FileNotFoundError as err:
        logger.error("Não foi possível encontrar os arquivos com emails: %s", err)


async def notificar_erros(topic: str, msg: str):
    email = Email()
    for destinatario in carregar_emails():
        try:
            await email.enviar(destinatario=destinatario, corpo=msg, assunto=topic)
        except Exception as _err:
            logger.error("Não foi possível enviar o e-mail para o %s", destinatario)
