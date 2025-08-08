import re
import os
import json
import base64
import logging
import asyncio
from dotenv import load_dotenv
from pathlib import Path
from src.services.smtp import Email
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

class Utils:

    def __init__(self):
        pass    
        
    def embed_local_images_in_markdown(markdown_text, base_path="."):
        """
        Substitui os paths de imagens locais no markdown por imagens embutidas em base64.

        Args:
            markdown_text (str): Conteúdo do arquivo markdown.
            base_path (str): Pasta base onde estão as imagens.

        Returns:
            str: Markdown com imagens embutidas.
        """
        # Regex para encontrar ![alt](path)
        pattern = r'!\[([^\]]*)\]\(([^)]+)\)'

        def replace_image(match):
            alt_text = match.group(1)
            img_path = Path(base_path) / match.group(2)

            if not img_path.exists():
                return f"❌ Imagem não encontrada: {img_path}"

            mime = "image/png" if img_path.suffix.lower() == ".png" else "image/jpeg"

            with open(img_path, "rb") as img_file:
                encoded = base64.b64encode(img_file.read()).decode()

            return f'<img src="data:{mime};base64,{encoded}" alt="{alt_text}" width="760px"/>'

        # Substituir todas as ocorrências no markdown
        return re.sub(pattern, replace_image, markdown_text)

    class validaPath:

        def __init__(self):
            pass

        def validar(self,path:str=None, mode:str=None, method:str=None, content=None):
            encoding = "utf-8"
            if not os.path.exists(path):
                logger.error("Arquivo não encontrado em %s.",path)
                email = Email()
                asyncio.run(email.notificar())
                return False
            else:
                if mode == 'r' and not content:
                    if method == 'full':
                        with open(file=path, mode=mode, encoding=encoding) as f:
                            content = f.read() 
                    elif method == 'lines':
                        with open(file=path, mode=mode, encoding=encoding) as f:
                            content = f.readlines()
                    elif method == 'json':
                        with open(file=path, mode=mode, encoding=encoding) as f:
                            content = json.load(f)
                    elif method == 'q-split':
                        with open(file=path, mode=mode, encoding=encoding) as f:
                            content = f.read().splitlines()
                    return content
                elif mode == 'w' and content:
                    if method in ['full','lines']:
                        with open(file=path, mode=mode, encoding=encoding) as f:
                            f.write(content) 
                    elif method == 'json':
                        with open(file=path, mode=mode, encoding=encoding) as f:
                            json.dump(content, f, indent=4, ensure_ascii=False)
                    return True
                else:
                    return None        