import os
import uvicorn
import logging
from dotenv import load_dotenv
from src.utils.log import Log

load_dotenv('keys/.env')
logger = logging.getLogger(__name__)
logging.basicConfig(filename=Log().buscar_path(),
                    encoding='utf-8',
                    format=os.getenv('LOGGER_FORMAT'),
                    datefmt='%Y-%m-%d %H:%M:%S',
                    level=logging.INFO)

if __name__ == "__main__":

    host = os.getenv('HOST')
    port = os.getenv('PORT')
    if not any([host,port]):
        raise ValueError("Host/Port config not found")
    
    try:
        port = int(port)
    except:
        raise ValueError(f"Port type invalid. Expected int. Found {type(port)}")

    uvicorn.run(app="app:app",
                host=host,
                port=port,
                reload=True)
