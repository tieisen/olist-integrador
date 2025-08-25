cd c:/repos/olist-integrador
call venv\Scripts\activate
set PYTHONPATH=%cd%
uvicorn __init__:app --host=192.168.0.166 --port=8180