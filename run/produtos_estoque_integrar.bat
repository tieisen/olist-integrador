cd c:/repos/olist-integrador
call venv\Scripts\activate
set PYTHONPATH=%cd%
python routines\produtos_estoque_integrar.py
call deactivate