cd c:/repos/olist-integrador
call venv\Scripts\activate
set PYTHONPATH=%cd%
python routines\faturamento_legado.py
call deactivate
pause