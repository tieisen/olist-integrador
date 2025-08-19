cd c:/repos/olist-integrador
call venv\Scripts\activate
set PYTHONPATH=%cd%
python routines\conferir.py
call deactivate
pause