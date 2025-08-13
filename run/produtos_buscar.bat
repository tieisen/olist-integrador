@echo off
cd c:/repos/olist-integrador
call venv\Scripts\activate
set PYTHONPATH=%cd%
python routines\produtos_buscar.py
call deactivate