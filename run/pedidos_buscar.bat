@echo off
cd c:/repos/olist-integrador
call venv\Scripts\activate
set PYTHONPATH=%cd%
python routines\pedidos_buscar.py
call deactivate