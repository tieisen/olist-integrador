@echo off
cd c:/repos/olist-snk
call venv\Scripts\activate
set PYTHONPATH=%cd%
python routines\buscar_pedidos_olist.py
call deactivate