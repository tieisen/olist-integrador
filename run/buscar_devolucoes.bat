@echo off
cd c:/repos/olist-integrador
call venv\Scripts\activate
set PYTHONPATH=%cd%
python routines\buscar_devolucoes.py
call deactivate