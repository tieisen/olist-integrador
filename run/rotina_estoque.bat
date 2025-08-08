@echo off
cd c:/repos/olist-snk
call venv\Scripts\activate
set PYTHONPATH=%cd%
python routines\integrar_estoque.py
call deactivate