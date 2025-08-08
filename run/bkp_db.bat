@echo off
cd c:/repos/olist-snk
call venv\Scripts\activate
set PYTHONPATH=%cd%
python routines\backup_sqlite.py
call deactivate