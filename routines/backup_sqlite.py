import os
import shutil
from datetime import datetime

# ROTINA A SER EXECUTADA DIARIAMENTE, ÀS 17H55

DB_PATH = os.path.join("database", "olist.db")
BACKUP_DIR = "database/backups"

# Garante que o diretório de backup existe
os.makedirs(BACKUP_DIR, exist_ok=True)

# Nome do arquivo de backup com data/hora
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
backup_filename = f"olist_backup_{timestamp}.db"
backup_path = os.path.join(BACKUP_DIR, backup_filename)

try:
    shutil.copy2(DB_PATH, backup_path)
    print(f"✅ Backup realizado com sucesso: {backup_path}")
except Exception as e:
    print(f"❌ Erro ao realizar o backup: {e}")
