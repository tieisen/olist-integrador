import os
import shutil
from datetime import datetime

# Caminho do banco de dados original
DB_PATH = os.path.join("database", "olist.db")

# Diretório de destino do backup
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
