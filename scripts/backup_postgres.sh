#!/bin/bash
# Backup automático do banco PostgreSQL do Airflow

set -e

BACKUP_DIR="/opt/airflow/mnt/backups"
MAX_BACKUPS=7
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
BACKUP_FILE="${BACKUP_DIR}/postgres_${TIMESTAMP}.sql.gz"

mkdir -p "$BACKUP_DIR"

echo "[$(date)] Iniciando backup do PostgreSQL..."

# Executa pg_dump via docker exec
docker exec ro-dou-registrale-postgres-1 \
    pg_dump -U airflow -d airflow | gzip > "$BACKUP_FILE"

if [ $? -eq 0 ]; then
    echo "[$(date)] Backup criado: $BACKUP_FILE"
    
    # Remove backups antigos (mantém apenas os últimos MAX_BACKUPS)
    cd "$BACKUP_DIR"
    ls -t postgres_*.sql.gz 2>/dev/null | tail -n +$((MAX_BACKUPS + 1)) | xargs -r rm
    
    echo "[$(date)] Backup concluído com sucesso."
else
    echo "[$(date)] ERRO: Falha ao criar backup."
    exit 1
fi
