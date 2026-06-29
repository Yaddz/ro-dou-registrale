#!/usr/bin/env python3
"""Backup automático do banco SQLite do dashboard."""

import os
import shutil
import sqlite3
from datetime import datetime

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
BACKUP_DIR = os.path.join(DATA_DIR, "backups")
DB_PATH = os.path.join(DATA_DIR, "database.db")
MAX_BACKUPS = 7


def verify_integrity(db_path):
    """Verifica integridade do banco SQLite."""
    if not os.path.exists(db_path):
        return False
    try:
        conn = sqlite3.connect(db_path)
        result = conn.execute("PRAGMA integrity_check").fetchone()
        conn.close()
        return result[0] == "ok"
    except Exception:
        return False


def backup_sqlite():
    """Cria backup do banco SQLite e mantém apenas os últimos N backups."""
    if not os.path.exists(DB_PATH):
        print(f"Banco não encontrado: {DB_PATH}")
        return False

    if not verify_integrity(DB_PATH):
        print("Banco corrompido! Tentando restaurar do último backup...")
        return restore_latest_backup()

    os.makedirs(BACKUP_DIR, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_name = f"database_{timestamp}.db"
    backup_path = os.path.join(BACKUP_DIR, backup_name)

    try:
        shutil.copy2(DB_PATH, backup_path)
        print(f"Backup criado: {backup_name}")
    except Exception as e:
        print(f"Erro ao criar backup: {e}")
        return False

    cleanup_old_backups()
    return True


def restore_latest_backup():
    """Restaura o banco do backup mais recente."""
    if not os.path.exists(BACKUP_DIR):
        print("Nenhum backup encontrado para restauração.")
        return False

    backups = sorted(
        [f for f in os.listdir(BACKUP_DIR) if f.endswith(".db")],
        reverse=True
    )

    for backup_name in backups:
        backup_path = os.path.join(BACKUP_DIR, backup_name)
        if verify_integrity(backup_path):
            shutil.copy2(backup_path, DB_PATH)
            print(f"Banco restaurado do backup: {backup_name}")
            return True

    print("Nenhum backup íntegro encontrado.")
    return False


def cleanup_old_backups():
    """Remove backups antigos, mantendo apenas os últimos MAX_BACKUPS."""
    if not os.path.exists(BACKUP_DIR):
        return

    backups = sorted(
        [f for f in os.listdir(BACKUP_DIR) if f.endswith(".db")],
        reverse=True
    )

    for old_backup in backups[MAX_BACKUPS:]:
        os.remove(os.path.join(BACKUP_DIR, old_backup))
        print(f"Backup antigo removido: {old_backup}")


if __name__ == "__main__":
    backup_sqlite()
