"""Testes de backup e restauração do SQLite."""

import pytest
import os
import sys
import shutil
import tempfile
import sqlite3

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


@pytest.fixture
def temp_db(tmp_path):
    """Cria banco SQLite temporário para testes."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT)")
    conn.execute("INSERT INTO users (username) VALUES ('admin')")
    conn.commit()
    conn.close()
    return db_path


@pytest.fixture
def backup_dir(tmp_path):
    """Cria diretório de backup temporário."""
    backup_path = tmp_path / "backups"
    backup_path.mkdir()
    return backup_path


class TestBackupIntegrity:
    """Testes de verificação de integridade."""

    def test_valid_database_passes_integrity_check(self, temp_db):
        from scripts.backup_sqlite import verify_integrity
        assert verify_integrity(str(temp_db)) is True

    def test_nonexistent_database_fails(self, tmp_path):
        from scripts.backup_sqlite import verify_integrity
        fake_path = tmp_path / "nonexistent.db"
        assert verify_integrity(str(fake_path)) is False

    def test_corrupted_database_fails(self, tmp_path):
        from scripts.backup_sqlite import verify_integrity
        corrupt_db = tmp_path / "corrupt.db"
        corrupt_db.write_bytes(b"this is not a valid sqlite database")
        assert verify_integrity(str(corrupt_db)) is False


class TestBackupCreation:
    """Testes de criação de backup."""

    def test_backup_creates_file(self, temp_db, backup_dir, monkeypatch):
        from scripts.backup_sqlite import backup_sqlite
        monkeypatch.setattr('scripts.backup_sqlite.DB_PATH', str(temp_db))
        monkeypatch.setattr('scripts.backup_sqlite.BACKUP_DIR', str(backup_dir))
        
        result = backup_sqlite()
        assert result is True
        assert len(list(backup_dir.glob("database_*.db"))) == 1

    def test_backup_preserves_data(self, temp_db, backup_dir, monkeypatch):
        from scripts.backup_sqlite import backup_sqlite
        monkeypatch.setattr('scripts.backup_sqlite.DB_PATH', str(temp_db))
        monkeypatch.setattr('scripts.backup_sqlite.BACKUP_DIR', str(backup_dir))
        
        backup_sqlite()
        
        backup_file = list(backup_dir.glob("database_*.db"))[0]
        conn = sqlite3.connect(str(backup_file))
        result = conn.execute("SELECT username FROM users").fetchone()
        conn.close()
        assert result[0] == 'admin'
        
    def test_backup_handles_copy_exception(self, temp_db, backup_dir, monkeypatch):
        from scripts.backup_sqlite import backup_sqlite
        monkeypatch.setattr('scripts.backup_sqlite.DB_PATH', str(temp_db))
        monkeypatch.setattr('scripts.backup_sqlite.BACKUP_DIR', str(backup_dir))
        
        def mock_copy2(*args, **kwargs):
            raise PermissionError("Permission denied")
            
        monkeypatch.setattr(shutil, 'copy2', mock_copy2)
        result = backup_sqlite()
        assert result is False


class TestBackupCleanup:
    """Testes de limpeza de backups antigos."""

    def test_keeps_only_max_backups(self, temp_db, backup_dir, monkeypatch):
        from scripts.backup_sqlite import backup_sqlite, MAX_BACKUPS
        import time
        monkeypatch.setattr('scripts.backup_sqlite.DB_PATH', str(temp_db))
        monkeypatch.setattr('scripts.backup_sqlite.BACKUP_DIR', str(backup_dir))
        
        for i in range(MAX_BACKUPS + 3):
            backup_sqlite()
            time.sleep(1.1)
        
        backups = list(backup_dir.glob("database_*.db"))
        assert len(backups) == MAX_BACKUPS


class TestBackupRestore:
    """Testes de restauração de backup."""

    def test_restore_from_latest_backup(self, temp_db, backup_dir, monkeypatch):
        from scripts.backup_sqlite import backup_sqlite, restore_latest_backup
        monkeypatch.setattr('scripts.backup_sqlite.DB_PATH', str(temp_db))
        monkeypatch.setattr('scripts.backup_sqlite.BACKUP_DIR', str(backup_dir))
        
        backup_sqlite()
        
        conn = sqlite3.connect(str(temp_db))
        conn.execute("INSERT INTO users (username) VALUES ('corrupted')")
        conn.execute("PRAGMA corrupt_db")  
        conn.commit()
        conn.close()
        
        result = restore_latest_backup()
        assert result is True
        
        conn = sqlite3.connect(str(temp_db))
        result = conn.execute("SELECT COUNT(*) FROM users").fetchone()
        conn.close()
        assert result[0] == 1

    def test_restore_fails_without_backups(self, tmp_path, monkeypatch):
        from scripts.backup_sqlite import restore_latest_backup
        empty_dir = tmp_path / "empty_backups"
        empty_dir.mkdir()
        monkeypatch.setattr('scripts.backup_sqlite.BACKUP_DIR', str(empty_dir))
        
        result = restore_latest_backup()
        assert result is False

    def test_restore_fails_if_backup_corrupted(self, temp_db, backup_dir, monkeypatch):
        from scripts.backup_sqlite import restore_latest_backup
        monkeypatch.setattr('scripts.backup_sqlite.DB_PATH', str(temp_db))
        monkeypatch.setattr('scripts.backup_sqlite.BACKUP_DIR', str(backup_dir))
        
        corrupted_backup = backup_dir / "database_20260101_120000.db"
        corrupted_backup.write_bytes(b"invalid sqlite format")
        
        result = restore_latest_backup()
        assert result is False
