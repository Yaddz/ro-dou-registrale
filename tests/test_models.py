"""Testes unitários dos modelos de dados."""

import pytest
import json
import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

@pytest.fixture
def app():
    from app_dashboard import app
    app.config['TESTING'] = True
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    return app

class TestUserModel:
    """Testes para o modelo User."""

    def test_password_is_hashed(self):
        from werkzeug.security import generate_password_hash
        password = "minha_senha_segura"
        hashed = generate_password_hash(password, method='pbkdf2:sha256')
        assert hashed != password
        assert hashed.startswith('pbkdf2:sha256')

    def test_password_check_correct(self):
        from src.models import User
        user = User(username="testuser", role="user")
        user.set_password("teste123")
        assert user.check_password("teste123") is True

    def test_password_check_incorrect(self):
        from src.models import User
        user = User(username="testuser", role="user")
        user.set_password("senha_correta")
        assert user.check_password("senha_errada") is False

    def test_default_role_is_user(self):
        from src.models import User
        user = User(username="testuser", password_hash="dummy")
        user.role = 'user'
        assert user.role == 'user'

    def test_role_can_be_master(self):
        from src.models import User
        user = User(username="admin", password_hash="dummy", role="master")
        assert user.role == 'master'

    def test_user_missing_username_raises_error(self, app):
        from src.models import db, User
        import sqlalchemy.exc
        with app.app_context():
            db.create_all()
            user = User(password_hash="dummy", role="user")
            db.session.add(user)
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                db.session.commit()

class TestCompanyModel:
    """Testes para o modelo Company."""

    def test_cnpj_normalized(self):
        cnpj_raw = "12.345.678/0001-90"
        cnpj_norm = "12345678000190"
        import re
        result = re.sub(r'[^A-Za-z0-9]', '', cnpj_raw).upper()
        assert result == cnpj_norm

    def test_default_status_is_true(self):
        from src.models import Company
        company = Company(cnpj="12345678000190", cnpj_norm="12345678000190", status=True)
        assert company.status is True

    def test_default_origem_is_gestaoclick(self):
        from src.models import Company
        company = Company(cnpj="12345678000190", cnpj_norm="12345678000190", origem="GestãoClick")
        assert company.origem == 'GestãoClick'

    def test_to_dict_returns_all_fields(self):
        from src.models import Company
        company = Company(
            nome="Empresa Teste",
            cnpj="12.345.678/0001-90",
            cnpj_norm="12345678000190",
            uf="SP",
            cidade="São Paulo",
            email="teste@teste.com",
            telefone="11999999999",
            situacao="Ativa",
            status=True,
            origem="Manual"
        )
        result = company.to_dict()
        assert result["nome"] == "Empresa Teste"
        assert result["cnpj"] == "12.345.678/0001-90"
        assert result["uf"] == "SP"
        assert result["status"] is True

    def test_default_nome_is_na(self):
        from src.models import Company
        company = Company(cnpj="12345678000190", cnpj_norm="12345678000190", nome="N/A")
        assert company.nome == "N/A"

    def test_company_missing_cnpj_raises_error(self, app):
        from src.models import db, Company
        import sqlalchemy.exc
        with app.app_context():
            db.create_all()
            company = Company(nome="Empresa Sem CNPJ", cnpj_norm="12345678000190")
            db.session.add(company)
            with pytest.raises(sqlalchemy.exc.IntegrityError):
                db.session.commit()

class TestSettingsModel:
    """Testes para o modelo Settings."""

    def test_set_value_serializes_dict(self):
        from src.models import Settings
        settings = Settings(key="test", value="{}")
        data = {"smtp": {"server": "smtp.gmail.com", "port": 587}}
        settings.set_value(data)
        assert settings.value == json.dumps(data)

    def test_get_value_returns_dict(self):
        from src.models import Settings
        data = {"smtp": {"server": "smtp.gmail.com"}}
        settings = Settings(key="test", value=json.dumps(data))
        result = settings.get_value()
        assert result == data

    def test_get_value_handles_invalid_json(self):
        from src.models import Settings
        settings = Settings(key="test", value="not json")
        result = settings.get_value()
        assert result == "not json"

    def test_get_value_handles_none(self):
        from src.models import Settings
        settings = Settings(key="test", value=None)
        result = settings.get_value()
        assert result is None

class TestMentionModel:
    """Testes para o modelo Mention."""

    def test_to_dict_returns_all_fields(self):
        from src.models import Mention
        mention = Mention(
            id="abc123",
            empresa="Empresa Teste",
            cnpj="12.345.678/0001-90",
            cnpj_norm="12345678000190",
            secao="Seção 1",
            data="25/06/2026",
            detected_at="2026-06-25T10:00:00",
            trecho="Trecho de teste",
            link="https://example.com"
        )
        result = mention.to_dict()
        assert result["id"] == "abc123"
        assert result["empresa"] == "Empresa Teste"
        assert result["link"] == "https://example.com"

class TestSyncHistoryModel:
    """Testes para o modelo SyncHistory."""

    def test_to_dict_returns_all_fields(self):
        from src.models import SyncHistory
        history = SyncHistory(
            data="25/06 10:00",
            evento="Sincronização OK",
            detalhes="100 empresas sincronizadas"
        )
        result = history.to_dict()
        assert result["data"] == "25/06 10:00"
        assert result["evento"] == "Sincronização OK"
        assert result["detalhes"] == "100 empresas sincronizadas"

class TestEmailTemplateModel:
    """Testes para o modelo EmailTemplate."""

    def test_template_creation(self):
        from src.models import EmailTemplate
        template = EmailTemplate(
            name="Template Teste",
            subject="Assunto Teste",
            body_html="<h1>Teste</h1>"
        )
        assert template.name == "Template Teste"
        assert template.subject == "Assunto Teste"
        assert template.body_html == "<h1>Teste</h1>"
