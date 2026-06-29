from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
import json

db = SQLAlchemy()

class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), default='user')

    def set_password(self, password):
        self.password_hash = generate_password_hash(password, method='pbkdf2:sha256')
        
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class Company(db.Model):
    __tablename__ = 'companies'
    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(255), nullable=False, default="N/A")
    cnpj = db.Column(db.String(20), unique=True, nullable=False)
    cnpj_norm = db.Column(db.String(20), unique=True, nullable=False)
    uf = db.Column(db.String(2), default='N/A')
    cidade = db.Column(db.String(100), default='N/A')
    email = db.Column(db.String(255), default='N/A')
    telefone = db.Column(db.String(50), default='N/A')
    situacao = db.Column(db.String(50), default='Ativa') 
    status = db.Column(db.Boolean, default=True) # Monitorado Sim/Não
    origem = db.Column(db.String(50), default='GestãoClick')

    def to_dict(self):
        return {
            "nome": self.nome,
            "cnpj": self.cnpj,
            "cnpj_norm": self.cnpj_norm,
            "uf": self.uf,
            "cidade": self.cidade,
            "email": self.email,
            "telefone": self.telefone,
            "situacao": self.situacao,
            "status": self.status,
            "origem": self.origem
        }

class SyncHistory(db.Model):
    __tablename__ = 'sync_history'
    id = db.Column(db.Integer, primary_key=True)
    data = db.Column(db.String(50), nullable=False)
    evento = db.Column(db.String(255), nullable=False)
    detalhes = db.Column(db.Text, nullable=True)

    def to_dict(self):
        return {
            "data": self.data,
            "evento": self.evento,
            "detalhes": self.detalhes
        }

class Mention(db.Model):
    __tablename__ = 'mentions'
    id = db.Column(db.String(255), primary_key=True) # pub_id ou fallback_id
    empresa = db.Column(db.String(255))
    cnpj = db.Column(db.String(20))
    cnpj_norm = db.Column(db.String(20))
    secao = db.Column(db.String(50))
    data = db.Column(db.String(20))
    detected_at = db.Column(db.String(100))
    trecho = db.Column(db.Text)
    link = db.Column(db.Text)

    def to_dict(self):
        return {
            "id": self.id,
            "empresa": self.empresa,
            "cnpj": self.cnpj,
            "cnpj_norm": self.cnpj_norm,
            "secao": self.secao,
            "data": self.data,
            "detected_at": self.detected_at,
            "trecho": self.trecho,
            "link": self.link
        }

class Settings(db.Model):
    __tablename__ = 'settings'
    id = db.Column(db.Integer, primary_key=True)
    key = db.Column(db.String(100), unique=True, nullable=False)
    value = db.Column(db.Text, nullable=False)

    def get_value(self):
        try:
            return json.loads(self.value)
        except:
            return self.value

    def set_value(self, val):
        self.value = json.dumps(val)

class EmailTemplate(db.Model):
    __tablename__ = 'email_templates'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), unique=True, nullable=False)
    subject = db.Column(db.String(255), nullable=False)
    body_html = db.Column(db.Text, nullable=False)
