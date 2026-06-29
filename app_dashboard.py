import os
import glob
import yaml
import json
import csv
import sys
import re
import ast
import logging
import threading
import time
from datetime import datetime, timedelta, timezone
from flask import Flask, render_template, jsonify, request, session, redirect, url_for, send_file, after_this_request
from dotenv import load_dotenv, set_key
from functools import wraps

# Configuração de Logs
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Adiciona o diretório src ao path
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(BASE_DIR, 'src'))

try:
    from utils.sync_cnpj import executar_sincronizacao
except ImportError:
    executar_sincronizacao = None

from flask_session import Session
from src.models import db, User, Company, Mention, SyncHistory, Settings

# Carrega variáveis de ambiente
load_dotenv()

app = Flask(__name__)
app.secret_key = os.getenv("SECRET_KEY", "rodou-secret-key-123")

DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

# Caminhos de persistência ABSOLUTOS
USERS_FILE = os.path.join(DATA_DIR, "users.json")
SETTINGS_FILE = os.path.join(DATA_DIR, "settings.json")
METADATA_FILE = os.path.join(DATA_DIR, "monitored_companies.json")
HISTORY_FILE = os.path.join(DATA_DIR, "sync_history.json")
MENTS_CACHE_FILE = os.path.join(DATA_DIR, "detected_mentions_cache.json")
LOGS_DIR = os.path.join(BASE_DIR, "mnt", "airflow-logs")

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///' + os.path.join(DATA_DIR, 'database.db')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)

_mentions_cache = None
_mentions_cache_time = 0
_mentions_deleted_at = 0

def init_default_data():
    """Inicializa dados padrão no banco (admin user e template de email)."""
    with app.app_context():
        db.create_all()
        from src.models import User, EmailTemplate
        if not User.query.filter_by(username='admin').first():
            admin = User(username='admin', role='master')
            admin.set_password('admin')
            db.session.add(admin)
            db.session.commit()
        if not EmailTemplate.query.filter_by(name='Padrão Registrale').first():
            create_default_email_template()

# Configuração de Sessão em SERVIDOR (FileSystem)
app.config.update(
    SESSION_TYPE='filesystem',
    SESSION_FILE_DIR=os.path.join(os.path.dirname(os.path.abspath(__file__)), 'flask_sessions'),
    SESSION_PERMANENT=True,
    SESSION_REFRESH_EACH_REQUEST=False,
    SESSION_USE_SIGNER=True,
    SESSION_COOKIE_NAME='registrale_secure_sid',
    SESSION_COOKIE_SAMESITE='Strict',
    SESSION_COOKIE_HTTPONLY=True,
    PERMANENT_SESSION_LIFETIME=timedelta(minutes=30)
)
Session(app)

@app.after_request
def add_header(response):
    """Previne o cache do navegador para evitar que páginas logadas apareçam após logout ou em novas janelas."""
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response

def load_json(file_path, default=[]):
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Erro ao carregar {file_path}: {e}")
            return default
    return default

def save_json(file_path, data):
    try:
        # Garante que a pasta exista
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        return True
    except Exception as e:
        logger.error(f"Erro fatal ao salvar {file_path}: {e}")
        return False

def add_history_event(evento, detalhes):
    try:
        from src.models import db, SyncHistory
        with app.app_context():
            new_event = SyncHistory(
                data=datetime.now(timezone(timedelta(hours=-3))).strftime('%d/%m %H:%M'),
                evento=evento,
                detalhes=detalhes
            )
            db.session.add(new_event)
            # Mantém os últimos 50
            if SyncHistory.query.count() >= 50:
                oldest = SyncHistory.query.order_by(SyncHistory.id.asc()).first()
                if oldest:
                    db.session.delete(oldest)
            db.session.commit()
    except Exception as e:
        logger.error(f"Erro ao adicionar histórico: {e}")

def normalize_cnpj(cnpj):
    if not cnpj: return ""
    return re.sub(r'[^A-Za-z0-9]', '', str(cnpj)).upper()

def get_monitored_cnpjs():
    dag_confs_path = os.path.join(BASE_DIR, "dag_confs")
    yaml_files = glob.glob(os.path.join(dag_confs_path, "Pesquisa_cnpj_sync.yaml"))
    if not yaml_files:
        yaml_files = glob.glob(os.path.join(dag_confs_path, "Pesquisa_cnpj_part_*.yaml"))
    active_cnpjs = set()
    for f_path in yaml_files:
        try:
            with open(f_path, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                search = config.get('dag', {}).get('search', [])
                if isinstance(search, list):
                    for s in search:
                        terms = s.get('terms', [])
                        if isinstance(terms, list):
                            for t in terms: active_cnpjs.add(normalize_cnpj(t))
                else:
                    terms = search.get('terms', [])
                    if isinstance(terms, list):
                        for t in terms: active_cnpjs.add(normalize_cnpj(t))
        except: continue
    return active_cnpjs

def get_companies_data():
    from src.models import Company
    active_cnpjs = get_monitored_cnpjs()
    with app.app_context():
        all_metadata = Company.query.all()
        empresas = []
        for meta in all_metadata:
            cnpj_bruto = meta.cnpj
            cnpj_norm = normalize_cnpj(cnpj_bruto)
            is_active = cnpj_norm in active_cnpjs
            empresas.append({
                "id": meta.id,
                "nome": meta.nome or "N/A",
                "cnpj": cnpj_bruto,
                "uf": meta.uf or "N/A",
                "cidade": meta.cidade or "N/A",
                "email": meta.email or "N/A",
                "telefone": meta.telefone or "N/A",
                "situacao": meta.situacao or "Ativa",
                "status": is_active,
                "origem": meta.origem
            })
        return sorted(empresas, key=lambda x: x['nome'])

def sync_json_to_db():
    """Importa empresas do monitored_companies.json para o SQLite."""
    from src.models import Company
    if not os.path.exists(METADATA_FILE):
        return
    try:
        with open(METADATA_FILE, 'r', encoding='utf-8') as f:
            empresas = json.load(f)
    except Exception as e:
        logger.error(f"Erro ao ler JSON de empresas: {e}")
        return
    
    with app.app_context():
        count_new = 0
        count_updated = 0
        for emp in empresas:
            cnpj = emp.get('cnpj', '')
            if not cnpj:
                continue
            cnpj_norm = normalize_cnpj(cnpj)
            existing = Company.query.filter_by(cnpj_norm=cnpj_norm).first()
            if not existing:
                existing = Company(
                    cnpj=cnpj,
                    cnpj_norm=cnpj_norm,
                    nome=emp.get('razao_social', emp.get('nome', 'N/A')),
                    uf=emp.get('uf', ''),
                    cidade=emp.get('cidade', ''),
                    email=emp.get('email', ''),
                    telefone=emp.get('telefone', ''),
                    situacao=emp.get('situacao', 'Ativa'),
                    origem='GestaoClick'
                )
                db.session.add(existing)
                count_new += 1
            else:
                if existing.origem == 'Manual':
                    continue
                existing.nome = emp.get('razao_social', emp.get('nome', existing.nome))
                existing.uf = emp.get('uf', existing.uf)
                existing.cidade = emp.get('cidade', existing.cidade)
                existing.email = emp.get('email', existing.email)
                existing.telefone = emp.get('telefone', existing.telefone)
                existing.situacao = emp.get('situacao', existing.situacao)
                count_updated += 1
        db.session.commit()
        logger.info(f"Sync JSON->DB: {count_new} novas, {count_updated} atualizadas")

def get_real_mentions():
    """Varre os logs do Airflow para extrair as menções reais encontradas, com cache otimizado."""
    global _mentions_cache, _mentions_cache_time, _mentions_deleted_at
    
    now = time.time()
    if _mentions_cache and (now - _mentions_cache_time) < 300:
        return _mentions_cache
    
    if now - _mentions_deleted_at < 60:
        return []
    
    if not os.path.exists(LOGS_DIR): return []

    log_files = glob.glob(os.path.join(LOGS_DIR, "dag_id=*", "run_id=*", "task_id=exec_searchs.exec_search_*", "attempt=*.log"), recursive=True)
    if not log_files: return []

    try:
        latest_log_mtime = max(os.path.getmtime(f) for f in log_files)
    except:
        latest_log_mtime = 0

    from src.models import db, Settings, Company, Mention
    
    with app.app_context():
        cache_data = {"last_parsed_at": 0, "mentions": []}
        cache_setting = Settings.query.filter_by(key='mentions_cache_meta').first()
        if cache_setting:
            cache_data["last_parsed_at"] = cache_setting.get_value().get("last_parsed_at", 0)
            
        if cache_data["last_parsed_at"] >= latest_log_mtime:
            cached_mentions = Mention.query.all()
            if cached_mentions:
                result = [m.to_dict() for m in cached_mentions]
                _mentions_cache = result
                _mentions_cache_time = now
                return result

        metadata = Company.query.all()
        cnpj_map = {m.cnpj_norm: m.nome for m in metadata}

    mentions_dict = {}
    
    for log_path in log_files:
        try:
            with open(log_path, 'rb') as f:
                size = os.path.getsize(log_path)
                if size > 100000: # 100KB
                    f.seek(size - 100000)
                content = f.read().decode('utf-8', errors='ignore')
                
                # Regex otimizada sem backtracking excessivo
                matches = re.finditer(r"\[(.*?)\].*?Done\. Returned value was: (\{.*?\})$", content, re.MULTILINE)
                
                for match in matches:
                    log_time = match.group(1)
                    dict_str = match.group(2).strip()
                    
                    try:
                        result_dict = ast.literal_eval(dict_str)
                        results = result_dict.get('result', {}).get('single_group', {})
                        if not results: continue

                        for cnpj_log, content_group in results.items():
                            cnpj_norm = normalize_cnpj(cnpj_log)
                            # content_group é um dicionário { 'Nome do Departamento': [ publicações ] }
                            for dept_name, depts in content_group.items():
                                for pub in depts:
                                    import hashlib
                                    raw_abstract = pub.get('abstract', '')
                                    fallback_id = hashlib.md5(f"{cnpj_norm}_{pub.get('date', '')}_{raw_abstract}".encode('utf-8', errors='ignore')).hexdigest()
                                    pub_id = pub.get('id')
                                    if not pub_id:
                                        pub_id = fallback_id
                                    unique_key = f"{cnpj_norm}_{pub_id}"
                                    
                                    # Apenas insere se for a detecção mais recente para aquele CNPJ+ID ou se não existir
                                    if unique_key not in mentions_dict or log_time > mentions_dict[unique_key]['detected_at']:
                                        raw_trecho = raw_abstract.replace("<span class='highlight' style='background:#FFA;'>", "").replace("</span>", "").replace("<span class='highlight'>", "")

                                        # Formata o trecho para o modal usando a mesma lógica do email
                                        try:
                                            from notification.email_sender import EmailSender
                                            formatted_trecho = EmailSender.format_abstract(raw_trecho, cnpj_norm)
                                        except:
                                            formatted_trecho = raw_trecho

                                        empresa_nome = cnpj_map.get(cnpj_norm)
                                        if not empresa_nome:
                                            comp = Company.query.filter_by(cnpj_norm=cnpj_norm).first()
                                            if not comp or comp.nome == 'N/A':
                                                comp = Company.query.filter_by(cnpj=cnpj_log).first()
                                            empresa_nome = comp.nome if comp and comp.nome != 'N/A' else cnpj_log

                                        mentions_dict[unique_key] = {
                                            "id": pub_id,
                                            "empresa": empresa_nome,
                                            "cnpj": cnpj_log,
                                            "cnpj_norm": cnpj_norm,
                                            "secao": pub.get('section', 'DOU'),
                                            "data": pub.get('date', 'N/A'),
                                            "detected_at": log_time,
                                            "trecho": formatted_trecho,
                                            "link": pub.get('href', '#')
                                        }
                    except: continue
        except: continue

    mentions = list(mentions_dict.values())

    # Ordenação Robusta
    try:
        mentions.sort(key=lambda x: (
            datetime.strptime(x['data'], '%d/%m/%Y') if x['data'] != 'N/A' else datetime.min,
            x['detected_at']
        ), reverse=True)
    except: pass

    # Salva no cache DB
    with app.app_context():
        try:
            Mention.query.delete()
            import uuid
            for m in mentions:
                m['id'] = str(uuid.uuid4())
                new_m = Mention(**m)
                db.session.add(new_m)
            
            cache_setting = Settings.query.filter_by(key='mentions_cache_meta').first()
            if not cache_setting:
                cache_setting = Settings(key='mentions_cache_meta')
                db.session.add(cache_setting)
            cache_setting.set_value({"last_parsed_at": datetime.now(timezone(timedelta(hours=-3))).timestamp()})
            db.session.commit()
        except Exception as e:
            logger.error(f"Erro ao salvar cache de menções no BD: {e}")
            db.session.rollback()

    _mentions_cache = mentions
    _mentions_cache_time = time.time()
    return mentions

def login_required(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user' not in session:
            return redirect(url_for('login'))
        return f(*args, **kwargs)
    return decorated_function

@app.route('/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        from src.models import User
        username = request.form.get('username')
        password = request.form.get('password')
        user = User.query.filter_by(username=username).first()
        if user and user.check_password(password):
            session.permanent = True
            session['user'] = {'username': user.username, 'role': user.role}
            session['expires_at'] = (datetime.now(timezone(timedelta(hours=-3))) + app.permanent_session_lifetime).timestamp()
            return redirect(url_for('index'))
        return render_template('login.html', error="Usuário ou senha inválidos")
    return render_template('login.html')

@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('login'))

@app.route('/api/extend_session', methods=['POST'])
@login_required
def extend_session():
    session.permanent = True
    session['expires_at'] = (datetime.now(timezone(timedelta(hours=-3))) + timedelta(minutes=60)).timestamp()
    return jsonify({"status": "ok", "time_left": 3600})

@app.route('/api/mentions')
@login_required
def api_mentions():
    return jsonify(get_real_mentions())

def get_last_search_time():
    if not os.path.exists(LOGS_DIR): return "N/A"
    log_files = glob.glob(os.path.join(LOGS_DIR, "dag_id=pesquisa_cnpj*", "run_id=*", "task_id=exec_searchs.exec_search_*", "attempt=*.log"), recursive=True)
    if not log_files: return "N/A"
    try:
        latest_log = max(log_files, key=os.path.getmtime)
        return datetime.fromtimestamp(os.path.getmtime(latest_log), timezone(timedelta(hours=-3))).strftime('%d/%m %H:%M')
    except:
        return "N/A"

def get_next_search_time():
    now = datetime.now(timezone(timedelta(hours=-3)))
    schedule_hour, schedule_minute = 8, 0
    try:
        yaml_files = glob.glob(os.path.join(BASE_DIR, "dag_confs", "Pesquisa_cnpj_sync.yaml"))
        if yaml_files:
            with open(yaml_files[0], 'r', encoding='utf-8') as f:
                d = yaml.safe_load(f)
                sched = d.get('dag', {}).get('schedule', '0 8 * * *')
                parts = sched.split()
                if len(parts) >= 2 and parts[0].isdigit() and parts[1].isdigit():
                    schedule_minute = int(parts[0])
                    schedule_hour = int(parts[1])
    except: pass
    
    next_run = now.replace(hour=schedule_hour, minute=schedule_minute, second=0, microsecond=0)
    if now >= next_run:
        next_run += timedelta(days=1)
    
    # Se cair no fim de semana, pula para segunda (MON-FRI)
    while next_run.weekday() > 4:
        next_run += timedelta(days=1)
        
    return next_run.strftime('%d/%m %H:%M')

@app.route('/')
@login_required
def index():
    # Verifica expiração absoluta
    expires_at = session.get('expires_at')
    if expires_at and datetime.now(timezone(timedelta(hours=-3))).timestamp() > expires_at:
        session.clear()
        return redirect(url_for('login'))

    is_master = session['user']['role'] == 'master'
    
    from src.models import Settings, User, SyncHistory, Company
    
    settings = {"smtp":{}, "api_keys":{}, "google_sheets":{}}
    users_list = []
    history = []
    
    if is_master:
        settings_record = Settings.query.filter_by(key='global_settings').first()
        if settings_record:
            settings = settings_record.get_value()
        users_list = [{"username": u.username, "role": u.role} for u in User.query.all()]
        
    history = [h.to_dict() for h in SyncHistory.query.order_by(SyncHistory.id.desc()).limit(50).all()]

    all_mentions = get_real_mentions()
    
    dag_confs_path = os.path.join(BASE_DIR, "dag_confs")
    yaml_files = glob.glob(os.path.join(dag_confs_path, "Pesquisa_cnpj_sync.yaml"))
    if not yaml_files: yaml_files = glob.glob(os.path.join(dag_confs_path, "Pesquisa_cnpj_part_*.yaml"))
    last_sync = "N/A"
    if yaml_files:
        mtime = os.path.getmtime(yaml_files[0])
        last_sync = datetime.fromtimestamp(mtime, timezone(timedelta(hours=-3))).strftime('%d/%m %H:%M')

    last_search = get_last_search_time()
    next_search = get_next_search_time()
    
    # Calcula tempo restante para o frontend
    time_left = 0
    if expires_at:
        time_left = max(0, int(expires_at - datetime.now(timezone(timedelta(hours=-3))).timestamp()))

    # Data inicial para o Alpine
    init_data = {
        "mencoes_recentes": all_mentions[:20],
        "kpis": {
            "cnpjs": Company.query.count(),
            "ativos": len(get_monitored_cnpjs()),
            "mencoes_hoje": len([m for m in all_mentions if m['data'] == datetime.now(timezone(timedelta(hours=-3))).strftime('%d/%m/%Y')]),
            "este_mes": len([m for m in all_mentions if datetime.now(timezone(timedelta(hours=-3))).strftime('/%m/%Y') in m['data']])
        }
    }

    return render_template('index.html', 
                           user=session['user'],
                           init_data=init_data,
                           mencoes=all_mentions[:20],
                           last_sync=last_sync,
                           last_search=last_search,
                           next_search=next_search,
                           time_left=time_left,
                           settings=settings,
                           users=users_list,
                           historico=history if history else [{"data": last_sync, "evento": "Status", "detalhes": "Aguardando sincronização."}])

@app.route('/api/companies', methods=['GET', 'POST'])
@login_required
def api_companies():
    if request.method == 'GET':
        return jsonify(get_companies_data())
    elif request.method == 'POST':
        from src.models import db, Company
        data = request.json
        if not data.get('cnpj'):
            return jsonify({"status": "error", "message": "CNPJ não informado."}), 400
        try:
            cnpj_norm = normalize_cnpj(data.get('cnpj'))
            if len(cnpj_norm) != 14:
                return jsonify({"status": "error", "message": "CNPJ inválido."}), 400
            existing = Company.query.filter_by(cnpj_norm=cnpj_norm).first()
            if existing:
                return jsonify({"status": "error", "message": "CNPJ já cadastrado."}), 400
            
            new_comp = Company(
                nome=data.get('nome', 'N/A'),
                cnpj=data.get('cnpj'),
                cnpj_norm=cnpj_norm,
                uf=data.get('uf', 'N/A'),
                cidade=data.get('cidade', 'N/A'),
                email=data.get('email', 'N/A'),
                telefone=data.get('telefone', 'N/A'),
                situacao=data.get('situacao', 'Ativa'),
                status=data.get('status', True),
                origem=data.get('origem', 'Manual')
            )
            db.session.add(new_comp)
            db.session.commit()
            return jsonify({"status": "success", "message": "Empresa adicionada!"})
        except Exception as e:
            db.session.rollback()
            return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/companies/<int:cnpj_id>', methods=['PUT'])
@login_required
def update_company(cnpj_id):
    if session['user']['role'] != 'master':
        return jsonify({"status": "error", "message": "Acesso negado."}), 403
    from src.models import db, Company
    company = Company.query.get(cnpj_id)
    if not company:
        return jsonify({"status": "error", "message": "Empresa não encontrada."}), 404
    data = request.json
    try:
        company.nome = data.get('nome', company.nome)
        company.email = data.get('email', company.email)
        company.telefone = data.get('telefone', company.telefone)
        company.situacao = data.get('situacao', company.situacao)
        company.status = data.get('status', company.status)
        company.origem = 'Manual'
        db.session.commit()
        return jsonify({"status": "success", "message": "Empresa atualizada!"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/company_history/<path:cnpj>')
@login_required
def company_history(cnpj):
    all_mentions = get_real_mentions()
    cnpj_norm = normalize_cnpj(cnpj)
    history = [m for m in all_mentions if m['cnpj_norm'] == cnpj_norm]
    return jsonify(history)

def get_routines():
    dag_confs_path = os.path.join(BASE_DIR, "dag_confs")
    yaml_files = glob.glob(os.path.join(dag_confs_path, "*.yaml"))
    
    routines = []
    sync_parts = []
    sync_base_data = None
    
    for f_path in yaml_files:
        name = os.path.basename(f_path)
        
        # Identifica se é uma parte da rotina de sincronização
        if "pesquisa_cnpj" in name.lower():
            if "_part_" in name.lower() or "_sync" in name.lower():
                sync_parts.append(f_path)
                continue
            elif name.lower() == "pesquisa_cnpj.yaml":
                # É o arquivo base, vamos processar para pegar as configurações padrão
                try:
                    with open(f_path, 'r', encoding='utf-8') as f:
                        data = yaml.safe_load(f)
                        if data and 'dag' in data:
                            dag = data.get('dag', {})
                            search = dag.get('search', {})
                            if isinstance(search, list): search = search[0]
                            report = dag.get('report', {})
                            sync_base_data = {
                                "id": dag.get('id', name),
                                "file": name,
                                "description": dag.get('description', ''),
                                "schedule": dag.get('schedule', '0 5 * * *'),
                                "terms": search.get('terms', []),
                                "organs": search.get('department', []),
                                "sections": search.get('dou_sections', ["SECAO_1", "SECAO_2", "SECAO_3"]),
                                "emails": report.get('emails', []),
                                "subject": report.get('subject', ''),
                                "type": "sync",
                                "is_exact_search": search.get('is_exact_search', True),
                                "terms_ignore": search.get('terms_ignore', [])
                            }
                except: pass
                continue

        # Processa outras rotinas customizadas
        try:
            with open(f_path, 'r', encoding='utf-8') as f:
                data = yaml.safe_load(f)
                if not data or 'dag' not in data: continue
                dag = data.get('dag', {})
                search = dag.get('search', {})
                if isinstance(search, list): search = search[0]
                report = dag.get('report', {})
                
                routines.append({
                    "id": dag.get('id', name),
                    "file": name,
                    "description": dag.get('description', ''),
                    "schedule": dag.get('schedule', '0 5 * * *'),
                    "terms": search.get('terms', []),
                    "organs": search.get('department', []),
                    "sections": search.get('dou_sections', ["SECAO_1", "SECAO_2", "SECAO_3"]),
                    "emails": report.get('emails', []),
                    "subject": report.get('subject', ''),
                    "type": "custom",
                    "is_exact_search": search.get('is_exact_search', True),
                    "terms_ignore": search.get('terms_ignore', [])
                })
        except Exception as e: 
            logger.error(f"Erro ao ler rotina {name}: {e}")
            continue
    
    # Consolida a Rotina de Sincronização
    total_cnpjs = 0
    # Soma termos de todas as partes encontradas
    for sp in sync_parts:
        try:
            with open(sp, 'r', encoding='utf-8') as f:
                d = yaml.safe_load(f)
                s = d.get('dag', {}).get('search', [])
                if isinstance(s, list):
                    for block in s:
                        total_cnpjs += len(block.get('terms', []))
                else:
                    total_cnpjs += len(s.get('terms', []))
        except: continue
    
    # Soma termos do arquivo base se ele tiver termos diretos (raro mas possível)
    if sync_base_data and isinstance(sync_base_data.get('terms'), list):
        if "_part_" not in sync_base_data['file'] and "_sync" not in sync_base_data['file']: # Evita duplicidade se base for confundida
             total_cnpjs += len(sync_base_data['terms'])

    # Monta o registro único de Sincronização
    sync_routine = {
        "id": "Sincronização Automática (GestãoClick)",
        "file": "Pesquisa_cnpj.yaml",
        "description": f"Sincronização automática via API. Monitorando {total_cnpjs} CNPJs.",
        "schedule": sync_base_data.get('schedule', '0 5 * * *') if sync_base_data else "0 5 * * *",
        "terms": [f"{total_cnpjs} CNPJs monitorados"],
        "organs": sync_base_data.get('organs', ["Diversos"]) if sync_base_data else ["Diversos"],
        "department": sync_base_data.get('department', ["Diversos"]) if sync_base_data else ["Diversos"],
        "sections": sync_base_data.get('sections', ["SECAO_1", "SECAO_2", "SECAO_3"]) if sync_base_data else ["SECAO_1", "SECAO_2", "SECAO_3"],
        "emails": sync_base_data.get('emails', []) if sync_base_data else [],
        "subject": sync_base_data.get('subject', '') if sync_base_data else '',
        "type": "sync",
        "is_exact_search": sync_base_data.get('is_exact_search', True) if sync_base_data else True,
        "terms_ignore": sync_base_data.get('terms_ignore', []) if sync_base_data else []
    }
    
    routines.insert(0, sync_routine)
    return routines

@app.route('/api/routines', methods=['GET', 'POST'])
@login_required
def manage_routines():
    if request.method == 'GET':
        return jsonify(get_routines())
    
    if session['user']['role'] != 'master': return jsonify({"status": "error"}), 403
    data = request.json
    
    name = data.get('name', '').strip()
    if not name:
        return jsonify({"status": "error", "message": "Nome da rotina é obrigatório."}), 400
    
    terms = data.get('terms', [])
    if not terms or len(terms) == 0:
        return jsonify({"status": "error", "message": "Adicione pelo menos um termo de busca."}), 400
    
    sections = data.get('sections', [])
    if not sections or len(sections) == 0:
        return jsonify({"status": "error", "message": "Selecione pelo menos uma seção do DOU."}), 400
    
    emails = data.get('emails', [])
    if not emails or len(emails) == 0:
        return jsonify({"status": "error", "message": "Adicione pelo menos um e-mail de destino."}), 400
    
    # Se for edição de arquivo existente ou criação de novo
    filename = data.get('file')
    if not filename:
        new_id = re.sub(r'\W+', '_', data['name'].lower())
        filename = f"{new_id}.yaml"
        
    file_path = os.path.join(BASE_DIR, "dag_confs", filename)
    
    # Se o arquivo já existe, carrega para manter campos não editados
    existing_data = {}
    if os.path.exists(file_path):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                existing_data = yaml.safe_load(f)
        except: pass

    # Monta a estrutura preservando campos do Airflow
    new_dag = existing_data or {"dag": {}}
    dag = new_dag["dag"]
    
    dag["id"] = dag.get("id") or re.sub(r'\.[^.]*$', '', filename)
    dag["description"] = data.get('description', dag.get('description', ''))
    dag["schedule"] = data.get('schedule', dag.get('schedule', '0 5 * * *'))
    dag["tags"] = dag.get("tags", ["custom"])
    dag["owner"] = dag.get("owner", ["admin"])
    
    # Search config
    search = dag.get("search", {})
    if isinstance(search, list): 
        search = search[0] if len(search) > 0 else {}
    
    search["header"] = data.get('name', search.get('header', 'Busca'))
    search["department"] = data.get('organs', search.get('department', []))
    search["organs"] = data.get('organs', search.get('organs', []))
    
    # Se for a rotina de sync, não sobrescreve os termos (pois eles vêm do GestãoClick)
    if filename != "Pesquisa_cnpj.yaml":
        search["terms"] = data.get('terms', search.get('terms', []))
    
    search["dou_sections"] = data.get('sections', search.get('dou_sections', ["SECAO_1", "SECAO_2", "SECAO_3"]))
    search["field"] = search.get("field", "TUDO")
    search["is_exact_search"] = data.get('is_exact_search', True)
    search["terms_ignore"] = data.get('terms_ignore', [])
    search["full_text"] = search.get("full_text", True)
    search["date"] = search.get("date", "DIA")
    
    # O Pydantic espera uma LISTA de SearchConfigs
    dag["search"] = [search]
    
    # Report config
    report = dag.get("report", {})
    report["title"] = data.get('name', report.get('title', 'Alerta'))
    report["emails"] = data.get('emails', report.get('emails', []))
    report["subject"] = data.get('subject', report.get('subject', ''))
    
    dag["report"] = report
    
    with open(file_path, 'w', encoding='utf-8') as f:
        yaml.safe_dump(new_dag, f, allow_unicode=True, sort_keys=False)
    
    return jsonify({"status": "success", "message": "Rotina salva com sucesso!"})

@app.route('/api/routines/<path:file>', methods=['DELETE'])
@login_required
def delete_routine(file):
    if session['user']['role'] != 'master': return jsonify({"status": "error", "message": "Acesso negado."}), 403
    
    if file == "Pesquisa_cnpj.yaml" or "_part_" in file or "_sync" in file or "gestaoclick" in file.lower():
        return jsonify({"status": "error", "message": "Não é possível excluir rotinas de sistema (Sync / GestãoClick)."}), 400
        
    file_path = os.path.join(BASE_DIR, "dag_confs", file)
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
            add_history_event("Rotina Excluída", f"Rotina {file} removida do sistema.")
            return jsonify({"status": "success", "message": "Rotina excluída com sucesso!"})
        except Exception as e:
            return jsonify({"status": "error", "message": f"Erro ao excluir o arquivo: {str(e)}"}), 500
    
    return jsonify({"status": "error", "message": "Arquivo não encontrado."}), 404

@app.route('/api/sync', methods=['POST'])
@login_required
def manual_sync_route():
    return trigger_sync_logic()

def run_sync_in_background():
    try:
        logger.info("Iniciando sincronização de CNPJs em segundo plano...")
        executar_sincronizacao()
        sync_json_to_db()
        add_history_event("Sincronização OK", "Sincronização realizada com sucesso.")
        logger.info("Sincronização em segundo plano concluída com sucesso.")
    except Exception as e:
        add_history_event("Erro Sync", str(e))
        logger.error(f"Erro na sincronização em segundo plano: {e}")

def trigger_sync_logic():
    if not executar_sincronizacao:
        return jsonify({"status": "error", "message": "Função de sincronização não encontrada."}), 500
    try:
        threading.Thread(target=run_sync_in_background, daemon=True).start()
        add_history_event("Sincronização Iniciada", "Sincronização em segundo plano iniciada.")
        return jsonify({"status": "success", "message": "Sincronização iniciada em segundo plano!"})
    except Exception as e:
        add_history_event("Erro ao iniciar Sync", str(e))
        logger.error(f"Erro na sincronização: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500

def trigger_airflow_dag(dag_id, logical_date=None):
    """Tenta disparar uma DAG no Airflow via API REST ou Docker CLI."""
    import requests
    import subprocess
    import json
    from datetime import datetime, timezone

    try:
        airflow_url = os.getenv('AIRFLOW_URL', 'http://localhost:8080')
        auth = ("airflow", "airflow")
        
        # 1. Unpause the DAG
        patch_url = f"{airflow_url}/api/v1/dags/{dag_id}"
        requests.patch(patch_url, json={"is_paused": False}, auth=auth, timeout=5)
        
        # 2. Trigger the DAG
        trigger_url = f"{airflow_url}/api/v1/dags/{dag_id}/dagRuns"
        payload = {}
        if logical_date:
            try:
                # O sistema Airflow espera que 'trigger_date' venha via conf para evitar conflito de data lógica.
                # Ele valida o formato estrito YYYY-MM-DD para parâmetros do tipo 'date'.
                payload["conf"] = {"trigger_date": logical_date}
            except: pass
            
        response = requests.post(trigger_url, json=payload, auth=auth, timeout=5)
        
        if response.status_code in [200, 201]:
            return True, f"DAG {dag_id} disparada via API."
        else:
            return False, f"Erro Airflow API ({response.status_code}): {response.text}"
    except Exception as e:
        # Fallback para docker exec caso a API não esteja acessível
        try:
            subprocess.run(["docker", "compose", "exec", "-T", "airflow-scheduler", "airflow", "dags", "unpause", dag_id], capture_output=True, timeout=15)
            cmd = ["docker", "compose", "exec", "-T", "airflow-scheduler", "airflow", "dags", "trigger", dag_id]
            if logical_date:
                cmd.extend(["--conf", json.dumps({"trigger_date": logical_date})])
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
            
            if result.returncode == 0:
                return True, f"DAG {dag_id} disparada via Docker CLI."
            else:
                return False, f"Erro Docker/Airflow: {result.stderr or result.stdout}"
        except Exception as e2:
            return False, f"Falha API REST ({str(e)}) e falha Docker CLI ({str(e2)})"

@app.route('/api/routines/trigger/<path:file>', methods=['POST'])
@login_required
def trigger_routine(file):
    req_data = request.get_json(silent=True) or {}
    logical_date = req_data.get('logical_date')
    
    if logical_date:
        try:
            datetime.strptime(logical_date, '%Y-%m-%d')
        except ValueError:
            return jsonify({"status": "error", "message": "Data inválida. Use o formato AAAA-MM-DD."}), 400
    
    dag_confs_path = os.path.join(BASE_DIR, "dag_confs")
    
    # Caso especial: Rotina de Sincronização (pode ter múltiplas partes)
    if file == "Pesquisa_cnpj.yaml":
        parts = glob.glob(os.path.join(dag_confs_path, "Pesquisa_cnpj_sync.yaml"))
        if not parts:
            parts = glob.glob(os.path.join(dag_confs_path, "Pesquisa_cnpj_part_*.yaml"))
        if not parts:
            # Se não tem partes, tenta a principal
            file_path = os.path.join(dag_confs_path, file)
            if not os.path.exists(file_path):
                return jsonify({"status": "error", "message": "Arquivo base não encontrado."}), 404
            parts = [file_path]
            
        success_count = 0
        errors = []
        for p in parts:
            try:
                with open(p, 'r', encoding='utf-8') as f:
                    data = yaml.safe_load(f)
                    dag_id = data.get('dag', {}).get('id')
                    if dag_id:
                        ok, msg = trigger_airflow_dag(dag_id, logical_date)
                        if ok: success_count += 1
                        else: errors.append(msg)
            except: continue
        
        if success_count > 0:
            add_history_event("Busca Iniciada", f"Rotina {file} (ou suas partes) disparada via Airflow. Data Lógica: {logical_date or 'Atual'}")
            return jsonify({"status": "success", "message": f"{success_count} parte(s) disparada(s)!"})
        else:
            return jsonify({"status": "error", "message": "Nenhuma parte pôde ser disparada.", "details": errors}), 500

    # Rotinas Customizadas
    file_path = os.path.join(dag_confs_path, file)
    if not os.path.exists(file_path):
        return jsonify({"status": "error", "message": "Arquivo de rotina não encontrado."}), 404
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            dag_id = data.get('dag', {}).get('id')
            if not dag_id: dag_id = re.sub(r'\.[^.]*$', '', file)
            
            ok, msg = trigger_airflow_dag(dag_id, logical_date)
            if ok:
                add_history_event("Busca Iniciada", f"Rotina {file} disparada via Airflow. Data Lógica: {logical_date or 'Atual'}")
                return jsonify({"status": "success", "message": f"Busca {dag_id} iniciada!"})
            else:
                # Mesmo se o comando falhar, registramos a tentativa
                add_history_event("Busca (Tentativa)", f"Tentativa de disparar {dag_id}: {msg}")
                return jsonify({"status": "warning", "message": "Busca solicitada, mas houve erro no Airflow.", "details": msg})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/save_settings', methods=['POST'])
@login_required
def save_settings():
    if session['user']['role'] != 'master': return jsonify({"status": "error"}), 403
    data = request.json
    from src.models import db, Settings
    
    try:
        settings_record = Settings.query.filter_by(key='global_settings').first()
        if not settings_record:
            settings_record = Settings(key='global_settings')
            db.session.add(settings_record)
        settings_record.set_value(data)
        db.session.commit()
        
        env_path = os.path.join(BASE_DIR, '.env')
        
        # Mapeamento para GestãoClick
        if 'api_keys' in data:
            ak = data['api_keys']
            mappings = {
                "gestaoclick_access_token": "ACCESS_TOKEN",
                "gestaoclick_secret_token": "SECRET_ACCESS_TOKEN",
                "gestaoclick_base_url": "BASE_URL",
                "yaml_path": "YAML_PATH"
            }
            for key, env_var in mappings.items():
                val = ak.get(key)
                if val:
                    set_key(env_path, env_var, val)
                    os.environ[env_var] = val
        
        # Mapeamento para SMTP (Airflow)
        if 'smtp' in data:
            smtp = data['smtp']
            smtp_mappings = {
                "server": "AIRFLOW__SMTP__SMTP_HOST",
                "port": "AIRFLOW__SMTP__SMTP_PORT",
                "user": "AIRFLOW__SMTP__SMTP_USER",
                "password": "AIRFLOW__SMTP__SMTP_PASSWORD",
                "from_email": "AIRFLOW__SMTP__SMTP_MAIL_FROM"
            }
            for key, env_var in smtp_mappings.items():
                val = smtp.get(key)
                if val:
                    set_key(env_path, env_var, str(val))
                    os.environ[env_var] = str(val)
            
            if not smtp.get('from_email') and smtp.get('user') and "@" in smtp.get('user'):
                set_key(env_path, "AIRFLOW__SMTP__SMTP_MAIL_FROM", smtp.get('user'))
                os.environ["AIRFLOW__SMTP__SMTP_MAIL_FROM"] = smtp.get('user')

        return jsonify({"status": "success", "message": "Configurações salvas e aplicadas!"})
    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "error", "message": "Erro ao salvar no BD: " + str(e)}), 500

@app.route('/api/users', methods=['GET', 'POST', 'DELETE'])
@login_required
def manage_users():
    if session['user']['role'] != 'master': return jsonify({"status": "error"}), 403
    from src.models import db, User
    
    if request.method == 'GET':
        users = User.query.all()
        return jsonify([{"username": u.username, "role": u.role} for u in users])
        
    if request.method == 'POST':
        data = request.json
        if not data.get('username') or not data.get('password'):
            return jsonify({"status": "error", "message": "Campos obrigatórios"}), 400
            
        if User.query.filter_by(username=data['username']).first():
            return jsonify({"status": "error", "message": "Já existe"}), 400
            
        new_user = User(username=data['username'], role=data.get('role', 'user'))
        new_user.set_password(data['password'])
        db.session.add(new_user)
        db.session.commit()
        return jsonify({"status": "success", "message": "Usuário criado com sucesso!"})
        
    elif request.method == 'DELETE':
        username = request.args.get('username')
        if username == session['user']['username']: return jsonify({"status": "error"}), 400
        user = User.query.filter_by(username=username).first()
        if user:
            db.session.delete(user)
            db.session.commit()
            return jsonify({"status": "success"})
    return jsonify({"status": "error"}), 500

@app.route('/api/export_report')
@login_required
def export_report():
    import pandas as pd
    import tempfile
    empresas = get_companies_data()
    
    df = pd.DataFrame(empresas)
    if not df.empty:
        df = df[['nome', 'cnpj', 'uf', 'cidade', 'email', 'telefone', 'situacao', 'status', 'origem']]
        df.columns = ['Empresa', 'CNPJ', 'UF', 'Cidade', 'Email', 'Telefone', 'Situação', 'Monitorado', 'Origem']
        df['Monitorado'] = df['Monitorado'].apply(lambda x: 'Sim' if x else 'Não')
    else:
        df = pd.DataFrame(columns=['Empresa', 'CNPJ', 'UF', 'Cidade', 'Email', 'Telefone', 'Situação', 'Monitorado', 'Origem'])
    
    tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False, dir=DATA_DIR)
    df.to_excel(tmp.name, index=False)
    tmp_path = tmp.name
    @after_this_request
    def cleanup(response):
        try:
            os.unlink(tmp_path)
        except:
            pass
        return response
    return send_file(tmp_path, as_attachment=True, download_name="relatorio_empresas.xlsx")

@app.route('/api/test_smtp', methods=['POST'])
@login_required
def test_smtp():
    if session['user']['role'] != 'master': return jsonify({"status": "error", "message": "Acesso negado."}), 403
    data = request.json
    smtp = data.get('smtp', {})
    test_email = data.get('test_email')
    
    server = smtp.get('server')
    port = smtp.get('port')
    user = smtp.get('user')
    password = smtp.get('password')
    from_email = smtp.get('from_email') or user
    
    if not all([server, port, user, password, test_email]):
        return jsonify({"status": "error", "message": "Todos os campos de SMTP e o email de teste são obrigatórios."}), 400
        
    
    msg = MIMEText("Este é um email de teste enviado pelo Painel de Controle do Ro-DOU Registrale para validar as configurações de SMTP.")
    msg['Subject'] = "Ro-DOU - Teste de Conexão SMTP"
    msg['From'] = from_email
    msg['To'] = test_email
    
    try:
        port_num = int(port)
        if port_num == 465:
            server_conn = smtplib.SMTP_SSL(server, port_num, timeout=10)
        else:
            server_conn = smtplib.SMTP(server, port_num, timeout=10)
            if port_num == 587 or port_num == 25:
                try:
                    server_conn.starttls()
                except:
                    pass
                
        server_conn.login(user, password)
        server_conn.send_message(msg)
        server_conn.quit()
        return jsonify({"status": "success", "message": f"Email de teste enviado com sucesso para {test_email}!"})
    except Exception as e:
        logger.error(f"Falha ao testar SMTP: {e}")
        return jsonify({"status": "error", "message": f"Erro de conexão: {str(e)}"}), 500

@app.route('/api/export_sheets', methods=['POST'])
@login_required
def export_sheets():
    from src.models import Settings
    settings_record = Settings.query.filter_by(key='global_settings').first()
    settings = settings_record.get_value() if settings_record else {}
    gs = settings.get('google_sheets', {})
    spreadsheet_id = gs.get('spreadsheet_id')
    sheet_name = gs.get('sheet_name', 'Deteções Ro-DOU')
    credentials_str = gs.get('credentials_json')
    
    if not all([spreadsheet_id, credentials_str]):
        return jsonify({"status": "error", "message": "Google Sheets não configurado nas Integrações."}), 400
        
    data = request.json
    mentions_to_export = data.get('mentions', [])
    if not mentions_to_export:
        return jsonify({"status": "error", "message": "Nenhuma menção selecionada para exportar."}), 400
        
    try:
        import googleapiclient.discovery
        from google.oauth2 import service_account
    except ImportError:
        return jsonify({"status": "error", "message": "Bibliotecas do Google Sheets não instaladas. Execute 'pip install google-api-python-client google-auth' no servidor."}), 500

    try:
        credentials_info = json.loads(credentials_str)
        creds = service_account.Credentials.from_service_account_info(
            credentials_info,
            scopes=['https://www.googleapis.com/auth/spreadsheets']
        )
        service = googleapiclient.discovery.build('sheets', 'v4', credentials=creds)
        
        values = []
        for m in mentions_to_export:
            values.append([
                m.get('data', ''),
                m.get('empresa', ''),
                m.get('cnpj', ''),
                m.get('secao', ''),
                m.get('trecho', ''),
                m.get('link', '')
            ])
            
        range_name = f"{sheet_name}!A:F"
        body = {
            'values': values
        }
        
        result = service.spreadsheets().values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        
        return jsonify({"status": "success", "message": f"Exportado com sucesso! {result.get('updates', {}).get('updatedRows', 0)} linhas adicionadas no Google Sheets."})
    except Exception as e:
        logger.error(f"Erro ao exportar para Google Sheets: {e}")
        return jsonify({"status": "error", "message": f"Erro de integração: {str(e)}"}), 500

@app.route('/api/admin/clear_data', methods=['POST'])
@login_required
def admin_clear_data():
    global _mentions_cache, _mentions_cache_time, _mentions_deleted_at
    if session['user']['role'] != 'master': return jsonify({"status": "error", "message": "Acesso negado"}), 403
    data = request.json
    action_type = data.get('type')
    
    try:
        from src.models import db, Company, SyncHistory, Mention, Settings
        if action_type == 'all':
            Company.query.delete()
            SyncHistory.query.delete()
            Mention.query.delete()
            Settings.query.filter_by(key='mentions_cache_meta').delete()
            db.session.commit()
            _mentions_cache = None
            _mentions_cache_time = 0
            _mentions_deleted_at = time.time()
            
            # Limpa logs do Airflow
            log_dir = os.path.join(LOGS_DIR)
            if os.path.exists(log_dir):
                import shutil
                for item in os.listdir(log_dir):
                    item_path = os.path.join(log_dir, item)
                    if os.path.isdir(item_path):
                        shutil.rmtree(item_path, ignore_errors=True)
            return jsonify({"status": "success", "message": "Banco de dados e logs completamente zerados."})
            
        elif action_type == 'history':
            SyncHistory.query.delete()
            Mention.query.delete()
            Settings.query.filter_by(key='mentions_cache_meta').delete()
            db.session.commit()
            _mentions_cache = None
            _mentions_cache_time = 0
            _mentions_deleted_at = time.time()
            return jsonify({"status": "success", "message": "Histórico e cache removidos."})
            
        elif action_type == 'mentions':
            Mention.query.delete()
            Settings.query.filter_by(key='mentions_cache_meta').delete()
            db.session.commit()
            _mentions_cache = None
            _mentions_cache_time = 0
            _mentions_deleted_at = time.time()
            return jsonify({"status": "success", "message": "Mentions (alertas) removidas do painel."})
            
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/templates', methods=['GET', 'POST'])
@login_required
def manage_templates():
    from src.models import db, EmailTemplate
    if request.method == 'GET':
        templates = EmailTemplate.query.all()
        return jsonify([{"id": t.id, "name": t.name, "subject": t.subject, "body_html": t.body_html} for t in templates])
        
    if request.method == 'POST':
        if session['user']['role'] != 'master': return jsonify({"status": "error"}), 403
        data = request.json
        if not data.get('name') or not data.get('body_html'):
            return jsonify({"status": "error", "message": "Nome e corpo HTML são obrigatórios."}), 400
        try:
            template = EmailTemplate.query.filter_by(name=data.get('name')).first()
            if not template:
                template = EmailTemplate(name=data.get('name'))
                db.session.add(template)
            template.subject = data.get('subject', '')
            template.body_html = data.get('body_html', '')
            db.session.commit()
            return jsonify({"status": "success", "message": "Template salvo!"})
        except Exception as e:
            db.session.rollback()
            return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/templates/<int:t_id>', methods=['DELETE'])
@login_required
def delete_template(t_id):
    if session['user']['role'] != 'master': return jsonify({"status": "error"}), 403
    from src.models import db, EmailTemplate
    try:
        template = EmailTemplate.query.get(t_id)
        if not template:
            return jsonify({"status": "error", "message": "Template não encontrado."}), 404
        if template:
            if template.name == 'Padrão Registrale':
                return jsonify({"status": "error", "message": "Template padrão não pode ser excluído."}), 400
            db.session.delete(template)
            db.session.commit()
        return jsonify({"status": "success"})
    except:
        db.session.rollback()
        return jsonify({"status": "error"}), 500

@app.route('/api/send_email', methods=['POST'])
@login_required
def send_email():
    from src.models import Settings
    data = request.json
    to_emails = data.get('to_emails', [])
    subject = data.get('subject', 'Notificação Registrale')
    body_html = data.get('body_html', '')
    
    settings_record = Settings.query.filter_by(key='global_settings').first()
    settings = settings_record.get_value() if settings_record else {}
    smtp = settings.get('smtp', {})
    
    server = smtp.get('server')
    port = smtp.get('port')
    user = smtp.get('user')
    password = smtp.get('password')
    from_email = smtp.get('from_email') or user
    
    if not all([server, port, user, password]):
        return jsonify({"status": "error", "message": "Configurações SMTP não definidas."}), 400
        
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    
    try:
        port_num = int(port)
        if port_num == 465:
            server_conn = smtplib.SMTP_SSL(server, port_num, timeout=10)
        else:
            server_conn = smtplib.SMTP(server, port_num, timeout=10)
            if port_num == 587 or port_num == 25:
                server_conn.starttls()
                
        server_conn.login(user, password)
        
        for recipient in to_emails:
            msg = MIMEMultipart('alternative')
            msg['Subject'] = subject
            msg['From'] = from_email
            msg['To'] = recipient
            msg.attach(MIMEText(body_html, 'html'))
            server_conn.send_message(msg)
            
        server_conn.quit()
        
        # Opcional: Adicionar ao histórico
        add_history_event("Email Enviado", f"Emails enviados para: {', '.join(to_emails)}")
        return jsonify({"status": "success", "message": "E-mails enviados com sucesso!"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route('/api/export_pdf', methods=['POST'])
@login_required
def export_pdf():
    data = request.json
    companies = data.get('companies', [])
    filters = data.get('filters', {})
    
    output_filename = os.path.join(DATA_DIR, 'relatorio_empresas.pdf')
    
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from datetime import datetime

    doc = SimpleDocTemplate(output_filename, pagesize=landscape(A4), rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    elements = []
    styles = getSampleStyleSheet()
    
    # Custom Styles
    title_style = ParagraphStyle('TitleStyle', parent=styles['Heading1'], alignment=1, fontSize=18, spaceAfter=20)
    meta_style = ParagraphStyle('MetaStyle', parent=styles['Normal'], fontSize=10, spaceAfter=10)
    
    # 1. Header
    elements.append(Paragraph("<b>REGISTRALE</b> - Relatório de Empresas Monitoradas", title_style))
    elements.append(Paragraph(f"Data de Geração: {datetime.now().strftime('%d/%m/%Y %H:%M')}", meta_style))
    elements.append(Spacer(1, 20))
    
    # 2. Table Data
    table_data = [['Razão Social', 'CNPJ', 'UF', 'Cidade', 'Situação', 'Origem', 'Status']]
    for c in companies:
        table_data.append([
            c.get('nome', '')[:40] + ('...' if len(c.get('nome',''))>40 else ''), 
            c.get('cnpj', ''), 
            c.get('uf', ''), 
            c.get('cidade', ''), 
            c.get('situacao', ''), 
            c.get('origem', ''), 
            'Monitorado' if c.get('status') else 'Inativo'
        ])
    
    if len(table_data) > 1:
        t = Table(table_data, colWidths=[200, 100, 40, 100, 80, 80, 80])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#0f172a')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f8fafc')),
            ('GRID', (0,0), (-1,-1), 1, colors.HexColor('#e2e8f0')),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ]))
        elements.append(t)
    else:
        elements.append(Paragraph("Nenhuma empresa encontrada com os filtros atuais.", styles['Normal']))
        
    elements.append(PageBreak())
    
    # 3. Metadata Page
    elements.append(Paragraph("<b>Metadados e Filtros Aplicados</b>", title_style))
    elements.append(Spacer(1, 20))
    
    elements.append(Paragraph(f"<b>Usuário Solicitante:</b> {session['user']['username']} ({session['user']['role']})", meta_style))
    elements.append(Paragraph(f"<b>Data da Exportação:</b> {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}", meta_style))
    elements.append(Paragraph(f"<b>Total de Registros:</b> {len(companies)}", meta_style))
    
    elements.append(Spacer(1, 10))
    elements.append(Paragraph("<b>Filtros Utilizados:</b>", meta_style))
    for k, v in filters.items():
        elements.append(Paragraph(f"- {k.capitalize()}: {v if v else 'Todos'}", meta_style))
        
    doc.build(elements)
    
    return send_file(output_filename, as_attachment=True, download_name="relatorio_empresas.pdf", mimetype='application/pdf')

@app.route('/api/export_mentions_pdf', methods=['POST'])
@login_required
def export_mentions_pdf():
    import tempfile
    data = request.json
    mentions = data.get('mentions', [])
    
    output_filename = os.path.join(DATA_DIR, 'relatorio_mencoes.pdf')
    
    from reportlab.lib import colors
    from reportlab.lib.pagesizes import A4, landscape
    from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_LEFT
    from datetime import datetime

    doc = SimpleDocTemplate(output_filename, pagesize=landscape(A4), rightMargin=30, leftMargin=30, topMargin=30, bottomMargin=30)
    elements = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('TitleStyle', parent=styles['Heading1'], alignment=1, fontSize=18, spaceAfter=20)
    meta_style = ParagraphStyle('MetaStyle', parent=styles['Normal'], fontSize=10, spaceAfter=10)
    cell_style = ParagraphStyle('CellStyle', parent=styles['Normal'], fontSize=8, leading=10)
    link_style = ParagraphStyle('LinkStyle', parent=styles['Normal'], fontSize=8, leading=10, textColor=colors.HexColor('#2563eb'))
    
    elements.append(Paragraph("<b>REGISTRALE</b> - Relatório de Menções Detectadas", title_style))
    elements.append(Paragraph(f"Data de Geração: {datetime.now().strftime('%d/%m/%Y %H:%M')}", meta_style))
    elements.append(Spacer(1, 20))
    
    table_data = [['Data', 'Empresa', 'CNPJ', 'Seção', 'Trecho', 'Link']]
    for m in mentions:
        trecho = (m.get('trecho', '') or '')[:80] + ('...' if len(m.get('trecho', '') or '') > 80 else '')
        link = m.get('link', '')
        link_para = Paragraph(f'<a href="{link}">Abrir</a>', link_style) if link else ''
        table_data.append([
            m.get('data', ''),
            Paragraph(m.get('empresa', ''), cell_style),
            m.get('cnpj', ''),
            m.get('secao', ''),
            Paragraph(trecho, cell_style),
            link_para
        ])
    
    if len(table_data) > 1:
        t = Table(table_data, colWidths=[70, 140, 90, 70, 300, 50])
        t.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1c1917')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f5f4')),
            ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#d6d3d1')),
            ('FONTSIZE', (0, 1), (-1, -1), 8),
            ('VALIGN', (0, 0), (-1, -1), 'TOP'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1), [colors.HexColor('#f5f5f4'), colors.white]),
        ]))
        elements.append(t)
    else:
        elements.append(Paragraph("Nenhuma menção encontrada.", styles['Normal']))
    
    elements.append(PageBreak())
    
    elements.append(Paragraph("<b>Informações da Geração</b>", title_style))
    elements.append(Spacer(1, 20))
    
    meta_table_data = [
        ['Campo', 'Valor'],
        ['Data', datetime.now().strftime('%d/%m/%Y')],
        ['Hora', datetime.now().strftime('%H:%M:%S')],
        ['Usuário', session['user']['username']],
        ['Total de Menções', str(len(mentions))],
    ]
    
    meta_t = Table(meta_table_data, colWidths=[200, 300])
    meta_t.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#1c1917')),
        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BACKGROUND', (0, 1), (-1, -1), colors.HexColor('#f5f5f4')),
        ('GRID', (0, 0), (-1, -1), 1, colors.HexColor('#d6d3d1')),
        ('FONTSIZE', (0, 1), (-1, -1), 10),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
    ]))
    elements.append(meta_t)
    
    doc.build(elements)
    
    return send_file(output_filename, as_attachment=True, download_name="relatorio_mencoes.pdf", mimetype='application/pdf')

@app.route('/api/export_mentions_excel', methods=['POST'])
@login_required
def export_mentions_excel():
    import pandas as pd
    import tempfile
    data = request.json
    mentions = data.get('mentions', [])
    
    df = pd.DataFrame(mentions)
    if not df.empty:
        df = df[['data', 'empresa', 'cnpj', 'secao', 'trecho', 'link']]
        df.columns = ['Data', 'Empresa', 'CNPJ', 'Seção', 'Trecho', 'Link']
    else:
        df = pd.DataFrame(columns=['Data', 'Empresa', 'CNPJ', 'Seção', 'Trecho', 'Link'])
    
    tmp = tempfile.NamedTemporaryFile(suffix='.xlsx', delete=False, dir=DATA_DIR)
    df.to_excel(tmp.name, index=False)
    tmp_path = tmp.name
    @after_this_request
    def cleanup(response):
        try:
            os.unlink(tmp_path)
        except:
            pass
        return response
    return send_file(tmp_path, as_attachment=True, download_name="relatorio_mencoes.xlsx")

def create_default_email_template():
    from src.models import EmailTemplate
    
    default_html = '''<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Relatório Registrale</title>
    <style>
        * { font-family: 'Inter', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; box-sizing: border-box; }
        body { margin: 0; padding: 30px; background-color: #f1f5f9; line-height: 1.6; color: #334155; }
        .ext_header, .ext_footer, .container { max-width: 900px; margin: 0 auto 25px auto; background-color: #ffffff; border-radius: 12px; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.05); overflow: hidden; border: 1px solid #e2e8f0; }
        .ext_header { padding: 25px 30px; border-top: 5px solid #2563eb; font-size: 22px; font-weight: 700; color: #1e293b; }
        .ext_footer { padding: 25px 30px; border-top: 5px solid #94a3b8; font-size: 14px; }
        .content { padding: 25px 30px; }
        .result-item { margin-bottom: 20px; padding: 15px; background-color: #f8fafc; border-radius: 8px; border-left: 4px solid #2563eb; }
        .result-item h4 { margin: 0 0 8px 0; color: #1e293b; font-size: 15px; }
        .result-item .meta { font-size: 12px; color: #64748b; margin-bottom: 8px; }
        .result-item .abstract { font-size: 14px; color: #475569; line-height: 1.6; }
        .result-item a { color: #2563eb; text-decoration: none; font-weight: 600; }
        .result-item a:hover { text-decoration: underline; }
        .tag { display: inline-block; padding: 3px 8px; font-size: 10px; font-weight: 700; text-transform: uppercase; border-radius: 4px; margin-right: 5px; }
        .tag-section { background-color: #dbeafe; color: #1d4ed8; }
        .tag-date { background-color: #f1f5f9; color: #475569; }
        .footer-text { font-size: 12px; color: #94a3b8; text-align: center; margin-top: 20px; }
    </style>
</head>
<body>
    <div class="ext_header">
        <div style="display:flex;align-items:center;gap:12px;">
            <div style="width:40px;height:40px;background:#2563eb;border-radius:8px;display:flex;align-items:center;justify-content:center;">
                <svg xmlns="http://www.w3.org/2000/svg" width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M12 8V4H8"/><rect width="16" height="12" x="4" y="8" rx="2"/><path d="M2 14h2"/><path d="M20 14h2"/><path d="M15 13v2"/><path d="M9 13v2"/></svg>
            </div>
            <div>
                <div style="margin:0;">REGISTRALE</div>
                <div style="font-size:12px;font-weight:400;color:#64748b;margin:0;">Relatório de Menções Detectadas</div>
            </div>
        </div>
    </div>
    
    <div class="container">
        <div class="content">
            <p style="margin:0 0 15px 0;">Prezado(a),</p>
            <p style="margin:0 0 20px 0;">Foram detectadas as seguintes menções no Diário Oficial da União:</p>
            
            {content}
            
            <hr style="border:0;border-top:1px solid #e2e8f0;margin:25px 0;">
            <p class="footer-text">Este é um e-mail automático gerado pelo Sistema Registrale.</p>
        </div>
    </div>
    
    <div class="ext_footer" style="text-align:center;">
        <p style="margin:0;font-size:13px;color:#475569;">Registrale - Sistema de Monitoramento DOU</p>
        <p style="margin:8px 0 0 0;font-size:11px;color:#94a3b8;">Ministério da Gestão</p>
    </div>
</body>
</html>'''
    
    existing = EmailTemplate.query.filter_by(name='Padrão Registrale').first()
    if existing:
        existing.subject = 'Notificação Registrale - Menções Detectadas no DOU'
        existing.body_html = default_html
    else:
        template1 = EmailTemplate(
            name='Padrão Registrale',
            subject='Notificação Registrale - Menções Detectadas no DOU',
            body_html=default_html
        )
        db.session.add(template1)
    db.session.commit()

init_default_data()

if __name__ == '__main__':
    os.makedirs(DATA_DIR, exist_ok=True)
    with app.app_context():
        db.create_all()
        from src.models import User
        if not User.query.filter_by(username='admin').first():
            admin = User(username='admin', role='master')
            admin.set_password('admin')
            db.session.add(admin)
            db.session.commit()
        create_default_email_template()
    app.run(host='0.0.0.0', debug=False, port=5000)
