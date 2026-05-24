import os
import json
import sqlite3
import smtplib
import re
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
from flask import Flask, request, jsonify, send_from_directory, Response, stream_with_context

try:
    from flask_compress import Compress
    HAS_COMPRESS = True
except ImportError:
    HAS_COMPRESS = False

try:
    from flask_cors import CORS
    HAS_CORS = True
except ImportError:
    HAS_CORS = False

try:
    import pymysql
    import pymysql.cursors
    HAS_PYMYSQL = True
except ImportError:
    HAS_PYMYSQL = False

# ── Rutas relativas del sistema ──────────────────────────────────────────────
BASE_DIR   = os.path.dirname(os.path.abspath(__file__))
DB_PATH    = os.path.join(BASE_DIR, 'data', 'gestor_academico.db')
STATIC_DIR = os.path.join(BASE_DIR, 'static')

os.makedirs(os.path.join(BASE_DIR, 'data'), exist_ok=True)

app = Flask(__name__, static_folder=STATIC_DIR)
app.config['JSON_SORT_KEYS'] = False
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = False

if HAS_COMPRESS:
    Compress(app)

if HAS_CORS:
    CORS(app, resources={r"/api/*": {"origins": "*"}})

# ── Detección de base de datos ────────────────────────────────────────────────
def _detect_db_url():
    """Devuelve la URL MySQL normalizada, o None si se usa SQLite."""
    raw = (os.environ.get('DATABASE_URL') or os.environ.get('MYSQL_URL') or '').strip()
    if not raw:
        return None
    if raw.startswith('mysql://'):
        raw = 'mysql+pymysql://' + raw[len('mysql://'):]
    if raw.startswith('mysql+pymysql://') or raw.startswith('mysql+'):
        return raw
    return None

_MYSQL_URL = _detect_db_url()
DB_TYPE    = 'mysql' if _MYSQL_URL else 'sqlite'

def _parse_mysql_url(url):
    """Parsea mysql+pymysql://user:pass@host:port/dbname → dict de kwargs."""
    from urllib.parse import urlparse, unquote
    p = urlparse(url)
    return {
        'host':     p.hostname or '127.0.0.1',
        'port':     p.port or 3306,
        'user':     unquote(p.username or ''),
        'password': unquote(p.password or ''),
        'database': p.path.lstrip('/') or 'gestor',
        'charset':  'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor,
        'connect_timeout': 10,
        'autocommit': False,
    }

# ── Traducción SQL SQLite → MySQL ─────────────────────────────────────────────
_SQL_REPLACEMENTS = [
    ("datetime('now')",              "NOW()"),
    ("updated_at=datetime('now')",   "updated_at=NOW()"),
    ("INTEGER PRIMARY KEY AUTOINCREMENT", "INT AUTO_INCREMENT PRIMARY KEY"),
    ("CREATE INDEX IF NOT EXISTS",   "CREATE INDEX IF NOT EXISTS"),  # MySQL 8+ ok
]

def _translate(sql):
    if DB_TYPE == 'sqlite':
        return sql
    for old, new in _SQL_REPLACEMENTS:
        sql = sql.replace(old, new)
    return sql

# ── Wrapper de conexión MySQL (imita la interfaz sqlite3) ────────────────────
class _MySQLConn:
    """Envuelve una conexión pymysql para que sea compatible con el patrón
    `with get_db() as conn: conn.execute(...)` usado en todas las rutas."""

    def __init__(self, conn):
        self._conn = conn

    def execute(self, sql, params=None):
        sql = _translate(sql)
        cur = self._conn.cursor()
        cur.execute(sql, params or ())
        return cur

    def fetchone(self):
        raise AttributeError("Use conn.execute(...).fetchone()")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            self._conn.rollback()
        else:
            self._conn.commit()
        self._conn.close()
        return False

# ── get_db() híbrido ──────────────────────────────────────────────────────────
def get_db():
    if DB_TYPE == 'mysql':
        if not HAS_PYMYSQL:
            raise RuntimeError("pymysql no instalado. Ejecute: pip install pymysql")
        kwargs = _parse_mysql_url(_MYSQL_URL)
        conn   = pymysql.connect(**kwargs)
        return _MySQLConn(conn)
    # ── SQLite con WAL para mayor velocidad ──
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA cache_size=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA temp_store=MEMORY")
    return conn

# ── init_db() híbrido ─────────────────────────────────────────────────────────
_INIT_STMTS_SQLITE = [
    """CREATE TABLE IF NOT EXISTS gestor_db (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        data       TEXT    NOT NULL DEFAULT '{}',
        updated_at TEXT    NOT NULL DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS institution_db (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        sk         TEXT    NOT NULL UNIQUE,
        data       TEXT    NOT NULL DEFAULT '{}',
        updated_at TEXT    NOT NULL DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS notifications (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        kind       TEXT    NOT NULL,
        actor      TEXT,
        message    TEXT    NOT NULL,
        meta       TEXT,
        seen       INTEGER NOT NULL DEFAULT 0,
        created_at TEXT    NOT NULL DEFAULT (datetime('now'))
    )""",
    """CREATE TABLE IF NOT EXISTS documents (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        clave      TEXT    NOT NULL UNIQUE,
        est_id     TEXT,
        data       TEXT    NOT NULL DEFAULT '{}',
        created_at TEXT    NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT    NOT NULL DEFAULT (datetime('now'))
    )""",
    "CREATE INDEX IF NOT EXISTS idx_institution_sk   ON institution_db(sk)",
    "CREATE INDEX IF NOT EXISTS idx_notif_created    ON notifications(created_at DESC)",
    "CREATE INDEX IF NOT EXISTS idx_docs_clave       ON documents(clave)",
    "CREATE INDEX IF NOT EXISTS idx_docs_estid       ON documents(est_id)",
]

_INIT_STMTS_MYSQL = [
    """CREATE TABLE IF NOT EXISTS gestor_db (
        id         INT AUTO_INCREMENT PRIMARY KEY,
        data       MEDIUMTEXT NOT NULL,
        updated_at DATETIME   NOT NULL DEFAULT NOW()
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS institution_db (
        id         INT AUTO_INCREMENT PRIMARY KEY,
        sk         VARCHAR(191) NOT NULL UNIQUE,
        data       MEDIUMTEXT   NOT NULL,
        updated_at DATETIME     NOT NULL DEFAULT NOW()
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS notifications (
        id         INT AUTO_INCREMENT PRIMARY KEY,
        kind       VARCHAR(100) NOT NULL,
        actor      VARCHAR(255),
        message    TEXT         NOT NULL,
        meta       MEDIUMTEXT,
        seen       TINYINT(1)   NOT NULL DEFAULT 0,
        created_at DATETIME     NOT NULL DEFAULT NOW()
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    """CREATE TABLE IF NOT EXISTS documents (
        id         INT AUTO_INCREMENT PRIMARY KEY,
        clave      VARCHAR(500) NOT NULL UNIQUE,
        est_id     VARCHAR(255),
        data       MEDIUMTEXT   NOT NULL,
        created_at DATETIME     NOT NULL DEFAULT NOW(),
        updated_at DATETIME     NOT NULL DEFAULT NOW()
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4""",
    "CREATE INDEX IF NOT EXISTS idx_institution_sk  ON institution_db(sk)",
    "CREATE INDEX IF NOT EXISTS idx_notif_created   ON notifications(created_at)",
    "CREATE INDEX IF NOT EXISTS idx_docs_clave      ON documents(clave(191))",
    "CREATE INDEX IF NOT EXISTS idx_docs_estid      ON documents(est_id(191))",
]

def init_db():
    stmts = _INIT_STMTS_MYSQL if DB_TYPE == 'mysql' else _INIT_STMTS_SQLITE
    with get_db() as conn:
        for stmt in stmts:
            try:
                conn.execute(stmt)
            except Exception:
                pass  # índice ya existe → continuar

init_db()

# ── Utilidades ────────────────────────────────────────────────────────────────
def now_iso():
    return datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')

def parse_json(text):
    try:
        return json.loads(text) if text else None
    except Exception:
        return None

# ── Cabeceras de caché para respuestas API ───────────────────────────────────
def no_cache(response):
    response.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    response.headers['Pragma'] = 'no-cache'
    return response

# ============================================================
# RUTAS ESTÁTICAS
# ============================================================

def _serve_portal():
    resp = send_from_directory(STATIC_DIR, 'portal.html')
    resp.headers['Cache-Control'] = 'no-store, no-cache, must-revalidate'
    return resp

@app.route('/')
def index():
    return _serve_portal()

@app.route('/login')
def login():
    return _serve_portal()

@app.route('/portal.html')
def portal():
    return _serve_portal()

@app.route('/static/<path:filename>')
def static_files(filename):
    resp = send_from_directory(STATIC_DIR, filename)
    resp.headers['Cache-Control'] = 'public, max-age=31536000'
    return resp

# ============================================================
# HEALTH CHECK
# ============================================================

@app.route('/api/healthz')
def healthz():
    return jsonify({'ok': True, 'ts': now_iso(), 'db': DB_TYPE})

# ============================================================
# GESTOR DB  (configuración global del gestor)
# ============================================================

@app.route('/api/inetis/gestordb', methods=['GET'])
def gestordb_get():
    with get_db() as conn:
        row = conn.execute("SELECT data FROM gestor_db ORDER BY id LIMIT 1").fetchone()
    data = parse_json(row['data']) if row else None
    resp = jsonify({'data': data})
    return no_cache(resp)

@app.route('/api/inetis/gestordb', methods=['POST'])
def gestordb_post():
    body = request.get_json(force=True, silent=True) or {}
    data = body.get('data')
    if data is None:
        return jsonify({'error': 'data es requerido'}), 400
    data_str = json.dumps(data, ensure_ascii=False)
    with get_db() as conn:
        row = conn.execute("SELECT id FROM gestor_db LIMIT 1").fetchone()
        if row:
            conn.execute(
                "UPDATE gestor_db SET data=?, updated_at=datetime('now') WHERE id=?",
                (data_str, row['id'])
            )
        else:
            conn.execute("INSERT INTO gestor_db(data) VALUES(?)", (data_str,))
    return jsonify({'ok': True})

# ============================================================
# INSTITUTION DB  (datos por institución, clave=sk)
# ============================================================

@app.route('/api/inetis/db', methods=['GET'])
def institution_get():
    sk = request.args.get('sk', '').strip()
    if not sk:
        return jsonify({'error': 'sk requerido'}), 400
    with get_db() as conn:
        row = conn.execute(
            "SELECT data FROM institution_db WHERE sk=? LIMIT 1", (sk,)
        ).fetchone()
    data = parse_json(row['data']) if row else None
    resp = jsonify({'data': data})
    return no_cache(resp)

@app.route('/api/inetis/db', methods=['POST'])
def institution_post():
    body = request.get_json(force=True, silent=True) or {}
    sk   = body.get('sk', '').strip()
    data = body.get('data')
    if not sk or data is None:
        return jsonify({'error': 'sk y data son requeridos'}), 400
    data_str = json.dumps(data, ensure_ascii=False)
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM institution_db WHERE sk=? LIMIT 1", (sk,)
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE institution_db SET data=?, updated_at=datetime('now') WHERE sk=?",
                (data_str, sk)
            )
        else:
            conn.execute(
                "INSERT INTO institution_db(sk, data) VALUES(?, ?)",
                (sk, data_str)
            )
    return jsonify({'ok': True})

# ============================================================
# NOTIFICACIONES
# ============================================================

@app.route('/api/inetis/notify', methods=['POST'])
def notify_post():
    body = request.get_json(force=True, silent=True) or {}
    kind    = body.get('kind', '').strip()
    message = body.get('message', '').strip()
    if not kind or not message:
        return jsonify({'error': 'kind y message son requeridos'}), 400
    actor = body.get('actor') or None
    meta  = body.get('meta')
    meta_str = json.dumps(meta, ensure_ascii=False) if meta is not None else None
    with get_db() as conn:
        conn.execute(
            "INSERT INTO notifications(kind, actor, message, meta) VALUES(?,?,?,?)",
            (kind, actor, message, meta_str)
        )
    return jsonify({'ok': True})

@app.route('/api/inetis/notify', methods=['GET'])
@app.route('/api/inetis/notifications', methods=['GET'])
def notify_get():
    limit = min(int(request.args.get('limit', 50)), 200)
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, kind, actor, message, meta, seen, created_at "
            "FROM notifications ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
    result = []
    for r in rows:
        result.append({
            'id':         r['id'],
            'kind':       r['kind'],
            'actor':      r['actor'],
            'message':    r['message'],
            'meta':       parse_json(r['meta']),
            'seen':       bool(r['seen']),
            'created_at': r['created_at'],
        })
    return jsonify({'notifications': result})

@app.route('/api/inetis/notify/seen', methods=['POST'])
def notify_seen():
    with get_db() as conn:
        conn.execute("UPDATE notifications SET seen=1 WHERE seen=0")
    return jsonify({'ok': True})

# ============================================================
# DOCUMENTOS  (actas, compromisos, etc.)
# ============================================================

@app.route('/api/inetis/docs', methods=['POST'])
def docs_save():
    body = request.get_json(force=True, silent=True) or {}
    clave = body.get('clave', '').strip()
    if not clave:
        return jsonify({'error': 'clave requerida'}), 400
    est_id = str(body.get('estId', body.get('est_id', ''))) or None
    data_str = json.dumps(body, ensure_ascii=False)
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM documents WHERE clave=? LIMIT 1", (clave,)
        ).fetchone()
        if row:
            conn.execute(
                "UPDATE documents SET data=?, est_id=?, updated_at=datetime('now') WHERE clave=?",
                (data_str, est_id, clave)
            )
        else:
            conn.execute(
                "INSERT INTO documents(clave, est_id, data) VALUES(?,?,?)",
                (clave, est_id, data_str)
            )
    return jsonify({'ok': True})

@app.route('/api/inetis/docs', methods=['GET'])
def docs_list():
    est_id = request.args.get('estId', '').strip()
    with get_db() as conn:
        if est_id:
            rows = conn.execute(
                "SELECT data FROM documents WHERE est_id=? ORDER BY updated_at DESC",
                (est_id,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT data FROM documents ORDER BY updated_at DESC LIMIT 100"
            ).fetchall()
    return jsonify([parse_json(r['data']) for r in rows if r['data']])

@app.route('/api/inetis/docs/<path:clave>', methods=['GET'])
def docs_get(clave):
    with get_db() as conn:
        row = conn.execute(
            "SELECT data FROM documents WHERE clave=? LIMIT 1", (clave,)
        ).fetchone()
    if not row:
        return jsonify(None)
    return jsonify(parse_json(row['data']))

@app.route('/api/inetis/docs/<path:clave>', methods=['DELETE'])
def docs_delete(clave):
    with get_db() as conn:
        conn.execute("DELETE FROM documents WHERE clave=?", (clave,))
    return jsonify({'ok': True})

# ============================================================
# IA — RESPUESTAS FALLBACK (sin Gemini)
# ============================================================

def _fallback_aidan(msg_lower, modo_general=False):
    m = msg_lower
    def t(*patterns):
        return any(re.search(p, m) for p in patterns)

    if modo_general:
        if t(r'hola|buenos|buenas|saludos'):
            return "¡Hola! Soy **Adán**, su asistente de consulta general. Puedo ayudarle con matemáticas, ciencias, historia, educación y mucho más. ¿Cuál es su pregunta?"
        if t(r'formula|area|calculo|ecuacion|matematica|algebra|geometria'):
            return "📐 **Matemáticas**: Área del círculo = **π·r²** | Rectángulo = **base × altura** | Triángulo = **(base × altura) / 2** | Pitágoras: **a²+b²=c²** | Fórmula cuadrática: **x = (−b ± √(b²−4ac)) / 2a**"
        if t(r'fotosintes|celula|biologia|cuerpo humano|ecosistema|adn'):
            return "🌱 **Biología**: La **fotosíntesis** convierte CO₂ + H₂O + luz solar → glucosa + O₂ en los cloroplastos. El ADN lleva la información genética en pares de bases (A-T, G-C)."
        if t(r'historia|colombia|independencia|simon bolivar'):
            return "🇨🇴 **Historia de Colombia**: La independencia se proclamó el **20 de julio de 1810**. Simón Bolívar fue figura clave en la liberación de la Gran Colombia. La Constitución vigente data de **1991**."
        if t(r'clima|temperatura|lluvia|meteorolog'):
            return "☁️ **Ciencias de la Tierra**: El clima colombiano varía por altitud: costas y valles (28-35°C), montañas (18-24°C), páramos (0-12°C). Colombia tiene estación seca e invernal."
        if t(r'ingles|english|traduccion|idioma'):
            return "🌐 **Idiomas**: Buenos días = **good morning** | Gracias = **thank you** | ¿Cómo está? = **how are you?** | Por favor = **please** | De nada = **you're welcome**"
        if t(r'convivencia|disciplina|conflicto|escolar'):
            return "🤝 **Convivencia Escolar**: Estrategias clave: 1) Mediación de pares, 2) Círculos restaurativos, 3) Pactos de aula, 4) Reconocimiento de logros, 5) Comunicación asertiva. Use el módulo **Observador** del Gestor."
        return "Soy **Adán**, su asistente de inteligencia artificial. Puedo ayudarle con matemáticas, ciencias, historia, idiomas, tecnología, legislación educativa y más. ¿Cuál es su pregunta?"

    if t(r'nota|calificac|planilla'):
        return "📊 **Planilla de Calificaciones**: Módulo 'Planilla' → seleccione el grado → haga clic en la celda del estudiante para ingresar la nota. Las notas se guardan automáticamente."
    if t(r'asistencia'):
        return "📋 **Asistencia**: Módulo 'Asistencia' → seleccione grado y asignatura → marque P (Presente), A (Ausente) o J (Justificado). Descargue planillas en PDF o Excel."
    if t(r'boletin|informe|reporte'):
        return "📄 **Boletines**: Módulo 'Informes' → pestaña 'Boletines' → seleccione grado y periodo → 'Generar PDF'. También puede generar boletín individual por estudiante."
    if t(r'observador|disciplina|conducta'):
        return "📝 **Observador**: Módulo 'Observador' → seleccione grado y estudiante → escriba la anotación y elija su tipo. Queda registrado con fecha automática."
    if t(r'horario'):
        return "🕐 **Horarios**: Módulo 'Horarios' → seleccione el grado → asigne asignaturas a los bloques de la semana."
    if t(r'estudiante|alumno|matric'):
        return "👤 **Estudiantes**: 'Configuración' → 'Estudiantes' → '+ Nuevo' para agregar. Puede asignarle grado, acudiente y contacto."
    if t(r'docente|profesor'):
        return "👩‍🏫 **Docentes**: 'Configuración' → 'Usuarios' para crear y gestionar docentes con sus asignaturas."
    if t(r'contrasena|clave|password|acceso'):
        return "🔑 **Contraseñas**: El rector o admin restablece contraseñas desde 'Configuración' → 'Usuarios'."
    if t(r'hola|buenos|buenas|saludos|ayuda'):
        return "¡Hola! Soy **Adán**. ¿En qué le puedo ayudar con el Gestor Académico? (Planilla, Asistencia, Boletines, Observador, Horarios, Estudiantes, Pre-matrícula…)"
    if t(r'prematricula|pre-matricula|inscripcion'):
        return "📋 **Pre-matrícula**: Módulo 'Pre-matrícula' → revise y apruebe solicitudes de inscripción de nuevos estudiantes."
    if t(r'descriptor|logro|competencia'):
        return "📚 **Descriptores**: En el módulo 'Planilla', cada asignatura tiene descriptores de desempeño por nivel (Superior, Alto, Básico, Bajo)."
    return "¡Hola! Soy **Adán**, el asistente del Gestor Académico YC. Puedo orientarle sobre cualquier módulo del sistema. ¿En qué le puedo ayudar?"

def _fallback_general(msg_lower):
    m = msg_lower
    def t(*patterns):
        return any(re.search(p, m) for p in patterns)
    if t(r'hola|buenos|buenas|saludos'):
        return "¡Hola! Bienvenido al Asistente Virtual. Estoy aquí para responder sus preguntas sobre cualquier tema académico o educativo. ¿En qué le puedo ayudar?"
    if t(r'formula|area|calculo|ecuacion|matematica|algebra|geometria|trigonometr'):
        return "📐 **Matemáticas**: Área círculo = π·r² | Área rectángulo = base × altura | Área triángulo = (base × altura)/2 | Pitágoras: a²+b²=c² | Cuadrática: x = (−b ± √(b²−4ac)) / 2a"
    if t(r'fotosintes|celula|biologia|cuerpo|ecosistema|adn|genetica'):
        return "🌱 **Biología**: Fotosíntesis: CO₂ + H₂O + luz → glucosa + O₂. Célula eucariota: núcleo, mitocondrias, membrana. ADN: pares de bases (A-T, G-C)."
    if t(r'historia|colombia|independencia|bolivar|constitucion'):
        return "🇨🇴 **Historia de Colombia**: Independencia: 20 de julio de 1810. Simón Bolívar lideró la Gran Colombia. Constitución vigente: 1991. Colombia tiene 32 departamentos."
    if t(r'ley 115|decreto 1290|decreto 1965|ley 1620|pei|siee|men|normativa|legislacion'):
        return "⚖️ **Normativa Educativa**: Ley 115 (1994) = Ley General de Educación. Decreto 1290 (2009) = Evaluación y promoción. Ley 1620 (2013) = Convivencia escolar. El PEI es el Proyecto Educativo Institucional."
    if t(r'convivencia|disciplina|conflicto|manual'):
        return "🤝 **Convivencia Escolar** (Ley 1620): 1) Mediación de pares, 2) Círculos restaurativos, 3) Pactos de aula, 4) Comité de convivencia, 5) Rutas de atención integral."
    if t(r'acta|compromiso|documento'):
        return "📋 **Actas de Compromiso**: Debe incluir: fecha, institución, datos del estudiante y acudientes, descripción de la situación, compromisos con responsables y fechas, firmas de todos."
    return "Hola, soy su Asistente Virtual. Puedo ayudarle con preguntas sobre educación, ciencias, matemáticas, historia, legislación colombiana y mucho más. ¿Cuál es su consulta?"

# ── Gemini opcional ───────────────────────────────────────────────────────────
def get_gemini():
    api_key = os.environ.get('GEMINI_API_KEY', '').strip()
    if not api_key:
        return None
    try:
        import google.generativeai as genai
        genai.configure(api_key=api_key)
        return genai
    except ImportError:
        return None

# ============================================================
# IA — CHAT (Adán)
# ============================================================

@app.route('/api/inetis/ai/chat', methods=['POST'])
def ai_chat():
    body = request.get_json(force=True, silent=True) or {}
    messages = body.get('messages', [])
    context  = body.get('context', {}) or {}

    if not isinstance(messages, list):
        return jsonify({'error': 'messages es requerido'}), 400

    modo_general = bool(context.get('modoGeneral'))
    modo_gestor  = bool(context.get('gestorMode'))

    last_msg = ''
    for m in reversed(messages):
        if m.get('role') == 'user' and m.get('content'):
            last_msg = m['content']
            break

    def generate():
        genai = get_gemini()
        if not genai:
            import unicodedata
            normalized = unicodedata.normalize('NFD', last_msg.lower())
            clean_m = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
            resp = _fallback_aidan(clean_m, modo_general)
            yield f"data: {json.dumps({'content': resp}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
            return

        try:
            if modo_general:
                sys_inst = (
                    f"Eres Adán, un asistente de IA de propósito general integrado en el Gestor Académico YC de Colombia. "
                    f"Puedes responder preguntas sobre CUALQUIER tema. Respondes siempre en español colombiano, amable, claro y profesional. "
                    f"Usuario={context.get('usuario','—')}, Institución={context.get('institucion','—')}, Año={context.get('anio', datetime.now().year)}."
                )
            elif modo_gestor:
                sys_inst = (
                    f"Eres Adán, el Asistente académico del Gestor Académico YC — sistema de gestión educativa multi-institución para Colombia. "
                    f"Ayudas al administrador general. Respondes siempre en español colombiano, amable, conciso y profesional. "
                    f"Usuario={context.get('usuario','Admin')}, Año={context.get('anio', datetime.now().year)}."
                )
            else:
                sys_inst = (
                    f"Eres Adán, el Asistente académico del Gestor Académico YC — sistema de gestión educativa colombiano. "
                    f"Ayudas con calificaciones, descriptores, boletines, asistencia, observadores, horarios, informes y pre-matrícula. "
                    f"Respondes en español colombiano, amable, conciso y profesional. "
                    f"Institución={context.get('institucion','—')}, Módulo={context.get('modulo','—')}, "
                    f"Usuario={context.get('usuario','—')}, Rol={context.get('rol','—')}, Año={context.get('anio', datetime.now().year)}."
                )

            model = genai.GenerativeModel(
                model_name='gemini-1.5-flash',
                system_instruction=sys_inst
            )

            valid = [m for m in messages if m.get('role') in ('user','assistant') and m.get('content')][-20:]
            history = []
            for m in valid[:-1]:
                history.append({'role': 'user' if m['role']=='user' else 'model',
                                 'parts': [m['content']]})

            chat = model.start_chat(history=history)
            response = chat.send_message(last_msg, stream=True)
            for chunk in response:
                text = chunk.text if hasattr(chunk, 'text') else ''
                if text:
                    yield f"data: {json.dumps({'content': text}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'error': 'Error al procesar la consulta. Intente nuevamente.'})}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

# ============================================================
# IA — ASISTENTE GENERAL
# ============================================================

@app.route('/api/inetis/ai/general', methods=['POST'])
def ai_general():
    body = request.get_json(force=True, silent=True) or {}
    messages = body.get('messages', [])
    context  = body.get('context', {}) or {}

    last_msg = ''
    for m in reversed(messages):
        if m.get('role') == 'user' and m.get('content'):
            last_msg = m['content']
            break

    def generate():
        genai = get_gemini()
        if not genai:
            import unicodedata
            normalized = unicodedata.normalize('NFD', last_msg.lower())
            clean_m = ''.join(c for c in normalized if unicodedata.category(c) != 'Mn')
            resp = _fallback_general(clean_m)
            yield f"data: {json.dumps({'content': resp}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
            return

        try:
            sys_inst = (
                f"Eres un Asistente Virtual educativo avanzado para instituciones colombianas. "
                f"Puedes responder sobre CUALQUIER tema: matemáticas, ciencias, historia, idiomas, tecnología, "
                f"legislación educativa colombiana (Ley 115, Decreto 1290, Ley 1620, etc.), pedagogía y más. "
                f"Respondes en español colombiano, amable, preciso y profesional. "
                f"Usuario={context.get('usuario','—')}, Institución={context.get('institucion','—')}, Año={context.get('anio', datetime.now().year)}."
            )
            model = genai.GenerativeModel(
                model_name='gemini-1.5-flash',
                system_instruction=sys_inst
            )
            valid = [m for m in messages if m.get('role') in ('user','assistant') and m.get('content')][-20:]
            history = []
            for m in valid[:-1]:
                history.append({'role': 'user' if m['role']=='user' else 'model',
                                 'parts': [m['content']]})
            chat = model.start_chat(history=history)
            response = chat.send_message(last_msg, stream=True)
            for chunk in response:
                text = chunk.text if hasattr(chunk, 'text') else ''
                if text:
                    yield f"data: {json.dumps({'content': text}, ensure_ascii=False)}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
        except Exception:
            yield f"data: {json.dumps({'error': 'Error al procesar la consulta.'})}\n\n"

    return Response(stream_with_context(generate()), mimetype='text/event-stream',
                    headers={'Cache-Control': 'no-cache', 'X-Accel-Buffering': 'no'})

# ============================================================
# IA — TTS  (síntesis de voz)
# ============================================================

@app.route('/api/inetis/ai/tts', methods=['POST'])
def ai_tts():
    body = request.get_json(force=True, silent=True) or {}
    text = body.get('text', '').strip()
    if not text:
        return jsonify({'error': 'text es requerido'}), 400
    return jsonify({'error': 'TTS no disponible — configure GEMINI_API_KEY para síntesis de voz avanzada'}), 503

# ============================================================
# CORREO ELECTRÓNICO
# ============================================================

@app.route('/api/inetis/send-email', methods=['POST'])
def send_email():
    body = request.get_json(force=True, silent=True) or {}
    to      = body.get('to', '').strip()
    subject = body.get('subject', '').strip()
    html    = body.get('html', '')
    text    = body.get('text', '')

    if not to or not subject or (not html and not text):
        return jsonify({'error': 'to, subject y html/text son requeridos'}), 400

    email_user = os.environ.get('EMAIL_USER', '').strip()
    email_pass = os.environ.get('EMAIL_PASS', '').strip()
    email_host = os.environ.get('EMAIL_HOST', 'smtp.gmail.com').strip()
    email_port = int(os.environ.get('EMAIL_PORT', '587'))

    if not email_user or not email_pass:
        return jsonify({
            'error': 'Servicio de correo no configurado',
            'hint': 'Configure EMAIL_USER, EMAIL_PASS, EMAIL_HOST y EMAIL_PORT'
        }), 503

    try:
        msg = MIMEMultipart('alternative')
        msg['From']    = f'"Gestor Académico YC" <{email_user}>'
        msg['To']      = to
        msg['Subject'] = subject

        if text:
            msg.attach(MIMEText(text, 'plain', 'utf-8'))
        if html:
            msg.attach(MIMEText(html, 'html', 'utf-8'))

        with smtplib.SMTP(email_host, email_port, timeout=15) as server:
            server.ehlo()
            server.starttls()
            server.login(email_user, email_pass)
            server.sendmail(email_user, to, msg.as_string())

        return jsonify({'ok': True})
    except Exception as e:
        return jsonify({'error': f'No se pudo enviar el correo: {str(e)}'}), 500

# ============================================================
# ARRANQUE
# ============================================================

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    app.run(host='0.0.0.0', port=port, debug=debug, threaded=True)
