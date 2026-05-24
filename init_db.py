"""
Script de inicialización de la base de datos SQLite.
Ejecutar UNA VEZ después de subir el proyecto al servidor:

    python3 init_db.py

Esto crea gestor_academico.db con el esquema completo.
Si la DB ya existe, no sobreescribe los datos.
"""
import os
import json
import sqlite3

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH  = os.path.join(BASE_DIR, 'data', 'gestor_academico.db')

os.makedirs(os.path.join(BASE_DIR, 'data'), exist_ok=True)

print(f"[init_db] Base de datos: {DB_PATH}")

conn = sqlite3.connect(DB_PATH)
conn.execute("PRAGMA journal_mode=WAL")
conn.execute("PRAGMA synchronous=NORMAL")

conn.executescript("""
    CREATE TABLE IF NOT EXISTS gestor_db (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        data       TEXT    NOT NULL DEFAULT '{}',
        updated_at TEXT    NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS institution_db (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        sk         TEXT    NOT NULL UNIQUE,
        data       TEXT    NOT NULL DEFAULT '{}',
        updated_at TEXT    NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS notifications (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        kind       TEXT    NOT NULL,
        actor      TEXT,
        message    TEXT    NOT NULL,
        meta       TEXT,
        seen       INTEGER NOT NULL DEFAULT 0,
        created_at TEXT    NOT NULL DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS documents (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        clave      TEXT    NOT NULL UNIQUE,
        est_id     TEXT,
        data       TEXT    NOT NULL DEFAULT '{}',
        created_at TEXT    NOT NULL DEFAULT (datetime('now')),
        updated_at TEXT    NOT NULL DEFAULT (datetime('now'))
    );

    CREATE INDEX IF NOT EXISTS idx_institution_sk   ON institution_db(sk);
    CREATE INDEX IF NOT EXISTS idx_notif_created    ON notifications(created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_docs_clave       ON documents(clave);
    CREATE INDEX IF NOT EXISTS idx_docs_estid       ON documents(est_id);
""")

# Verificar si ya existe un registro en gestor_db
row = conn.execute("SELECT COUNT(*) FROM gestor_db").fetchone()
if row[0] == 0:
    # Datos iniciales del gestor con institución de ejemplo
    gestor_inicial = {
        "plataformas": [
            {
                "id": "inst-demo-001",
                "sk": "inst-demo-001",
                "nombre": "Institución Educativa Demo",
                "nit": "000000000-0",
                "municipio": "Su Municipio",
                "departamento": "Su Departamento",
                "telefono": "",
                "email": "",
                "rector": "Rector(a) Ejemplo",
                "users": [
                    {"u": "admin",   "p": "admin2024",  "r": "rector",  "n": "Administrador"},
                    {"u": "docente", "p": "docente2024", "r": "docente", "n": "Docente Ejemplo"}
                ],
                "anio": 2024
            }
        ],
        "adminUser": "gestor",
        "adminPass": "gestor2024"
    }
    conn.execute(
        "INSERT INTO gestor_db(data) VALUES(?)",
        (json.dumps(gestor_inicial, ensure_ascii=False),)
    )
    print("[init_db] ✅ Datos iniciales del gestor insertados")
else:
    print("[init_db] ℹ️  gestor_db ya tiene datos — no se sobreescribe")

# Verificar institution_db
row2 = conn.execute("SELECT COUNT(*) FROM institution_db").fetchone()
if row2[0] == 0:
    inst_inicial = {
        "nombre": "Institución Educativa Demo",
        "anio": 2024,
        "grados": ["Preescolar","1°","2°","3°","4°","5°","6°","7°","8°","9°","10°","11°"],
        "asignaturas": ["Matemáticas","Español","Ciencias Naturales","Ciencias Sociales",
                        "Inglés","Educación Física","Artística","Ética","Religión","Tecnología"],
        "estudiantes": [],
        "users": [
            {"u": "admin",   "p": "admin2024",  "r": "rector",  "n": "Administrador"},
            {"u": "docente", "p": "docente2024", "r": "docente", "n": "Docente Ejemplo"}
        ],
        "notas": {},
        "asistencia": {},
        "observador": {},
        "horarios": {}
    }
    conn.execute(
        "INSERT INTO institution_db(sk, data) VALUES(?,?)",
        ("inst-demo-001", json.dumps(inst_inicial, ensure_ascii=False))
    )
    print("[init_db] ✅ Institución demo insertada")
else:
    print(f"[init_db] ℹ️  institution_db ya tiene {row2[0]} registro(s) — no se sobreescribe")

conn.commit()
conn.close()

print("[init_db] ✅ Base de datos lista en:", DB_PATH)
print()
print("Credenciales iniciales:")
print("  Gestor (admin general): usuario=gestor  contraseña=gestor2024")
print("  Rector de la demo:      usuario=admin   contraseña=admin2024")
print("  Docente de la demo:     usuario=docente contraseña=docente2024")
print()
print("¡CAMBIE ESTAS CONTRASEÑAS INMEDIATAMENTE desde el panel del sistema!")
