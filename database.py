import os
import csv
import io
from config import Config

def get_db():
    database_url = os.environ.get('DATABASE_URL')
    if database_url:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(database_url)
        conn.autocommit = False
        return conn
    else:
        import sqlite3
        conn = sqlite3.connect(Config.DATABASE)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

def is_postgres():
    return bool(os.environ.get('DATABASE_URL'))

def placeholder():
    return '%s' if is_postgres() else '?'

def init_db():
    conn = get_db()
    c = conn.cursor()
    pg = is_postgres()

    if pg:
        c.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id            SERIAL PRIMARY KEY,
                email         TEXT    NOT NULL UNIQUE,
                password_hash TEXT    NOT NULL,
                created_at    TIMESTAMP DEFAULT NOW()
            )
        ''')
        c.execute('''
            ALTER TABLE users ADD COLUMN IF NOT EXISTS email_verified BOOLEAN DEFAULT FALSE
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS assets (
                id          SERIAL PRIMARY KEY,
                user_id     INTEGER NOT NULL,
                isin        TEXT,
                ticker      TEXT    NOT NULL,
                name        TEXT    NOT NULL,
                asset_type  TEXT    NOT NULL DEFAULT 'ETF',
                currency    TEXT    NOT NULL DEFAULT 'EUR',
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(user_id, ticker)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                id              SERIAL PRIMARY KEY,
                user_id         INTEGER NOT NULL,
                asset_id        INTEGER NOT NULL,
                date            TEXT    NOT NULL,
                shares          REAL    NOT NULL,
                price_per_share REAL    NOT NULL,
                total_cost      REAL    NOT NULL,
                fees            REAL    DEFAULT 0,
                notes           TEXT    DEFAULT '',
                created_at      TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (user_id)  REFERENCES users(id)   ON DELETE CASCADE,
                FOREIGN KEY (asset_id) REFERENCES assets(id)  ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS sales (
                id                SERIAL PRIMARY KEY,
                user_id           INTEGER NOT NULL,
                asset_id          INTEGER NOT NULL,
                date              TEXT    NOT NULL,
                shares            REAL    NOT NULL,
                price_per_share   REAL    NOT NULL,
                total_proceeds    REAL    NOT NULL,
                fees              REAL    DEFAULT 0,
                notes             TEXT    DEFAULT '',
                created_at        TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (user_id)  REFERENCES users(id)  ON DELETE CASCADE,
                FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS dca_goals (
                id             SERIAL PRIMARY KEY,
                user_id        INTEGER NOT NULL UNIQUE,
                monthly_target REAL    NOT NULL DEFAULT 0,
                updated_at     TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS dividends (
                id          SERIAL PRIMARY KEY,
                user_id     INTEGER NOT NULL,
                asset_id    INTEGER NOT NULL,
                date        TEXT    NOT NULL,
                amount      REAL    NOT NULL,
                notes       TEXT    DEFAULT '',
                created_at  TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (user_id)  REFERENCES users(id)  ON DELETE CASCADE,
                FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS email_tokens (
                id         SERIAL PRIMARY KEY,
                user_id    INTEGER NOT NULL,
                token      TEXT    NOT NULL UNIQUE,
                created_at TIMESTAMP DEFAULT NOW(),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        ''')
    else:
        c.execute('''
            CREATE TABLE IF NOT EXISTS users (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                email         TEXT    NOT NULL UNIQUE,
                password_hash TEXT    NOT NULL,
                created_at    TEXT    DEFAULT (datetime('now'))
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS assets (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                isin        TEXT,
                ticker      TEXT    NOT NULL,
                name        TEXT    NOT NULL,
                asset_type  TEXT    NOT NULL DEFAULT 'ETF',
                currency    TEXT    NOT NULL DEFAULT 'EUR',
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(user_id, ticker)
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id         INTEGER NOT NULL,
                asset_id        INTEGER NOT NULL,
                date            TEXT    NOT NULL,
                shares          REAL    NOT NULL,
                price_per_share REAL    NOT NULL,
                total_cost      REAL    NOT NULL,
                fees            REAL    DEFAULT 0,
                notes           TEXT    DEFAULT '',
                created_at      TEXT    DEFAULT (datetime('now')),
                FOREIGN KEY (user_id)  REFERENCES users(id)   ON DELETE CASCADE,
                FOREIGN KEY (asset_id) REFERENCES assets(id)  ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS sales (
                id                INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id           INTEGER NOT NULL,
                asset_id          INTEGER NOT NULL,
                date              TEXT    NOT NULL,
                shares            REAL    NOT NULL,
                price_per_share   REAL    NOT NULL,
                total_proceeds    REAL    NOT NULL,
                fees              REAL    DEFAULT 0,
                notes             TEXT    DEFAULT '',
                created_at        TEXT    DEFAULT (datetime('now')),
                FOREIGN KEY (user_id)  REFERENCES users(id)  ON DELETE CASCADE,
                FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS dca_goals (
                id             INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id        INTEGER NOT NULL UNIQUE,
                monthly_target REAL    NOT NULL DEFAULT 0,
                updated_at     TEXT    DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS dividends (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id     INTEGER NOT NULL,
                asset_id    INTEGER NOT NULL,
                date        TEXT    NOT NULL,
                amount      REAL    NOT NULL,
                notes       TEXT    DEFAULT '',
                created_at  TEXT    DEFAULT (datetime('now')),
                FOREIGN KEY (user_id)  REFERENCES users(id)  ON DELETE CASCADE,
                FOREIGN KEY (asset_id) REFERENCES assets(id) ON DELETE CASCADE
            )
        ''')
        c.execute('''
            CREATE TABLE IF NOT EXISTS email_tokens (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER NOT NULL,
                token      TEXT    NOT NULL UNIQUE,
                created_at TEXT    DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
            )
        ''')

    conn.commit()
    conn.close()
    print("Base de données initialisée.")

def fetchall_as_dict(cursor):
    if is_postgres():
        cols = [desc[0] for desc in cursor.description]
        return [dict(zip(cols, row)) for row in cursor.fetchall()]
    else:
        return cursor.fetchall()

def fetchone_as_dict(cursor):
    if is_postgres():
        cols = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        return dict(zip(cols, row)) if row else None
    else:
        return cursor.fetchone()

def create_user(email, password_hash):
    p = placeholder()
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute(f'INSERT INTO users (email, password_hash) VALUES ({p}, {p})', (email, password_hash))
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        conn.close()

def get_user_by_email(email):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT * FROM users WHERE email = {p}', (email,))
    row = fetchone_as_dict(c)
    conn.close()
    return row

def get_user_by_id(user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT * FROM users WHERE id = {p}', (user_id,))
    row = fetchone_as_dict(c)
    conn.close()
    return row

def add_asset(user_id, ticker, name, asset_type, currency, isin=''):
    p = placeholder()
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute(
            f'''INSERT INTO assets (user_id, isin, ticker, name, asset_type, currency)
               VALUES ({p}, {p}, {p}, {p}, {p}, {p})''',
            (user_id, isin.upper() if isin else '', ticker.upper(), name, asset_type, currency.upper())
        )
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        conn.close()

def get_user_assets(user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT * FROM assets WHERE user_id = {p} ORDER BY asset_type, name ASC', (user_id,))
    rows = fetchall_as_dict(c)
    conn.close()
    return rows

def get_asset_by_id(asset_id, user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT * FROM assets WHERE id = {p} AND user_id = {p}', (asset_id, user_id))
    row = fetchone_as_dict(c)
    conn.close()
    return row

def delete_asset(asset_id, user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'DELETE FROM assets WHERE id = {p} AND user_id = {p}', (asset_id, user_id))
    conn.commit()
    conn.close()

def add_purchase(user_id, asset_id, date, shares, price_per_share, fees=0, notes=''):
    p = placeholder()
    total_cost = round(shares * price_per_share, 4)
    conn = get_db()
    c = conn.cursor()
    c.execute(
        f'''INSERT INTO purchases
           (user_id, asset_id, date, shares, price_per_share, total_cost, fees, notes)
           VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})''',
        (user_id, asset_id, date, shares, price_per_share, total_cost, fees, notes)
    )
    conn.commit()
    conn.close()

def add_purchases_bulk(user_id, rows):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    for r in rows:
        total_cost = round(r['shares'] * r['price_per_share'], 4)
        c.execute(
            f'''INSERT INTO purchases
               (user_id, asset_id, date, shares, price_per_share, total_cost, fees, notes)
               VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})''',
            (user_id, r['asset_id'], r['date'], r['shares'],
             r['price_per_share'], total_cost, r.get('fees', 0), r.get('notes', ''))
        )
    conn.commit()
    conn.close()

def get_purchases_by_asset(asset_id, user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(
        f'''SELECT p.*, a.ticker, a.name, a.currency, a.asset_type
           FROM purchases p
           JOIN assets a ON p.asset_id = a.id
           WHERE p.asset_id = {p} AND p.user_id = {p}
           ORDER BY p.date ASC''',
        (asset_id, user_id)
    )
    rows = fetchall_as_dict(c)
    conn.close()
    return rows

def get_all_purchases(user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(
        f'''SELECT p.*, a.ticker, a.name, a.currency, a.isin, a.asset_type
           FROM purchases p
           JOIN assets a ON p.asset_id = a.id
           WHERE p.user_id = {p}
           ORDER BY p.date DESC''',
        (user_id,)
    )
    rows = fetchall_as_dict(c)
    conn.close()
    return rows

def get_purchase_by_id(purchase_id, user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(
        f'''SELECT p.*, a.ticker, a.currency
           FROM purchases p
           JOIN assets a ON p.asset_id = a.id
           WHERE p.id = {p} AND p.user_id = {p}''',
        (purchase_id, user_id)
    )
    row = fetchone_as_dict(c)
    conn.close()
    return row

def update_purchase(purchase_id, user_id, date, shares, price_per_share, fees=0, notes=''):
    p = placeholder()
    total_cost = round(shares * price_per_share, 4)
    conn = get_db()
    c = conn.cursor()
    c.execute(
        f'''UPDATE purchases
           SET date={p}, shares={p}, price_per_share={p}, total_cost={p}, fees={p}, notes={p}
           WHERE id={p} AND user_id={p}''',
        (date, shares, price_per_share, total_cost, fees, notes, purchase_id, user_id)
    )
    conn.commit()
    conn.close()

def delete_purchase(purchase_id, user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'DELETE FROM purchases WHERE id = {p} AND user_id = {p}', (purchase_id, user_id))
    conn.commit()
    conn.close()

def add_sale(user_id, asset_id, date, shares, price_per_share, fees=0, notes=''):
    p = placeholder()
    total_proceeds = round(shares * price_per_share, 4)
    conn = get_db()
    c = conn.cursor()
    c.execute(
        f'''INSERT INTO sales
           (user_id, asset_id, date, shares, price_per_share, total_proceeds, fees, notes)
           VALUES ({p}, {p}, {p}, {p}, {p}, {p}, {p}, {p})''',
        (user_id, asset_id, date, shares, price_per_share, total_proceeds, fees, notes)
    )
    conn.commit()
    conn.close()

def get_sales_by_asset(asset_id, user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(
        f'''SELECT s.*, a.ticker, a.name, a.currency
           FROM sales s
           JOIN assets a ON s.asset_id = a.id
           WHERE s.asset_id = {p} AND s.user_id = {p}
           ORDER BY s.date ASC''',
        (asset_id, user_id)
    )
    rows = fetchall_as_dict(c)
    conn.close()
    return rows

def get_all_sales(user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(
        f'''SELECT s.*, a.ticker, a.name, a.currency, a.asset_type
           FROM sales s
           JOIN assets a ON s.asset_id = a.id
           WHERE s.user_id = {p}
           ORDER BY s.date DESC''',
        (user_id,)
    )
    rows = fetchall_as_dict(c)
    conn.close()
    return rows

def get_sale_by_id(sale_id, user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT * FROM sales WHERE id = {p} AND user_id = {p}', (sale_id, user_id))
    row = fetchone_as_dict(c)
    conn.close()
    return row

def delete_sale(sale_id, user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'DELETE FROM sales WHERE id = {p} AND user_id = {p}', (sale_id, user_id))
    conn.commit()
    conn.close()

def set_dca_goal(user_id, monthly_target):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    if is_postgres():
        c.execute(
            f'''INSERT INTO dca_goals (user_id, monthly_target)
               VALUES ({p}, {p})
               ON CONFLICT(user_id) DO UPDATE SET
                   monthly_target = EXCLUDED.monthly_target,
                   updated_at = NOW()''',
            (user_id, monthly_target)
        )
    else:
        c.execute(
            f'''INSERT INTO dca_goals (user_id, monthly_target)
               VALUES ({p}, {p})
               ON CONFLICT(user_id) DO UPDATE SET
                   monthly_target = excluded.monthly_target,
                   updated_at = datetime('now')''',
            (user_id, monthly_target)
        )
    conn.commit()
    conn.close()

def get_dca_goal(user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT * FROM dca_goals WHERE user_id = {p}', (user_id,))
    row = fetchone_as_dict(c)
    conn.close()
    return row

def add_dividend(user_id, asset_id, date, amount, notes=''):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(
        f'''INSERT INTO dividends (user_id, asset_id, date, amount, notes)
           VALUES ({p}, {p}, {p}, {p}, {p})''',
        (user_id, asset_id, date, round(amount, 4), notes)
    )
    conn.commit()
    conn.close()

def get_dividends_by_asset(asset_id, user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(
        f'''SELECT d.*, a.ticker, a.name, a.currency
           FROM dividends d
           JOIN assets a ON d.asset_id = a.id
           WHERE d.asset_id = {p} AND d.user_id = {p}
           ORDER BY d.date ASC''',
        (asset_id, user_id)
    )
    rows = fetchall_as_dict(c)
    conn.close()
    return rows

def get_all_dividends(user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(
        f'''SELECT d.*, a.ticker, a.name, a.currency, a.asset_type
           FROM dividends d
           JOIN assets a ON d.asset_id = a.id
           WHERE d.user_id = {p}
           ORDER BY d.date DESC''',
        (user_id,)
    )
    rows = fetchall_as_dict(c)
    conn.close()
    return rows

def delete_dividend(dividend_id, user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'DELETE FROM dividends WHERE id = {p} AND user_id = {p}', (dividend_id, user_id))
    conn.commit()
    conn.close()

def get_total_dividends(user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT COALESCE(SUM(amount), 0) as total FROM dividends WHERE user_id = {p}', (user_id,))
    row = fetchone_as_dict(c)
    conn.close()
    return round(row['total'], 2)

def update_user_email(user_id, new_email):
    p = placeholder()
    conn = get_db()
    try:
        c = conn.cursor()
        c.execute(f'UPDATE users SET email = {p} WHERE id = {p}', (new_email, user_id))
        conn.commit()
        return True
    except Exception:
        conn.rollback()
        return False
    finally:
        conn.close()

def update_user_password(user_id, new_password_hash):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'UPDATE users SET password_hash = {p} WHERE id = {p}', (new_password_hash, user_id))
    conn.commit()
    conn.close()

def create_email_token(user_id, token):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'DELETE FROM email_tokens WHERE user_id = {p}', (user_id,))
    c.execute(f'INSERT INTO email_tokens (user_id, token) VALUES ({p}, {p})', (user_id, token))
    conn.commit()
    conn.close()

def get_email_token(token):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'SELECT * FROM email_tokens WHERE token = {p}', (token,))
    row = fetchone_as_dict(c)
    conn.close()
    return row

def verify_user_email(user_id):
    p = placeholder()
    conn = get_db()
    c = conn.cursor()
    c.execute(f'UPDATE users SET email_verified = TRUE WHERE id = {p}', (user_id,))
    c.execute(f'DELETE FROM email_tokens WHERE user_id = {p}', (user_id,))
    conn.commit()
    conn.close()

def export_purchases_csv(user_id):
    purchases = get_all_purchases(user_id)
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Date', 'Ticker', 'Nom', 'Type', 'ISIN', 'Parts',
                     'Prix/part', 'Total investi', 'Frais', 'Notes'])
    for p in purchases:
        writer.writerow([
            p['date'], p['ticker'], p['name'], p['asset_type'],
            p['isin'], p['shares'], p['price_per_share'],
            p['total_cost'], p['fees'] or 0, p['notes'] or ''
        ])
    return output.getvalue()

def generate_csv_template():
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    import io as _io

    wb = Workbook()
    ws = wb.active
    ws.title = "Import Achats"

    headers = ['ticker', 'date', 'shares', 'price_per_share', 'fees', 'notes']
    header_fill = PatternFill("solid", fgColor="5B5FED")
    header_font = Font(bold=True, color="FFFFFF")

    for col, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=col, value=h)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal='center')

    examples = [
        ['ESE.PA',   '17/01/2025', 4, 28.982, 0, 'Achat DCA'],
        ['PAEEM.PA', '04/02/2025', 2, 15.50,  0, 'Achat DCA'],
    ]
    for row_data in examples:
        ws.append(row_data)

    ws.column_dimensions['A'].width = 14
    ws.column_dimensions['B'].width = 14
    ws.column_dimensions['C'].width = 10
    ws.column_dimensions['D'].width = 16
    ws.column_dimensions['E'].width = 8
    ws.column_dimensions['F'].width = 24

    output = _io.BytesIO()
    wb.save(output)
    output.seek(0)
    return output.getvalue()

def import_purchases_csv(user_id, file_bytes, filename):
    assets     = get_user_assets(user_id)
    ticker_map = {a['ticker'].upper(): a['id'] for a in assets}
    imported   = 0
    errors     = []
    rows_raw   = []

    if filename.endswith('.xlsx'):
        from openpyxl import load_workbook
        import io as _io
        wb      = load_workbook(filename=_io.BytesIO(file_bytes))
        ws      = wb.active
        headers = [
            str(cell.value).strip().lower() if cell.value is not None else ''
            for cell in ws[1]
        ]
        for row in ws.iter_rows(min_row=2, values_only=True):
            if any(v is not None for v in row):
                rows_raw.append(dict(zip(headers, row)))
    else:
        content   = file_bytes.decode('utf-8-sig')
        sample    = content[:1024]
        delimiter = ';' if sample.count(';') > sample.count(',') else ','
        reader    = csv.DictReader(io.StringIO(content), delimiter=delimiter)
        for row in reader:
            rows_raw.append({k.strip().lower(): v for k, v in row.items() if k is not None})

    required = {'ticker', 'date', 'shares', 'price_per_share'}

    for i, row in enumerate(rows_raw, start=2):
        missing = required - set(row.keys())
        if missing:
            errors.append(f'Ligne {i} : colonnes manquantes {missing}')
            continue

        ticker = str(row['ticker']).strip().upper()
        if ticker not in ticker_map:
            errors.append(f'Ligne {i} : ticker "{ticker}" introuvable — ajoute-le d\'abord.')
            continue

        try:
            date_raw = str(row['date']).strip()
            if '/' in date_raw:
                parts = date_raw.split('/')
                date  = f'{parts[2]}-{parts[1].zfill(2)}-{parts[0].zfill(2)}'
            else:
                date = date_raw[:10]

            def clean_num(val):
                return str(val).replace('€', '').replace(' ', '').replace(',', '.').strip()

            shares          = float(clean_num(row['shares']))
            price_per_share = float(clean_num(row['price_per_share']))
            fees            = float(clean_num(row.get('fees') or 0))
            notes           = str(row.get('notes') or '').strip()

            if shares <= 0 or price_per_share <= 0:
                errors.append(f'Ligne {i} : parts ou prix invalide.')
                continue

            add_purchase(user_id, ticker_map[ticker], date,
                         shares, price_per_share, fees, notes)
            imported += 1

        except (ValueError, KeyError) as e:
            errors.append(f'Ligne {i} : format incorrect ({e})')

    return imported, errors