from flask import Flask, render_template, request, redirect, url_for, flash, Response
from flask_login import LoginManager, login_required, current_user
from config import Config
from database import (
    init_db,
    add_asset, get_user_assets, get_asset_by_id, delete_asset,
    add_purchase, add_purchases_bulk, get_all_purchases,
    get_purchase_by_id, update_purchase, delete_purchase,
    add_sale, get_all_sales, get_sale_by_id, delete_sale,
    set_dca_goal, export_purchases_csv,
    generate_csv_template, import_purchases_csv,
    add_dividend, get_all_dividends, delete_dividend,
    update_user_email, update_user_password
)
from portfolio import (
    get_portfolio_summary, get_chart_data, get_current_price,
    get_portfolio_chart_data, get_benchmark_curve
)
from auth import auth_bp, get_user_object
import plotly.graph_objects as go
import plotly.utils
import json

# ── Init ──────────────────────────────────────────────────────────────────────
app = Flask(__name__)
app.config.from_object(Config)

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'auth.login'
login_manager.login_message = 'Connecte-toi pour accéder à ton portefeuille.'

@login_manager.user_loader
def load_user(user_id):
    return get_user_object(user_id)

app.register_blueprint(auth_bp)

from flask import Blueprint
main_bp = Blueprint('main', __name__)

ASSET_TYPES = ['ETF', 'Action', 'Crypto', 'Obligation', 'Autre']

# ── Dashboard ─────────────────────────────────────────────────────────────────
@main_bp.route('/')
def dashboard():
    if not current_user.is_authenticated:
        return render_template('landing.html')
    summary = get_portfolio_summary(current_user.id)
    return render_template('dashboard.html', summary=summary)

# ── Landing ───────────────────────────────────────────────────────────────────
@main_bp.route('/landing')
def landing():
    if current_user.is_authenticated:
        return redirect(url_for('main.dashboard'))
    return render_template('landing.html')

# ── Graphiques ────────────────────────────────────────────────────────────────
@main_bp.route('/charts')
@login_required
def charts():
    from database import get_purchases_by_asset, get_sales_by_asset
    assets = get_user_assets(current_user.id)
    charts = {}

    # Graphique global portefeuille
    dates, invested, market = get_portfolio_chart_data(current_user.id)
    if dates:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates, y=invested,
            name='Total investi (€)',
            line=dict(color='#6B7280', dash='dot', width=2),
            fill='tozeroy', fillcolor='rgba(107,114,128,0.08)'
        ))
        fig.add_trace(go.Scatter(
            x=dates, y=market,
            name='Valeur marché totale (€)',
            line=dict(color='#5B5FED', width=2.5),
            fill='tozeroy', fillcolor='rgba(91,95,237,0.12)'
        ))
        from database import get_all_purchases
        all_p = get_all_purchases(current_user.id)
        b_dates, b_values = get_benchmark_curve(dates[0], dates[-1],
                                                 purchases=[dict(p) for p in all_p])
        if b_dates and b_values:
            b_scaled = b_values
            fig.add_trace(go.Scatter(
                x=b_dates, y=b_scaled,
                name='MSCI World (base €)',
                line=dict(color='#F6C90E', width=1.8, dash='dot'),
            ))
        fig.update_layout(
            title='Portefeuille total — Investi vs Marché vs MSCI World',
            template='plotly_white', height=380,
            margin=dict(l=20, r=20, t=40, b=20),
            legend=dict(orientation='h', y=-0.15),
            hovermode='x unified'
        )
        portfolio_chart = json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
    else:
        portfolio_chart = None

    # Graphiques par actif
    for asset in assets:
        purchases = get_purchases_by_asset(asset['id'], current_user.id)
        sales     = get_sales_by_asset(asset['id'], current_user.id)
        dates, invested, market = get_chart_data(asset, purchases, sales)
        if not dates:
            continue
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=dates, y=invested,
            name='Investi (€)',
            line=dict(color='#6B7280', dash='dot', width=2),
            fill='tozeroy', fillcolor='rgba(107,114,128,0.08)'
        ))
        fig.add_trace(go.Scatter(
            x=dates, y=market,
            name='Valeur marché (€)',
            line=dict(color='#5B5FED', width=2.5),
            fill='tozeroy', fillcolor='rgba(91,95,237,0.12)'
        ))
        b_dates, b_values = get_benchmark_curve(dates[0], dates[-1],
                                                 purchases=[dict(p) for p in purchases])
        if b_dates and b_values:
            b_scaled = b_values
            fig.add_trace(go.Scatter(
                x=b_dates, y=b_scaled,
                name='MSCI World (base €)',
                line=dict(color='#F6C90E', width=1.8, dash='dot'),
            ))
        fig.update_layout(
            title=f'{asset["name"]} ({asset["ticker"]}) — Investi vs Marché vs MSCI World',
            template='plotly_white', height=360,
            margin=dict(l=20, r=20, t=40, b=20),
            legend=dict(orientation='h', y=-0.15),
            hovermode='x unified'
        )
        charts[asset['id']] = {
            'asset': dict(asset),
            'plot':  json.dumps(fig, cls=plotly.utils.PlotlyJSONEncoder)
        }

    return render_template('charts.html', charts=charts, portfolio_chart=portfolio_chart)

# ── Ajouter un actif ──────────────────────────────────────────────────────────
@main_bp.route('/assets/add', methods=['GET', 'POST'])
@login_required
def add_asset_route():
    if request.method == 'POST':
        ticker     = request.form['ticker'].strip().upper()
        name       = request.form['name'].strip()
        asset_type = request.form['asset_type']
        currency   = request.form['currency'].strip().upper()
        isin       = request.form.get('isin', '').strip().upper()

        price = get_current_price(ticker)
        if price is None:
            flash(f'Ticker "{ticker}" introuvable sur Yahoo Finance.', 'error')
            return redirect(url_for('main.add_asset_route'))

        success = add_asset(current_user.id, ticker, name, asset_type, currency, isin)
        if not success:
            flash('Ce ticker est déjà dans ton portefeuille.', 'error')
            return redirect(url_for('main.add_asset_route'))

        flash(f'{name} ajouté ✓ (prix actuel : {price} {currency})', 'success')
        return redirect(url_for('main.dashboard'))

    return render_template('add_asset.html', asset_types=ASSET_TYPES)

# ── Supprimer un actif ────────────────────────────────────────────────────────
@main_bp.route('/assets/delete/<int:asset_id>', methods=['POST'])
@login_required
def delete_asset_route(asset_id):
    asset = get_asset_by_id(asset_id, current_user.id)
    if not asset:
        flash('Actif introuvable.', 'error')
        return redirect(url_for('main.dashboard'))
    delete_asset(asset_id, current_user.id)
    flash(f'{asset["name"]} supprimé.', 'success')
    return redirect(url_for('main.dashboard'))

# ── Ajouter achats (saisie multiple) ──────────────────────────────────────────
@main_bp.route('/purchases/add', methods=['GET', 'POST'])
@login_required
def add_purchase_route():
    assets = get_user_assets(current_user.id)
    if not assets:
        flash('Ajoute d\'abord un actif à ton portefeuille.', 'error')
        return redirect(url_for('main.add_asset_route'))

    if request.method == 'POST':
        rows   = []
        errors = []
        i      = 0
        while f'asset_id_{i}' in request.form:
            try:
                asset_id        = int(request.form[f'asset_id_{i}'])
                date            = request.form[f'date_{i}']
                shares          = float(request.form[f'shares_{i}'])
                price_per_share = float(request.form[f'price_per_share_{i}'])
                fees            = float(request.form.get(f'fees_{i}', 0) or 0)
                notes           = request.form.get(f'notes_{i}', '')

                if not date or shares <= 0 or price_per_share <= 0:
                    errors.append(f'Ligne {i+1} : données invalides.')
                else:
                    asset = get_asset_by_id(asset_id, current_user.id)
                    if not asset:
                        errors.append(f'Ligne {i+1} : actif invalide.')
                    else:
                        rows.append({
                            'asset_id':        asset_id,
                            'date':            date,
                            'shares':          shares,
                            'price_per_share': price_per_share,
                            'fees':            fees,
                            'notes':           notes,
                        })
            except (ValueError, KeyError):
                errors.append(f'Ligne {i+1} : format incorrect.')
            i += 1

        if errors:
            for e in errors:
                flash(e, 'error')
            return redirect(url_for('main.add_purchase_route'))

        if rows:
            add_purchases_bulk(current_user.id, rows)
            flash(f'{len(rows)} achat(s) enregistré(s) ✓', 'success')

        return redirect(url_for('main.add_purchase_route'))

    return render_template('add_purchase.html', assets=[dict(a) for a in assets])

# ── Historique achats ─────────────────────────────────────────────────────────
@main_bp.route('/purchases')
@login_required
def purchases():
    all_p  = get_all_purchases(current_user.id)
    assets = get_user_assets(current_user.id)
    return render_template('purchases.html', purchases=all_p, assets=assets)

# ── Éditer un achat ───────────────────────────────────────────────────────────
@main_bp.route('/purchases/edit/<int:purchase_id>', methods=['GET', 'POST'])
@login_required
def edit_purchase(purchase_id):
    p      = get_purchase_by_id(purchase_id, current_user.id)
    assets = get_user_assets(current_user.id)
    if not p:
        flash('Achat introuvable.', 'error')
        return redirect(url_for('main.purchases'))
    if request.method == 'POST':
        date            = request.form['date']
        shares          = float(request.form['shares'])
        price_per_share = float(request.form['price_per_share'])
        fees            = float(request.form.get('fees', 0) or 0)
        notes           = request.form.get('notes', '')
        update_purchase(purchase_id, current_user.id, date, shares,
                        price_per_share, fees, notes)
        flash('Achat mis à jour ✓', 'success')
        return redirect(url_for('main.purchases'))
    return render_template('edit_purchase.html', purchase=p, assets=assets)

# ── Supprimer un achat ────────────────────────────────────────────────────────
@main_bp.route('/purchases/delete/<int:purchase_id>', methods=['POST'])
@login_required
def delete_purchase_route(purchase_id):
    p = get_purchase_by_id(purchase_id, current_user.id)
    if not p:
        flash('Achat introuvable.', 'error')
        return redirect(url_for('main.purchases'))
    delete_purchase(purchase_id, current_user.id)
    flash('Achat supprimé.', 'success')
    return redirect(url_for('main.purchases'))

# ── Ajouter une vente ─────────────────────────────────────────────────────────
@main_bp.route('/sales/add', methods=['GET', 'POST'])
@login_required
def add_sale_route():
    assets = get_user_assets(current_user.id)
    if not assets:
        flash('Aucun actif dans ton portefeuille.', 'error')
        return redirect(url_for('main.dashboard'))
    if request.method == 'POST':
        asset_id        = int(request.form['asset_id'])
        date            = request.form['date']
        shares          = float(request.form['shares'])
        price_per_share = float(request.form['price_per_share'])
        fees            = float(request.form.get('fees', 0) or 0)
        notes           = request.form.get('notes', '')
        asset = get_asset_by_id(asset_id, current_user.id)
        if not asset:
            flash('Actif invalide.', 'error')
            return redirect(url_for('main.add_sale_route'))
        add_sale(current_user.id, asset_id, date, shares, price_per_share, fees, notes)
        flash('Vente enregistrée ✓', 'success')
        return redirect(url_for('main.sales'))
    return render_template('add_sale.html', assets=[dict(a) for a in assets])

# ── Historique ventes ─────────────────────────────────────────────────────────
@main_bp.route('/sales')
@login_required
def sales():
    all_s  = get_all_sales(current_user.id)
    assets = get_user_assets(current_user.id)
    return render_template('sales.html', sales=all_s, assets=assets)

# ── Supprimer une vente ───────────────────────────────────────────────────────
@main_bp.route('/sales/delete/<int:sale_id>', methods=['POST'])
@login_required
def delete_sale_route(sale_id):
    s = get_sale_by_id(sale_id, current_user.id)
    if not s:
        flash('Vente introuvable.', 'error')
        return redirect(url_for('main.sales'))
    delete_sale(sale_id, current_user.id)
    flash('Vente supprimée.', 'success')
    return redirect(url_for('main.sales'))

# ── Dividendes ────────────────────────────────────────────────────────────────
@main_bp.route('/dividends/add', methods=['GET', 'POST'])
@login_required
def add_dividend_route():
    assets = get_user_assets(current_user.id)
    if not assets:
        flash('Aucun actif dans ton portefeuille.', 'error')
        return redirect(url_for('main.dashboard'))
    if request.method == 'POST':
        asset_id = int(request.form['asset_id'])
        date     = request.form['date']
        amount   = float(request.form['amount'])
        notes    = request.form.get('notes', '')
        asset    = get_asset_by_id(asset_id, current_user.id)
        if not asset:
            flash('Actif invalide.', 'error')
            return redirect(url_for('main.add_dividend_route'))
        add_dividend(current_user.id, asset_id, date, amount, notes)
        flash('Dividende enregistré ✓', 'success')
        return redirect(url_for('main.dividends'))
    return render_template('add_dividend.html', assets=[dict(a) for a in assets])

@main_bp.route('/dividends')
@login_required
def dividends():
    all_d = get_all_dividends(current_user.id)
    return render_template('dividends.html', dividends=all_d)

@main_bp.route('/dividends/delete/<int:dividend_id>', methods=['POST'])
@login_required
def delete_dividend_route(dividend_id):
    delete_dividend(dividend_id, current_user.id)
    flash('Dividende supprimé.', 'success')
    return redirect(url_for('main.dividends'))

# ── Objectif DCA ──────────────────────────────────────────────────────────────
@main_bp.route('/settings', methods=['GET', 'POST'])
@login_required
def settings():
    from database import get_dca_goal
    if request.method == 'POST':
        target = float(request.form.get('monthly_target', 0) or 0)
        set_dca_goal(current_user.id, target)
        flash('Objectif DCA mis à jour ✓', 'success')
        return redirect(url_for('main.settings'))
    goal = get_dca_goal(current_user.id)
    return render_template('settings.html', goal=goal)

# ── Export CSV ────────────────────────────────────────────────────────────────
@main_bp.route('/export/csv')
@login_required
def export_csv():
    csv_data = export_purchases_csv(current_user.id)
    return Response(
        csv_data,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=portefeuille.csv'}
    )

# ── Import Excel ──────────────────────────────────────────────────────────────
@main_bp.route('/import', methods=['GET', 'POST'])
@login_required
def import_csv():
    if request.method == 'POST':
        if 'csv_file' not in request.files:
            flash('Aucun fichier sélectionné.', 'error')
            return redirect(url_for('main.import_csv'))
        f = request.files['csv_file']
        if f.filename == '':
            flash('Aucun fichier sélectionné.', 'error')
            return redirect(url_for('main.import_csv'))
        if not f.filename.endswith('.xlsx'):
            flash('Le fichier doit être au format .xlsx', 'error')
            return redirect(url_for('main.import_csv'))
        file_bytes = f.read()
        imported, errors = import_purchases_csv(current_user.id, file_bytes, f.filename)
        if imported:
            flash(f'{imported} achat(s) importé(s) avec succès ✓', 'success')
        for e in errors:
            flash(e, 'error')
        return redirect(url_for('main.import_csv'))
    return render_template('import.html')

# ── Template Excel ────────────────────────────────────────────────────────────
@main_bp.route('/import/template')
@login_required
def download_template():
    xlsx_data = generate_csv_template()
    return Response(
        xlsx_data,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        headers={'Content-Disposition': 'attachment; filename=template_import.xlsx'}
    )
# ── Simulateur DCA ────────────────────────────────────────────────────────────
@main_bp.route('/simulateur')
def simulateur():
    return render_template('simulateur.html')
# ── Contact ───────────────────────────────────────────────────────────────────
@main_bp.route('/contact')
def contact():
    return render_template('contact.html')
# ── Profil ────────────────────────────────────────────────────────────────────
@main_bp.route('/profile', methods=['GET', 'POST'])
@login_required
def profile():
    from werkzeug.security import generate_password_hash, check_password_hash
    from database import update_user_email, update_user_password

    if request.method == 'POST':
        action = request.form.get('action')

        if action == 'email':
            new_email    = request.form.get('new_email', '').strip()
            confirm_pass = request.form.get('confirm_password', '')
            from auth import get_user_by_email
            from database import get_db
            conn = get_db()
            user = conn.execute(
                'SELECT * FROM users WHERE id = ?', (current_user.id,)
            ).fetchone()
            conn.close()
            if not check_password_hash(user['password_hash'], confirm_pass):
                flash('Mot de passe incorrect.', 'error')
            elif not new_email:
                flash('Email invalide.', 'error')
            else:
                success = update_user_email(current_user.id, new_email)
                if success:
                    flash('Email mis à jour ✓', 'success')
                else:
                    flash('Cet email est déjà utilisé.', 'error')

        elif action == 'password':
            current_pass = request.form.get('current_password', '')
            new_pass     = request.form.get('new_password', '')
            confirm_pass = request.form.get('confirm_new_password', '')
            from database import get_db
            conn = get_db()
            user = conn.execute(
                'SELECT * FROM users WHERE id = ?', (current_user.id,)
            ).fetchone()
            conn.close()
            if not check_password_hash(user['password_hash'], current_pass):
                flash('Mot de passe actuel incorrect.', 'error')
            elif len(new_pass) < 6:
                flash('Le nouveau mot de passe doit faire au moins 6 caractères.', 'error')
            elif new_pass != confirm_pass:
                flash('Les mots de passe ne correspondent pas.', 'error')
            else:
                update_user_password(current_user.id, generate_password_hash(new_pass))
                flash('Mot de passe mis à jour ✓', 'success')

    return render_template('profile.html')
# ── Enregistrement blueprint + lancement ──────────────────────────────────────
app.register_blueprint(main_bp)

if __name__ == '__main__':
    init_db()
    app.run(debug=True)