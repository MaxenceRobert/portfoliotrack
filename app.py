from dotenv import load_dotenv
load_dotenv()

import markdown as md_lib

from flask import Flask, render_template, request, redirect, url_for, flash, Response, jsonify, session
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
    update_user_email, update_user_password,
    save_profil_investisseur, get_last_profil_investisseur,
    get_cached_risk_score, save_risk_score,
    get_user_by_id, set_onboarding_completed,
    update_asset_envelope,
)
from portfolio import (
    get_portfolio_summary, get_chart_data, get_current_price,
    get_portfolio_chart_data, get_benchmark_curve,
    get_ticker_info, get_ticker_history
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

# ── Scoring de risque ─────────────────────────────────────────────────────────
def get_risk_score(ticker, asset_type='Autre'):
    """
    Score de risque 0-100 basé sur 5 métriques pondérées.

    Métriques et pondérations :
      - Volatilité annualisée    30%
      - Max Drawdown 3 ans       25%
      - Beta vs IWDA.AS (hebdo)  20%
      - Sharpe Ratio inversé     15%
      - VaR 95% annualisée       10%

    Cache DB 24h. Fallback sur score par défaut si données insuffisantes.
    """
    import datetime as _dt
    import re

    cached = get_cached_risk_score(ticker)
    if cached:
        return cached

    # ── Correspondances type d'actif ─────────────────────────────────────────
    TYPE_MAP = {
        'etf': 'ETF', 'equity': 'Action', 'action': 'Action',
        'cryptocurrency': 'Crypto', 'crypto': 'Crypto',
        'bond': 'Obligation', 'obligation': 'Obligation',
        'mutualfund': 'ETF', 'autre': 'Autre',
    }
    DEFAULT_BY_TYPE = {
        'ETF': 45, 'Action': 52, 'Crypto': 88,
        'Obligation': 20, 'Autre': 40,
    }
    # Tickers spéciaux sans données Yahoo Finance
    SPECIAL_TICKER_DEFAULTS = [
        (r'(?i)(livret[\._]?a|lep)', 5),
        (r'(?i)(fonds[\._]?euro)', 7),
        (r'(?i)(oblig[\._]?souverain|gov[\._]?bond)', 15),
        (r'(?i)(oblig[\._]?corp|investment[\._]?grade)', 22),
        (r'(?i)(high[\._]?yield)', 35),
        (r'(?i)(scpi|pierre[\._]?papier)', 28),
        (r'(?i)(immo[\._]?direct|bien[\._]?immo)', 25),
        (r'(?i)(private[\._]?equity|pe[\._]?fund)', 55),
        (r'(?i)(matieres?[\._]?prem|commodity)', 45),
    ]

    normalized_type = TYPE_MAP.get((asset_type or '').lower(), 'Autre')
    default_score   = DEFAULT_BY_TYPE.get(normalized_type, 40)

    for pattern, score in SPECIAL_TICKER_DEFAULTS:
        if re.search(pattern, ticker):
            default_score = score
            break

    # ── Interpolation linéaire par morceaux ──────────────────────────────────
    def piecewise(val, breakpoints):
        """breakpoints = [(x0, y0), (x1, y1), ...] triés par x croissant."""
        if val <= breakpoints[0][0]:
            return float(breakpoints[0][1])
        for i in range(len(breakpoints) - 1):
            x0, y0 = breakpoints[i]
            x1, y1 = breakpoints[i + 1]
            if x0 <= val <= x1:
                return y0 + (val - x0) / (x1 - x0) * (y1 - y0)
        # Extrapolation au-delà du dernier point, plafonnée à 100
        x0, y0 = breakpoints[-2]
        x1, y1 = breakpoints[-1]
        slope = (y1 - y0) / (x1 - x0) if x1 > x0 else 0.0
        return min(100.0, y1 + slope * (val - x1))

    # ── Tables de normalisation ───────────────────────────────────────────────
    # Volatilité annualisée (décimal, ex: 0.18 = 18%)
    # Paliers 30% et 50% légèrement relevés pour mieux discriminer les actifs
    # très volatils (crypto, ETF levier) vis-à-vis des actions ordinaires.
    VOL_BP = [
        (0.00,  0), (0.05, 10), (0.10, 25), (0.15, 40),
        (0.20, 55), (0.30, 75), (0.50, 88), (1.00, 100),
    ]
    # Max Drawdown (valeur absolue, décimal)
    DD_BP = [
        (0.00,  0), (0.05, 10), (0.15, 25), (0.25, 45),
        (0.40, 65), (0.60, 80), (1.00, 100),
    ]
    # Beta (>= 0) — beta < 0 traité séparément → 20 pts
    BETA_BP = [
        (0.0,  5), (0.3, 15), (0.7, 30), (1.0, 45),
        (1.3, 60), (1.8, 75), (2.5, 90), (5.0, 100),
    ]
    # Sharpe inversé : Sharpe élevé → score bas (calculé avec taux sans risque)
    SHARPE_BP = [
        (-3.0, 100), (0.0, 70), (0.5, 50),
        (1.0,  30),  (1.5, 15), (3.0,  0),
    ]
    # VaR 95% annualisée (décimal)
    VAR_BP = [
        (0.00,  0), (0.05, 15), (0.10, 30),
        (0.20, 55), (0.35, 75), (1.00, 100),
    ]
    # Taux sans risque annuel pour Sharpe (moyenne long terme)
    RF_ANNUAL = 0.035

    try:
        import yfinance as yf
        import numpy as np

        end      = _dt.date.today()
        start_3y = end - _dt.timedelta(days=3 * 365)
        start_1y = end - _dt.timedelta(days=365)

        # ── Télécharger historique (3 ans, fallback 1 an) ────────────────────
        hist = yf.download(
            ticker, start=start_3y.isoformat(), end=end.isoformat(),
            progress=False, auto_adjust=True,
        )

        if hist.empty or len(hist) < 60:
            raise ValueError(f"Données insuffisantes ({len(hist)} jours)")

        fallback_period = False
        if len(hist) < 126:  # moins de 6 mois sur 3 ans → fallback 1 an
            print(f"[risk_score] {ticker}: fallback 1 an ({len(hist)} jours disponibles)")
            hist = yf.download(
                ticker, start=start_1y.isoformat(), end=end.isoformat(),
                progress=False, auto_adjust=True,
            )
            if hist.empty or len(hist) < 60:
                raise ValueError("Données insuffisantes même sur 1 an")
            fallback_period = True

        closes        = hist['Close'].squeeze()
        daily_returns = closes.pct_change().dropna()
        n_days        = len(daily_returns)

        # ── 1. Volatilité annualisée (30%) ───────────────────────────────────
        vol       = float(daily_returns.std()) * (252 ** 0.5)
        vol_score = piecewise(vol, VOL_BP)

        # ── 2. Max Drawdown (25%) ────────────────────────────────────────────
        cum      = (1 + daily_returns).cumprod()
        roll_max = cum.cummax()
        dd       = float(((cum - roll_max) / roll_max).min())
        dd_abs   = abs(dd)
        dd_score = piecewise(dd_abs, DD_BP)

        # ── 3. Beta vs IWDA.AS – rendements hebdomadaires (20%) ─────────────
        beta     = 1.0
        vol_iwda = None
        try:
            mkt_start = start_1y if fallback_period else start_3y
            mkt_hist  = yf.download(
                'IWDA.AS', start=mkt_start.isoformat(), end=end.isoformat(),
                progress=False, auto_adjust=True,
            )
            if not mkt_hist.empty and len(mkt_hist) >= 50:
                mkt_closes     = mkt_hist['Close'].squeeze()
                mkt_daily_ret  = mkt_closes.pct_change().dropna()
                vol_iwda       = float(mkt_daily_ret.std()) * (252 ** 0.5)
                asset_weekly   = closes.resample('W').last().pct_change().dropna()
                market_weekly  = mkt_closes.resample('W').last().pct_change().dropna()
                common         = asset_weekly.index.intersection(market_weekly.index)
                if len(common) >= 30:
                    ar      = asset_weekly.loc[common].values
                    mr      = market_weekly.loc[common].values
                    cov     = float(np.cov(ar, mr)[0][1])
                    var_mkt = float(np.var(mr, ddof=1))
                    beta    = cov / var_mkt if var_mkt > 0 else 1.0
        except Exception as e_beta:
            print(f"[risk_score] {ticker}: beta fallback ({e_beta})")

        # Effective beta : pour les actifs à très haute volatilité standalone
        # (vol > 2.5× la vol du marché) et à beta de covariance positif,
        # on utilise un plancher basé sur le ratio de volatilité.
        # Cela corrige les assets comme BTC dont le beta de covariance est faible
        # (faible corrélation aux actions) mais dont le risque standalone est élevé.
        effective_beta = beta
        if (beta > 0 and vol_iwda and vol_iwda > 0):
            vol_ratio = vol / vol_iwda
            if vol_ratio > 2.5:
                effective_beta = max(beta, vol_ratio * 1.5)

        beta_score = 20.0 if effective_beta < 0 else piecewise(effective_beta, BETA_BP)

        # ── 4. Sharpe Ratio inversé (15% → réduit à 5%) ─────────────────────
        # Ajusté du taux sans risque pour corriger le biais des périodes bull.
        # Le poids réduit à 5% limite l'impact d'un bon Sharpe conjoncturel
        # sur le score global.
        annual_return = float((1 + daily_returns).prod() ** (252 / n_days) - 1)
        sharpe        = (annual_return - RF_ANNUAL) / vol if vol > 0 else 0.0
        sharpe_score  = piecewise(sharpe, SHARPE_BP)

        # ── 5. VaR 95% annualisée (10% → augmenté à 15%) ────────────────────
        daily_var_95  = float(-np.percentile(daily_returns.values, 5))
        var_95_annual = daily_var_95 * (252 ** 0.5)
        var_score     = piecewise(var_95_annual, VAR_BP)

        # ── Score final (pondérations : vol 35% | dd 25% | beta 20% | sharpe 5% | var 15%) ──
        score = round(
            vol_score    * 0.35 +
            dd_score     * 0.25 +
            beta_score   * 0.20 +
            sharpe_score * 0.05 +
            var_score    * 0.15
        )
        score = max(0, min(100, score))

        beta_display = f"{beta:.2f}" if effective_beta == beta else f"{beta:.2f}→eff={effective_beta:.2f}"
        print(
            f"[risk_score] {ticker}: score={score}/100 | "
            f"vol={vol*100:.1f}%→{vol_score:.1f}pts(×0.35) | "
            f"dd={dd_abs*100:.1f}%→{dd_score:.1f}pts(×0.25) | "
            f"beta={beta_display}→{beta_score:.1f}pts(×0.20) | "
            f"sharpe_rf={sharpe:.2f}→{sharpe_score:.1f}pts(×0.05) | "
            f"var95={var_95_annual*100:.1f}%→{var_score:.1f}pts(×0.15)"
        )

        result = {
            'score':      score,
            'volatilite': round(vol * 100, 2),
            'drawdown':   round(dd_abs * 100, 2),
            'beta':       round(beta, 3),
            'sharpe':     round(sharpe, 3),
            'var_95':     round(var_95_annual * 100, 2),
            'source':     'yahoo',
        }

    except Exception as e:
        print(f"[risk_score] {ticker}: erreur → {e} | fallback score={default_score}")
        result = {
            'score':      default_score,
            'volatilite': None,
            'drawdown':   None,
            'beta':       None,
            'sharpe':     None,
            'var_95':     None,
            'source':     'default',
        }

    save_risk_score(ticker, result)
    return result

# ── API Search Ticker ─────────────────────────────────────────────────────────
@app.route('/api/search-ticker')
def search_ticker():
    q = request.args.get('q', '').strip()
    if len(q) < 2:
        return jsonify({'results': []})
    try:
        import requests as req
        url = f'https://query1.finance.yahoo.com/v1/finance/search?q={q}&quotesCount=8&newsCount=0&listsCount=0'
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = req.get(url, headers=headers, timeout=5)
        data = r.json()
        quotes = data.get('quotes', [])
        results = []
        for q_item in quotes:
            symbol = q_item.get('symbol', '')
            name   = q_item.get('longname') or q_item.get('shortname') or symbol
            qtype  = q_item.get('quoteType', '')
            exch   = q_item.get('exchDisp') or q_item.get('exchange', '')
            curr   = q_item.get('currency', '')
            if not curr:
                if '.PA' in symbol or '.AS' in symbol or '.DE' in symbol:
                    curr = 'EUR'
                elif '-USD' in symbol:
                    curr = 'USD'
                else:
                    curr = 'USD'
            results.append({
                'ticker':   symbol,
                'name':     name,
                'type':     qtype,
                'exchange': exch,
                'currency': curr,
            })
        return jsonify({'results': results})
    except Exception as e:
        print(f"Search error: {e}")
        return jsonify({'results': []})

# ── Enveloppes fiscales — plafonds légaux ─────────────────────────────────────
ENVELOPE_PLAFONDS = {
    'PEA':       150000,
    'PEA-PME':   225000,
    'Livret A':  22950,
    'LDDS':      12000,
    'LEP':       10000,
}

# ── Dashboard ─────────────────────────────────────────────────────────────────
@main_bp.route('/')
def dashboard():
    if not current_user.is_authenticated:
        return render_template('landing.html')

    summary = get_portfolio_summary(current_user.id)

    # Score de risque par actif + score pondéré du portefeuille
    risk_data            = {}
    weighted_risk_sum    = 0.0
    total_risk_weight    = 0.0

    for assets_list in summary.get('by_type', {}).values():
        for a in assets_list:
            if not a.get('fully_sold', False):
                ticker = a.get('ticker', '')
                if ticker and ticker not in risk_data:
                    risk_data[ticker] = get_risk_score(ticker, a.get('asset_type', 'Autre'))
                val = float(a.get('current_value') or 0)
                if val > 0 and ticker in risk_data:
                    weighted_risk_sum += risk_data[ticker]['score'] * val
                    total_risk_weight += val

    portfolio_risk_score = (
        round(weighted_risk_sum / total_risk_weight) if total_risk_weight > 0 else None
    )

    # Cohérence avec profil investisseur
    last_profil       = get_last_profil_investisseur(current_user.id)
    profil_risk_score = last_profil.get('score_global') if last_profil else None

    coherence = None
    if portfolio_risk_score is not None and profil_risk_score is not None:
        coherence = max(0, 100 - abs(portfolio_risk_score - profil_risk_score))

    # Enveloppes fiscales — résumé par enveloppe
    envelope_summary = {}
    for assets_list in summary.get('by_type', {}).values():
        for a in assets_list:
            if not a.get('fully_sold', False):
                env = a.get('envelope') or ''
                if not env:
                    continue
                if env not in envelope_summary:
                    envelope_summary[env] = {
                        'count':           0,
                        'total_invested':  0.0,
                        'total_value':     0.0,
                        'plafond':         ENVELOPE_PLAFONDS.get(env),
                    }
                envelope_summary[env]['count']          += 1
                envelope_summary[env]['total_invested'] += float(a.get('total_invested') or 0)
                envelope_summary[env]['total_value']    += float(a.get('current_value') or 0)

    # Arrondir les totaux
    for env_data in envelope_summary.values():
        env_data['total_invested'] = round(env_data['total_invested'], 2)
        env_data['total_value']    = round(env_data['total_value'], 2)
        if env_data['plafond']:
            env_data['plafond_restant'] = max(0, env_data['plafond'] - env_data['total_invested'])
        else:
            env_data['plafond_restant'] = None

    # all_assets_json — pour le filtre client-side par enveloppe
    from database import get_all_purchases as _all_purch
    import datetime as _dt2
    _this_month = _dt2.date.today().strftime('%Y-%m')
    monthly_by_asset = {}
    for _p in _all_purch(current_user.id):
        if str(_p['date']).startswith(_this_month):
            _aid = _p['asset_id']
            monthly_by_asset[_aid] = monthly_by_asset.get(_aid, 0) + float(_p['total_cost'])

    all_assets_json = []
    for assets_list in summary.get('by_type', {}).values():
        for a in assets_list:
            if not a.get('fully_sold', False):
                ri = risk_data.get(a.get('ticker', '')) or {}
                all_assets_json.append({
                    'asset_id':           a['asset_id'],
                    'ticker':             a['ticker'],
                    'name':               a['name'],
                    'asset_type':         a['asset_type'],
                    'envelope':           a.get('envelope') or '',
                    'current_value':      float(a.get('current_value') or 0),
                    'total_invested':     float(a.get('total_invested') or 0),
                    'unrealized_gain':    float(a.get('unrealized_gain') or 0),
                    'realized_gain':      float(a.get('realized_gain') or 0),
                    'dividends_received': float(a.get('dividends_received') or 0),
                    'risk_score':         ri.get('score'),
                    'invested_this_month': round(monthly_by_asset.get(a['asset_id'], 0), 2),
                })

    # Onboarding
    user_db              = get_user_by_id(current_user.id)
    onboarding_completed = bool(user_db.get('onboarding_completed')) if user_db else False
    has_assets           = bool(summary.get('assets'))
    has_profil           = bool(last_profil)
    onboarding_state     = {
        'show':       not onboarding_completed,
        'has_profil': has_profil,
        'has_assets': has_assets,
        'all_done':   has_profil and has_assets,
    }

    return render_template(
        'dashboard.html',
        summary=summary,
        risk_data=risk_data,
        portfolio_risk_score=portfolio_risk_score,
        profil_risk_score=profil_risk_score,
        coherence=coherence,
        last_profil=last_profil,
        envelope_summary=envelope_summary,
        onboarding_state=onboarding_state,
        all_assets_json=all_assets_json,
    )

# ── Mise à jour enveloppe d'un actif (AJAX) ───────────────────────────────────
@main_bp.route('/asset/update-envelope', methods=['POST'])
@login_required
def update_envelope_route():
    data       = request.get_json(silent=True) or {}
    asset_id   = data.get('asset_id')
    envelope   = data.get('envelope', '')
    if not asset_id:
        return jsonify({'success': False, 'error': 'asset_id manquant'})
    asset = get_asset_by_id(int(asset_id), current_user.id)
    if not asset:
        return jsonify({'success': False, 'error': 'Actif introuvable'})
    update_asset_envelope(int(asset_id), current_user.id, envelope)
    return jsonify({'success': True})

# ── Onboarding dismiss ────────────────────────────────────────────────────────
@main_bp.route('/onboarding/dismiss', methods=['POST'])
@login_required
def onboarding_dismiss():
    set_onboarding_completed(current_user.id)
    return redirect(url_for('main.dashboard'))

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
            fig.add_trace(go.Scatter(
                x=b_dates, y=b_values,
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
            fig.add_trace(go.Scatter(
                x=b_dates, y=b_values,
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
        envelope   = request.form.get('envelope', '').strip()

        price = get_current_price(ticker)
        if price is None:
            flash(f'Ticker "{ticker}" introuvable sur Yahoo Finance.', 'error')
            return redirect(url_for('main.add_asset_route'))

        success = add_asset(current_user.id, ticker, name, asset_type, currency, isin, envelope)
        if not success:
            flash('Ce ticker est déjà dans ton portefeuille.', 'error')
            return redirect(url_for('main.add_asset_route'))

        flash(f'{name} ajouté ✓ (prix actuel : {price} {currency})', 'success')
        return redirect(url_for('main.dashboard'))

    ticker_prefill     = request.args.get('ticker', '')
    name_prefill       = request.args.get('name', '')
    currency_prefill   = request.args.get('currency', '')
    asset_type_prefill = request.args.get('asset_type', '')

    return render_template('add_asset.html', asset_types=ASSET_TYPES,
                           ticker_prefill=ticker_prefill,
                           name_prefill=name_prefill,
                           currency_prefill=currency_prefill,
                           asset_type_prefill=asset_type_prefill)

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

# ── Ajouter achats ────────────────────────────────────────────────────────────
@main_bp.route('/purchases/add', methods=['GET', 'POST'])
@login_required
def add_purchase_route():
    assets = get_user_assets(current_user.id)

    if request.method == 'POST':
        rows   = []
        errors = []
        i      = 0

        while f'ticker_{i}' in request.form or f'asset_id_{i}' in request.form:
            try:
                ticker     = request.form.get(f'ticker_{i}', '').strip().upper()
                asset_name = request.form.get(f'asset_name_{i}', '').strip()
                currency   = request.form.get(f'asset_currency_{i}', 'EUR').strip().upper()
                asset_type = request.form.get(f'asset_type_{i}', 'Autre').strip()
                asset_id   = request.form.get(f'asset_id_{i}', '').strip()
                envelope   = request.form.get(f'envelope_{i}', '').strip()
                date            = request.form.get(f'date_{i}', '')
                shares          = float(request.form.get(f'shares_{i}', 0))
                price_per_share = float(request.form.get(f'price_per_share_{i}', 0))
                fees            = float(request.form.get(f'fees_{i}', 0) or 0)
                notes           = request.form.get(f'notes_{i}', '')

                if not ticker:
                    errors.append(f'Ligne {i+1} : aucun actif sélectionné.')
                    i += 1
                    continue

                if not date or shares <= 0 or price_per_share <= 0:
                    errors.append(f'Ligne {i+1} : données invalides.')
                    i += 1
                    continue

                if not asset_id:
                    price = get_current_price(ticker)
                    if price is None:
                        errors.append(f'Ligne {i+1} : ticker "{ticker}" introuvable sur Yahoo Finance.')
                        i += 1
                        continue

                    type_map = {'ETF': 'ETF', 'EQUITY': 'Action',
                                'CRYPTOCURRENCY': 'Crypto', 'Action': 'Action',
                                'Crypto': 'Crypto', 'Autre': 'Autre'}
                    mapped_type = type_map.get(asset_type, 'Autre')

                    add_asset(current_user.id, ticker,
                              asset_name or ticker, mapped_type,
                              currency or 'EUR', '', envelope)

                    from database import get_user_assets as _get_assets
                    all_assets = _get_assets(current_user.id)
                    found = next((a for a in all_assets if a['ticker'] == ticker), None)
                    if not found:
                        errors.append(f'Ligne {i+1} : impossible de créer l\'actif "{ticker}".')
                        i += 1
                        continue
                    asset_id = found['id']
                else:
                    asset_id = int(asset_id)
                    asset = get_asset_by_id(asset_id, current_user.id)
                    if not asset:
                        errors.append(f'Ligne {i+1} : actif invalide.')
                        i += 1
                        continue

                rows.append({
                    'asset_id':        asset_id,
                    'date':            date,
                    'shares':          shares,
                    'price_per_share': price_per_share,
                    'fees':            fees,
                    'notes':           notes,
                })

            except (ValueError, KeyError) as e:
                errors.append(f'Ligne {i+1} : format incorrect ({e}).')
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

    price_cache = {}
    enriched = []
    for p in all_p:
        p = dict(p)
        ticker = p.get('ticker')
        if ticker not in price_cache:
            price_cache[ticker] = get_current_price(ticker)
        p['current_price'] = price_cache[ticker]
        enriched.append(p)

    return render_template('purchases.html', purchases=enriched, assets=assets)

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

# ── Explorer ──────────────────────────────────────────────────────────────────
@main_bp.route('/explorer')
def explorer():
    ticker = request.args.get('ticker', '').strip().upper()
    period = request.args.get('period', '1y')
    info      = None
    error     = None
    risk_info = None

    if ticker:
        if period != '1y':
            info = get_ticker_info(ticker)
            if info:
                dates, closes = get_ticker_history(ticker, period)
                info['dates']  = dates
                info['closes'] = closes
        else:
            info = get_ticker_info(ticker)

        if not info:
            error = f'Ticker "{ticker}" introuvable sur Yahoo Finance.'
        else:
            risk_info = get_risk_score(ticker, info.get('asset_type', 'Autre'))

    return render_template('explorer.html', ticker=ticker, info=info,
                           error=error, period=period, risk_info=risk_info)

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
            from database import get_db
            conn = get_db()
            c = conn.cursor()
            c.execute('SELECT * FROM users WHERE id = %s' if hasattr(conn, 'autocommit') else 'SELECT * FROM users WHERE id = ?', (current_user.id,))
            from database import fetchone_as_dict
            user = fetchone_as_dict(c)
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
            c = conn.cursor()
            c.execute('SELECT * FROM users WHERE id = %s' if hasattr(conn, 'autocommit') else 'SELECT * FROM users WHERE id = ?', (current_user.id,))
            from database import fetchone_as_dict
            user = fetchone_as_dict(c)
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

    last_profil = get_last_profil_investisseur(current_user.id)
    return render_template('profile.html', last_profil=last_profil)

# ── Test profil investisseur ──────────────────────────────────────────────────
@main_bp.route('/test-profil')
def test_profil():
    return render_template('test_profil.html')

@main_bp.route('/resultat-profil', methods=['POST'])
def resultat_profil():
    import os
    import requests as req

    answers = {}
    for i in range(1, 41):
        answers[i] = request.form.get(f'q{i}', '')
    answers_text = {}
    for i in range(1, 41):
        answers_text[i] = request.form.get(f'q{i}_text', '')

    DEFAULT = {'A': 0, 'B': 33, 'C': 66, 'D': 100}
    EXCEPTIONS = {
        5:  {'A': 0, 'B': 25, 'C': 50, 'D': 100},
        8:  {'A': 25, 'B': 50, 'C': 75, 'D': 100},
        13: {'A': 100, 'B': 80, 'C': 50, 'D': 25},
        19: {'A': 25, 'B': 25, 'C': 75, 'D': 100},
        26: {'A': 0, 'B': 50, 'C': 100, 'D': 75},
        27: {'A': 25, 'B': 75, 'C': 75, 'D': 100},
        33: {'A': 25, 'B': 0, 'C': 50, 'D': 100},
        34: {'A': 25, 'B': 0, 'C': 50, 'D': 100},
        36: {'A': 100, 'B': 25, 'C': 50, 'D': 0},
        38: {'A': 100, 'B': 75, 'C': 25, 'D': 0},
    }
    OPEN_QS = {22, 23, 28, 37}
    AXES = {
        1: [1, 2, 3, 4, 5],
        2: [6, 7, 8, 9, 10],
        3: [11, 12, 13, 14, 15],
        4: [16, 17, 18, 19, 20],
        5: [21, 22, 23, 24, 25],
        6: [26, 27, 28, 29, 30],
        7: [31, 32, 33, 34, 35],
        8: [36, 37, 38, 39, 40],
    }
    AXIS_WEIGHTS = {1: 0.20, 2: 0.20, 3: 0.15, 4: 0.10, 5: 0.05, 6: 0.15, 7: 0.10, 8: 0.05}
    AXIS_NAMES = {
        1: 'Tolérance émotionnelle',
        2: 'Capacité financière',
        3: 'Horizon temporel',
        4: 'Connaissances financières',
        5: 'Valeurs et contraintes',
        6: 'Objectif financier',
        7: 'Comportement passé',
        8: 'Projets futurs',
    }

    def get_score(qn, ans):
        if not ans or ans == 'E':
            return 50
        return EXCEPTIONS.get(qn, DEFAULT).get(ans, 50)

    axis_scores = {}
    for ax, qs in AXES.items():
        scored = [q for q in qs if q not in OPEN_QS]
        axis_scores[ax] = (sum(get_score(q, answers.get(q, '')) for q in scored) / len(scored)) if scored else 50

    global_score = round(sum(axis_scores[ax] * AXIS_WEIGHTS[ax] for ax in AXES))

    if global_score <= 20:
        profil, allocation, dca_rate, profil_color, profil_emoji = 'Prudent', '80% fonds euros / 20% actions', 3.0, '#34D399', '🛡️'
    elif global_score <= 40:
        profil, allocation, dca_rate, profil_color, profil_emoji = 'Modéré Prudent', '60% fonds euros / 40% actions', 4.0, '#6EE7B7', '⚖️'
    elif global_score <= 60:
        profil, allocation, dca_rate, profil_color, profil_emoji = 'Équilibré', '50% obligations / 50% actions', 5.0, '#F6C90E', '🎯'
    elif global_score <= 80:
        profil, allocation, dca_rate, profil_color, profil_emoji = 'Dynamique', '20% obligations / 80% actions', 7.0, '#5B5FED', '🚀'
    else:
        profil, allocation, dca_rate, profil_color, profil_emoji = 'Agressif', '95% actions / 5% liquidités', 9.0, '#F87171', '⚡'

    axis_scores_r = {k: round(v) for k, v in axis_scores.items()}

    def ans_display(qn):
        val = answers.get(qn, '')
        if not val:
            return 'Non renseigné'
        if val == 'E':
            return f"Autre: {answers_text.get(qn, '').strip()}"
        return val

    contraintes_text = (
        f"Contraintes éthiques/religieuses: {ans_display(21)}. "
        f"Importance de l'impact: {ans_display(24)}. "
        f"Contraintes fiscales: {ans_display(25)}."
    )

    system_prompt = (
        "Tu es un outil pédagogique d'orientation financière. Tu génères des recommandations "
        "éducatives personnalisées basées sur un profil utilisateur. Tu n'es pas un conseiller "
        "en investissement agréé. Chaque recommandation doit inclure un disclaimer clair précisant "
        "qu'il s'agit d'une orientation pédagogique et non d'un conseil en investissement "
        "personnalisé. Tu rédiges en français, avec un ton accessible, direct et bienveillant."
    )

    user_prompt = (
        f"Voici le profil complet d'un utilisateur ayant complété le test de profil investisseur.\n\n"
        f"Score global : {global_score}/100. Profil : {profil}.\n\n"
        f"Scores par axe — Tolérance émotionnelle : {axis_scores_r[1]}/100, "
        f"Capacité financière : {axis_scores_r[2]}/100, "
        f"Horizon temporel : {axis_scores_r[3]}/100, "
        f"Connaissances financières : {axis_scores_r[4]}/100, "
        f"Objectif financier : {axis_scores_r[6]}/100, "
        f"Comportement passé : {axis_scores_r[7]}/100, "
        f"Projets futurs : {axis_scores_r[8]}/100.\n\n"
        f"Contraintes et valeurs : {contraintes_text}\n"
        f"Convictions sectorielles : {answers.get(22, '') or 'Aucune préférence particulière'}.\n"
        f"Convictions géographiques : {answers.get(23, '') or 'Aucune préférence particulière'}.\n"
        f"Objectif de vie : {answers.get(28, '') or 'Non renseigné'}.\n"
        f"Projets futurs détaillés : {answers.get(37, '') or 'Aucun projet précis'}.\n\n"
        f"Génère une recommandation concise et accessible structurée ainsi : "
        f"1) Analyse du profil en 3 phrases maximum, "
        f"2) 2-3 enveloppes fiscales adaptées avec 3 bullet points chacune, "
        f"3) Maximum 3 exemples d'ETF illustratifs avec disclaimer, "
        f"4) Allocation indicative en pourcentages par grande catégorie uniquement, "
        f"5) 3 points de vigilance de 2 lignes chacun, "
        f"6) Disclaimer légal en une phrase. "
        f"Sois direct, accessible, et va à l'essentiel. Pas de tableaux complexes, pas de listes exhaustives."
    )

    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    recommendation = ''
    if api_key:
        try:
            resp = req.post(
                'https://api.anthropic.com/v1/messages',
                headers={
                    'x-api-key': api_key,
                    'anthropic-version': '2023-06-01',
                    'content-type': 'application/json',
                },
                json={
                    'model': 'claude-haiku-4-5-20251001',
                    'max_tokens': 1500,
                    'system': system_prompt,
                    'messages': [{'role': 'user', 'content': user_prompt}]
                },
                timeout=30
            )
            if resp.status_code == 200:
                recommendation = resp.json()['content'][0]['text']
            else:
                print(f"Anthropic API error: {resp.status_code} {resp.text}")
        except Exception as e:
            print(f"Anthropic API error: {e}")

    if not recommendation:
        recommendation = (
            f"**Profil {profil}** — Score global : {global_score}/100\n\n"
            "La génération de recommandation personnalisée n'est pas disponible pour le moment. "
            "Consultez un conseiller financier agréé pour obtenir des conseils adaptés à votre situation.\n\n"
            "> ⚠️ Les informations affichées sont fournies à titre indicatif uniquement et ne constituent "
            "pas un conseil en investissement personnalisé."
        )

    rec_html = md_lib.markdown(recommendation, extensions=['nl2br'])

    if current_user.is_authenticated:
        try:
            save_profil_investisseur(
                user_id=current_user.id,
                score_global=global_score,
                nom_profil=profil,
                scores_axes=axis_scores_r,
                recommandation=recommendation,
            )
        except Exception as e:
            print(f"Erreur sauvegarde profil investisseur: {e}")

    return render_template(
        'resultat_profil.html',
        profil=profil,
        profil_emoji=profil_emoji,
        global_score=global_score,
        allocation=allocation,
        dca_rate=dca_rate,
        profil_color=profil_color,
        axis_scores=axis_scores_r,
        axis_names=AXIS_NAMES,
        recommendation_html=rec_html,
    )

# ── Mon profil investisseur (résultat sauvegardé) ────────────────────────────
@main_bp.route('/mon-profil-investisseur')
@login_required
def mon_profil_investisseur():
    last_profil = get_last_profil_investisseur(current_user.id)
    if not last_profil:
        flash('Tu n\'as pas encore fait le test de profil investisseur.', 'error')
        return redirect(url_for('main.test_profil'))

    sc = last_profil['score_global']
    if sc <= 20:
        profil_color, profil_emoji, allocation, dca_rate = '#34D399', '🛡️', '80% fonds euros / 20% actions', 3.0
    elif sc <= 40:
        profil_color, profil_emoji, allocation, dca_rate = '#6EE7B7', '⚖️', '60% fonds euros / 40% actions', 4.0
    elif sc <= 60:
        profil_color, profil_emoji, allocation, dca_rate = '#F6C90E', '🎯', '50% obligations / 50% actions', 5.0
    elif sc <= 80:
        profil_color, profil_emoji, allocation, dca_rate = '#5B5FED', '🚀', '20% obligations / 80% actions', 7.0
    else:
        profil_color, profil_emoji, allocation, dca_rate = '#F87171', '⚡', '95% actions / 5% liquidités', 9.0

    rec_html = md_lib.markdown(last_profil.get('recommandation', ''), extensions=['nl2br'])

    axis_names = {
        '1': 'Tolérance émotionnelle',
        '2': 'Capacité financière',
        '3': 'Horizon temporel',
        '4': 'Connaissances financières',
        '5': 'Valeurs et contraintes',
        '6': 'Objectif financier',
        '7': 'Comportement passé',
        '8': 'Projets futurs',
    }

    return render_template(
        'mon_profil_investisseur.html',
        last_profil=last_profil,
        profil=last_profil['nom_profil'],
        profil_emoji=profil_emoji,
        profil_color=profil_color,
        allocation=allocation,
        dca_rate=dca_rate,
        axis_names=axis_names,
        recommendation_html=rec_html,
    )

# ── Enregistrement blueprint + lancement ──────────────────────────────────────
app.register_blueprint(main_bp)

init_db()

if __name__ == '__main__':
    app.run(debug=True)
