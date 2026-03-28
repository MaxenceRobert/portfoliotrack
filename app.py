from dotenv import load_dotenv
load_dotenv()

import markdown as md_lib

from flask import Flask, render_template, request, redirect, url_for, flash, Response, jsonify, session
from flask_login import LoginManager, login_required, login_user, logout_user, current_user
from config import Config
from database import (
    init_db, populate_asset_catalog,
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
    get_user_by_email, create_user,
    add_alternative_asset, get_alternative_assets,
    update_alternative_asset, delete_alternative_asset,
    add_envelope, get_savings_envelopes, update_envelope_solde, delete_envelope,
)
from portfolio import (
    get_portfolio_summary, get_chart_data, get_current_price,
    get_portfolio_chart_data, get_benchmark_curve,
    get_ticker_info, get_ticker_history,
    get_auto_dividends_for_asset, get_estimated_annual_dividend,
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

# ── Enveloppes épargne garantie ───────────────────────────────────────────────
INVESTMENT_ENVELOPES = ['PEA', 'PEA-PME', 'CTO', 'Assurance Vie', 'PER', 'Autre']

SAVINGS_ENVELOPE_DEFAULTS = {
    'Livret A':                  {'taux': 2.4,  'plafond': 22950},
    'LDDS':                      {'taux': 2.4,  'plafond': 12000},
    'LEP':                       {'taux': 3.5,  'plafond': 10000},
    'Livret Jeune':              {'taux': 2.4,  'plafond': 1600},
    'Livret épargne entreprise': {'taux': 1.0,  'plafond': None},
    'Fonds euros AV':            {'taux': 2.5,  'plafond': None},
    'CEL':                       {'taux': 2.0,  'plafond': None},
    'PEL':                       {'taux': 2.25, 'plafond': None},
}

SAVINGS_ENVELOPE_RISK = {
    'Livret A':                  3,
    'LDDS':                      3,
    'LEP':                       2,
    'Livret Jeune':              3,
    'Livret épargne entreprise': 4,
    'Fonds euros AV':            6,
    'CEL':                       3,
    'PEL':                       4,
}

INFLATION_RATE = 2.0  # % — taux d'inflation de référence pour rendement réel

# ── Scoring de risque ─────────────────────────────────────────────────────────
def get_risk_score(ticker, asset_type='Autre'):
    """
    Score de risque 0-100 basé sur 5 métriques pondérées.

    Métriques et pondérations :
      - Volatilité annualisée    35%
      - Max Drawdown 3 ans       25%
      - Beta vs IWDA.AS (hebdo)  20%
      - Sharpe Ratio inversé      5%
      - VaR 95% annualisée       15%

    Cache DB 24h. Fallback sur score par défaut si données insuffisantes.
    ETF leviers : scores hardcodés ou multiplicateur 1.8.
    """
    import datetime as _dt
    import re

    # Actifs sans ticker → pas de calcul
    if not ticker or not ticker.strip():
        return {'score': 8, 'volatilite': None, 'drawdown': None, 'beta': None,
                'sharpe': None, 'var_95': None, 'source': 'default_no_ticker'}

    cached = get_cached_risk_score(ticker)
    if cached:
        return cached

    # ── ETF Leviers : scores hardcodés ───────────────────────────────────────
    LEVER_HARDCODED = {
        'LWLD': 78, 'LQQ': 82, 'CL2': 80, 'UST': 75,
        '3USL': 88, 'TPXL': 76, '2BTC': 90, 'DBXS': 79, 'LCWD': 78,
    }
    ticker_base = ticker.split('.')[0].upper()
    if ticker_base in LEVER_HARDCODED:
        result = {
            'score':      LEVER_HARDCODED[ticker_base],
            'volatilite': None, 'drawdown': None, 'beta': None,
            'sharpe':     None, 'var_95':   None,
            'source':     'hardcoded_leverage',
        }
        save_risk_score(ticker, result)
        return result

    # ── Détection pattern levier ──────────────────────────────────────────────
    _LEVER_RE = r'(?i)(2x|x2|leverage|levier|lw[a-z]|lqq|cl2|3x|x3|3us[a-z]?)'
    is_leveraged = bool(re.search(_LEVER_RE, ticker))

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

        if hist.empty or len(hist) < 100:
            print(f"[risk_score] Données insuffisantes pour {ticker}, score par défaut utilisé")
            raise ValueError(f"Données insuffisantes ({len(hist)} jours)")

        fallback_period = False
        if len(hist) < 126:  # moins de 6 mois sur 3 ans → fallback 1 an
            print(f"[risk_score] {ticker}: fallback 1 an ({len(hist)} jours disponibles)")
            hist = yf.download(
                ticker, start=start_1y.isoformat(), end=end.isoformat(),
                progress=False, auto_adjust=True,
            )
            if hist.empty or len(hist) < 100:
                print(f"[risk_score] Données insuffisantes pour {ticker}, score par défaut utilisé")
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

        # ── Multiplicateur levier ─────────────────────────────────────────────
        if is_leveraged:
            score = max(70, min(100, round(score * 1.8)))

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

    active_workspace = session.get('workspace', 'all')
    summary = get_portfolio_summary(current_user.id, active_workspace)

    # Enveloppes épargne garantie
    savings_envelopes_raw = get_savings_envelopes(current_user.id)
    savings_envelopes = []
    savings_total_solde = 0.0
    for env in savings_envelopes_raw:
        solde      = float(env.get('solde') or 0)
        taux       = float(env.get('taux_annuel') or 0)
        plafond    = env.get('plafond')
        rendement_brut  = round(solde * taux / 100, 2)
        rendement_reel  = round(solde * (taux - INFLATION_RATE) / 100, 2)
        pct_brut        = round(taux, 2)
        pct_reel        = round(taux - INFLATION_RATE, 2)
        plafond_pct     = round(solde / plafond * 100, 1) if plafond and plafond > 0 else None
        savings_envelopes.append({
            'id':             env['id'],
            'type':           env['type'],
            'nom':            env['nom'],
            'solde':          round(solde, 2),
            'taux_annuel':    taux,
            'plafond':        plafond,
            'plafond_pct':    plafond_pct,
            'date_ouverture': env.get('date_ouverture'),
            'rendement_brut': rendement_brut,
            'rendement_reel': rendement_reel,
            'pct_brut':       pct_brut,
            'pct_reel':       pct_reel,
        })
        savings_total_solde += solde

    # Score de risque par actif + score pondéré du portefeuille
    risk_data            = {}
    weighted_risk_sum    = 0.0
    total_risk_weight    = 0.0

    for assets_list in summary.get('by_type', {}).values():
        for a in assets_list:
            if not a.get('fully_sold', False):
                ticker = a.get('ticker', '') or ''
                if ticker and ticker not in risk_data:
                    risk_data[ticker] = get_risk_score(ticker, a.get('asset_type', 'Autre'))
                elif not ticker:
                    # Actif sans ticker : score par défaut selon enveloppe
                    env = a.get('envelope') or ''
                    default_s = _NOTICKER_ENV_SCORES.get(env, 8)
                    risk_data[f'__noticker_{a["asset_id"]}'] = {
                        'score': default_s, 'volatilite': None, 'drawdown': None,
                        'beta': None, 'sharpe': None, 'var_95': None, 'source': 'default_no_ticker',
                    }
                    ticker = f'__noticker_{a["asset_id"]}'
                val = float(a.get('current_value') or 0)
                if val > 0 and ticker in risk_data:
                    weighted_risk_sum += risk_data[ticker]['score'] * val
                    total_risk_weight += val

    # Inclure les enveloppes épargne dans le score de risque pondéré
    for senv in savings_envelopes:
        solde = senv['solde']
        if solde > 0:
            senv_score = SAVINGS_ENVELOPE_RISK.get(senv['type'], 3)
            weighted_risk_sum += senv_score * solde
            total_risk_weight += solde

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
    _all_purchases_list = list(_all_purch(current_user.id))
    monthly_by_asset = {}
    for _p in _all_purchases_list:
        if str(_p['date']).startswith(_this_month):
            _aid = _p['asset_id']
            monthly_by_asset[_aid] = monthly_by_asset.get(_aid, 0) + float(_p['total_cost'])

    all_assets_json = []
    for assets_list in summary.get('by_type', {}).values():
        for a in assets_list:
            if not a.get('fully_sold', False):
                _t = a.get('ticker', '') or ''
                _rk = _t if _t else f'__noticker_{a["asset_id"]}'
                ri = risk_data.get(_rk) or {}
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
                    'workspace':          a.get('workspace', 'perso'),
                })

    # Historique cumul capital investi
    from collections import defaultdict as _defaultdict
    _hist_by_date = _defaultdict(float)
    for _p in _all_purchases_list:
        _d = str(_p.get('date', ''))[:10]
        if _d:
            _hist_by_date[_d] += float(_p.get('total_cost') or 0)
    _hist_dates_sorted = sorted(_hist_by_date.keys())
    history_dates  = []
    history_values = []
    _cumul = 0.0
    for _d in _hist_dates_sorted:
        _cumul += _hist_by_date[_d]
        history_dates.append(_d)
        history_values.append(round(_cumul, 2))

    # Purchases JSON pour filtre JS évolution (envelope depuis la jointure)
    purchases_json = [
        {
            'date':       str(_p.get('date', ''))[:10],
            'total_cost': float(_p.get('total_cost') or 0),
            'envelope':   _p.get('envelope') or '',
        }
        for _p in _all_purchases_list
        if str(_p.get('date', ''))[:10]
    ]

    # Top 3 actifs par score de risque
    _risk_candidates = []
    for assets_list in summary.get('by_type', {}).values():
        for a in assets_list:
            if not a.get('fully_sold', False) and float(a.get('current_value') or 0) > 0:
                _t = a.get('ticker', '') or ''
                _rk = _t if _t else f'__noticker_{a["asset_id"]}'
                ri = risk_data.get(_rk) or {}
                score = ri.get('score')
                if score is not None:
                    _risk_candidates.append({
                        'ticker':     a['ticker'],
                        'name':       a['name'],
                        'risk_score': score,
                        'envelope':   a.get('envelope') or '',
                    })
    _risk_candidates.sort(key=lambda x: x['risk_score'], reverse=True)
    top_risks = _risk_candidates[:3]

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

    # KPI dividendes estimés cette année (trailing 12 mois × parts détenues)
    estimated_dividends_year = 0.0
    for a in summary.get('assets', []):
        if not a.get('fully_sold', False) and float(a.get('shares_held') or 0) > 0:
            estimated_dividends_year += get_estimated_annual_dividend(
                a['ticker'], float(a['shares_held'])
            )
    estimated_dividends_year = round(estimated_dividends_year, 2)

    is_demo = current_user.email == DEMO_EMAIL

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
        estimated_dividends_year=estimated_dividends_year,
        is_demo=is_demo,
        active_workspace=active_workspace,
        history_dates=history_dates,
        history_values=history_values,
        purchases_json=purchases_json,
        top_risks=top_risks,
        savings_envelopes=savings_envelopes,
        savings_total_solde=round(savings_total_solde, 2),
        inflation_rate=INFLATION_RATE,
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

# ── Score de risque par défaut selon enveloppe (actifs sans ticker) ───────────
_NOTICKER_ENV_SCORES = {
    'Livret A': 5, 'LDDS': 5, 'LEP': 4,
    'Assurance Vie': 7, 'PER': 15,
}

# ── Ajouter un actif ──────────────────────────────────────────────────────────
@main_bp.route('/assets/add', methods=['GET', 'POST'])
@login_required
def add_asset_route():
    if request.method == 'POST':
        no_ticker  = request.form.get('no_ticker') == '1'
        ticker     = request.form.get('ticker', '').strip().upper()
        name       = request.form['name'].strip()
        asset_type = request.form['asset_type']
        currency   = request.form.get('currency', 'EUR').strip().upper()
        isin       = request.form.get('isin', '').strip().upper()
        envelope   = request.form.get('envelope', '').strip()
        workspace  = request.form.get('workspace', 'perso').strip()
        taux_fixe  = None
        flash_msg  = None

        if no_ticker or not ticker:
            # Actif sans cours en temps réel
            ticker = ''
            try:
                taux_fixe = float(request.form.get('taux_fixe', '3.0') or 3.0)
            except (ValueError, TypeError):
                taux_fixe = 3.0
            flash_msg = f'{name} ajouté ✓ (taux fixe : {taux_fixe}%/an)'
        else:
            price = get_current_price(ticker)
            if price is None:
                flash(f'Ticker "{ticker}" introuvable sur Yahoo Finance.', 'error')
                return redirect(url_for('main.add_asset_route'))
            flash_msg = f'{name} ajouté ✓ (prix actuel : {price} {currency})'

        success = add_asset(current_user.id, ticker, name, asset_type, currency,
                            isin, envelope, workspace, taux_fixe=taux_fixe)
        if not success:
            flash('Cet actif est déjà dans ton portefeuille.', 'error')
            return redirect(url_for('main.add_asset_route'))

        flash(flash_msg, 'success')
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

# ── Ajouter une enveloppe ──────────────────────────────────────────────────────
@main_bp.route('/add-envelope', methods=['GET', 'POST'])
@login_required
def add_envelope_route():
    if request.method == 'POST':
        env_type   = request.form.get('env_type', '').strip()
        env_kind   = request.form.get('env_kind', '')   # 'investment' | 'savings'
        if env_kind == 'investment':
            flash(f'Enveloppe {env_type} créée — elle est disponible dans le formulaire d\'ajout d\'actifs.', 'success')
            return redirect(url_for('main.add_asset_route'))

        # Enveloppe épargne garantie
        nom_custom   = request.form.get('nom', '').strip() or env_type
        try:
            solde = float(request.form.get('solde', '0') or 0)
        except (ValueError, TypeError):
            solde = 0.0
        defaults = SAVINGS_ENVELOPE_DEFAULTS.get(env_type, {})
        try:
            taux = float(request.form.get('taux_annuel', defaults.get('taux', 0)) or 0)
        except (ValueError, TypeError):
            taux = float(defaults.get('taux', 0))
        plafond_default = defaults.get('plafond')
        try:
            plafond = float(request.form.get('plafond', plafond_default) or 0) or None
        except (ValueError, TypeError):
            plafond = plafond_default
        date_ouverture = request.form.get('date_ouverture', '').strip() or None

        ok = add_envelope(current_user.id, env_type, nom_custom, solde, taux, plafond, date_ouverture)
        if ok:
            flash(f'{nom_custom} ajouté ✓', 'success')
        else:
            flash('Erreur lors de l\'ajout de l\'enveloppe.', 'error')
        return redirect(url_for('main.dashboard'))

    return render_template(
        'add_envelope.html',
        investment_envelopes=INVESTMENT_ENVELOPES,
        savings_defaults=SAVINGS_ENVELOPE_DEFAULTS,
    )

# ── Modifier le solde d'une enveloppe (AJAX) ──────────────────────────────────
@main_bp.route('/update-envelope/<int:env_id>', methods=['POST'])
@login_required
def update_envelope_route(env_id):
    data = request.get_json(silent=True) or {}
    try:
        solde = float(data.get('solde', 0))
    except (ValueError, TypeError):
        return jsonify({'success': False, 'error': 'Solde invalide'}), 400
    ok = update_envelope_solde(env_id, current_user.id, solde)
    if ok:
        return jsonify({'success': True, 'solde': solde})
    return jsonify({'success': False, 'error': 'Mise à jour échouée'}), 500

# ── Supprimer une enveloppe ────────────────────────────────────────────────────
@main_bp.route('/delete-envelope/<int:env_id>', methods=['POST'])
@login_required
def delete_envelope_route(env_id):
    delete_envelope(env_id, current_user.id)
    flash('Enveloppe supprimée.', 'success')
    return redirect(url_for('main.dashboard'))

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
                    workspace_i = request.form.get(f'workspace_{i}', 'perso').strip()

                    add_asset(current_user.id, ticker,
                              asset_name or ticker, mapped_type,
                              currency or 'EUR', '', envelope, workspace_i)

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
    from database import get_purchases_by_asset, get_sales_by_asset
    all_d = get_all_dividends(current_user.id)

    assets = get_user_assets(current_user.id)
    auto_divs_list = []
    for asset in assets:
        purchases = get_purchases_by_asset(asset['id'], current_user.id)
        sales     = get_sales_by_asset(asset['id'], current_user.id)
        for d in get_auto_dividends_for_asset(asset['ticker'], purchases, sales):
            auto_divs_list.append({
                'date':             d['date'],
                'ticker':           asset['ticker'],
                'name':             asset['name'],
                'amount':           d['total_amount'],
                'shares_held':      d['shares_held'],
                'amount_per_share': d['amount_per_share'],
                'source':           'auto',
            })
    auto_divs_list.sort(key=lambda x: x['date'], reverse=True)

    return render_template('dividends.html', dividends=all_d, auto_dividends=auto_divs_list)

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
    import json as _json

    # ── Lecture des réponses (15 questions) ────────────────────────────────────
    answers = {}
    for i in range(1, 16):
        answers[i] = request.form.get(f'q{i}', '').strip()
    answers_text = {}
    for i in range(1, 16):
        answers_text[i] = request.form.get(f'q{i}_text', '').strip()

    # ── Noms des axes ─────────────────────────────────────────────────────────
    AXIS_NAMES = {
        1: 'Tolérance émotionnelle',
        2: 'Horizon temporel',
        3: 'Objectif financier',
        4: 'Capacité financière',
        5: 'Connaissances',
        6: 'Projets futurs',
    }

    # ── Scoring des questions numériques (Q2-Q4) ──────────────────────────────
    def numeric_score(raw, breakpoints):
        try:
            v = float(raw)
        except (ValueError, TypeError):
            return 50.0
        if v <= breakpoints[0][0]:
            return float(breakpoints[0][1])
        for i in range(len(breakpoints) - 1):
            x0, y0 = breakpoints[i]
            x1, y1 = breakpoints[i + 1]
            if x0 <= v <= x1:
                return y0 + (v - x0) / (x1 - x0) * (y1 - y0)
        return float(breakpoints[-1][1])

    # Q2 — horizon en années (facteur le plus discriminant après tolérance)
    HORIZ_BP  = [(0,5),(2,20),(5,50),(10,70),(20,85),(40,95)]
    # Q3 — mois d'épargne de précaution
    EPARG_BP  = [(0,5),(1,20),(3,45),(6,65),(12,80),(24,95)]
    # Q4 — % revenus investissables
    PCT_BP    = [(0,10),(5,30),(10,50),(20,70),(30,85),(50,95)]

    # Q5-Q10 : choix multiples
    SC = {
        5:  {'A':0,  'B':25, 'C':60, 'D':100, 'E':35},  # réaction chute -30% — facteur limitant principal
        6:  {'A':20, 'B':60, 'C':90, 'D':70,  'E':50},  # objectif financier
        7:  {'A':10, 'B':40, 'C':75, 'D':95,  'E':45},  # expérience / connaissances
        8:  {'A':10, 'B':35, 'C':70, 'D':90,  'E':40},  # situation professionnelle
        9:  {'A':10, 'B':35, 'C':65, 'D':90,  'E':50},  # projets 3 ans (A=gros projet imminent → risque bas)
        10: {'A':90, 'B':60, 'C':30,           'E':50},  # dépendants financiers (A=aucun → capacité élevée)
    }

    def q_score(qn):
        v = answers.get(qn, '')
        if qn == 2:
            return numeric_score(v, HORIZ_BP)
        if qn == 3:
            return numeric_score(v, EPARG_BP)
        if qn == 4:
            return numeric_score(v, PCT_BP)
        return float(SC.get(qn, {}).get(v, 50))

    # Axes recalibrés — 6 facteurs
    # Principe : tolérance émotionnelle est le FACTEUR LIMITANT (35%)
    # Capacité financière ≠ appétit pour le risque (réduit à 15%)
    AXES = {
        1: [5],           # tolérance émotionnelle : réaction à une chute de 30%
        2: [2],           # horizon temporel : années d'investissement
        3: [6],           # objectif financier
        4: [3, 4, 8, 10], # capacité financière : épargne, % revenus, situation pro, dépendants
        5: [7],           # connaissances et expérience
        6: [9],           # projets futurs à court terme
    }
    AXIS_WEIGHTS = {1: 0.35, 2: 0.20, 3: 0.15, 4: 0.15, 5: 0.10, 6: 0.05}

    axis_scores = {}
    for ax, qs in AXES.items():
        if qs:
            axis_scores[ax] = sum(q_score(q) for q in qs) / len(qs)
        else:
            axis_scores[ax] = 50.0

    global_score = max(0, min(100, round(
        sum(axis_scores[ax] * AXIS_WEIGHTS[ax] for ax in AXES)
    )))

    if global_score <= 20:
        profil, allocation, dca_rate, profil_color, profil_emoji = 'Prudent',       '80% fonds euros / 20% actions',    3.0, '#34D399', '🛡️'
    elif global_score <= 40:
        profil, allocation, dca_rate, profil_color, profil_emoji = 'Modéré Prudent','60% fonds euros / 40% actions',    4.0, '#6EE7B7', '⚖️'
    elif global_score <= 60:
        profil, allocation, dca_rate, profil_color, profil_emoji = 'Équilibré',     '50% obligations / 50% actions',    5.0, '#F6C90E', '🎯'
    elif global_score <= 80:
        profil, allocation, dca_rate, profil_color, profil_emoji = 'Dynamique',     '20% obligations / 80% actions',    7.0, '#5B5FED', '🚀'
    else:
        profil, allocation, dca_rate, profil_color, profil_emoji = 'Agressif',      '95% actions / 5% liquidités',      9.0, '#F87171', '⚡'

    axis_scores_r = {k: round(v) for k, v in axis_scores.items()}

    # ── Contexte des questions ouvertes pour Claude ────────────────────────────
    open_ctx = (
        f"Situation financière / objectif: {answers.get(11) or 'Non renseigné'}. "
        f"Convictions sectorielles ou contraintes éthiques: {answers.get(12) or 'Aucune'}. "
        f"Convictions géographiques: {answers.get(13) or 'Aucune'}. "
        f"Types d'actifs souhaités ou à éviter: {answers.get(14) or 'Aucune préférence'}. "
        f"Montant mensuel envisagé: {answers.get(15) or '?'} €/mois."
    )

    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    api_headers = {
        'x-api-key': api_key,
        'anthropic-version': '2023-06-01',
        'content-type': 'application/json',
    }

    # ── Appel 1 : recommandation textuelle ────────────────────────────────────
    system_prompt = (
        "Tu es un outil pédagogique d'orientation financière. Tu génères des recommandations "
        "éducatives personnalisées. Tu n'es pas un conseiller en investissement agréé. "
        "Chaque recommandation inclut un disclaimer clair. Tu rédiges en français, ton accessible et bienveillant."
    )
    user_prompt = (
        f"Profil investisseur — score {global_score}/100 ({profil}).\n"
        f"Axes (0-100) : tolérance émotionnelle {axis_scores_r[1]}, "
        f"horizon temporel {axis_scores_r[2]}, objectif financier {axis_scores_r[3]}, "
        f"capacité financière {axis_scores_r[4]}, connaissances {axis_scores_r[5]}, "
        f"projets futurs {axis_scores_r[6]}.\n"
        f"Contexte libre : {open_ctx}\n\n"
        f"Génère une recommandation concise : "
        f"1) Analyse du profil (3 phrases max). "
        f"2) 2-3 enveloppes fiscales adaptées (3 bullet points chacune). "
        f"3) Max 3 ETF illustratifs avec disclaimer. "
        f"4) Allocation indicative par catégorie (%). "
        f"5) 3 points de vigilance (2 lignes chacun). "
        f"6) Disclaimer légal en 1 phrase. "
        f"Sois direct, accessible, sans tableaux complexes."
    )

    recommendation = ''
    if api_key:
        try:
            resp = req.post(
                'https://api.anthropic.com/v1/messages',
                headers=api_headers,
                json={
                    'model':      'claude-haiku-4-5-20251001',
                    'max_tokens': 1500,
                    'system':     system_prompt,
                    'messages':   [{'role': 'user', 'content': user_prompt}],
                },
                timeout=30,
            )
            if resp.status_code == 200:
                recommendation = resp.json()['content'][0]['text']
            else:
                print(f"Anthropic rec error: {resp.status_code}")
        except Exception as e:
            print(f"Anthropic rec error: {e}")

    if not recommendation:
        recommendation = (
            f"**Profil {profil}** — Score global : {global_score}/100\n\n"
            "La génération de recommandation personnalisée n'est pas disponible pour le moment. "
            "Consultez un conseiller financier agréé.\n\n"
            "> ⚠️ À titre indicatif uniquement, pas un conseil en investissement."
        )

    rec_html = md_lib.markdown(recommendation, extensions=['nl2br'])

    # ── Appel 2 : actifs suggérés (JSON) ──────────────────────────────────────
    asset_suggestions = []
    if api_key:
        assets_prompt = (
            f"Tu es un outil pédagogique en investissement. "
            f"Profil : score {global_score}/100 ({profil}), horizon {answers.get(2, '?')} ans. "
            f"{open_ctx}\n\n"
            f"Recommande exactement 6 actifs réels adaptés à ce profil. "
            f"Réponds UNIQUEMENT en JSON valide, sans markdown ni texte autour, format exact :\n"
            f'[{{"ticker":"XXXX","nom":"Nom complet","type":"ETF","score_risque":45,'
            f'"allocation_pct":20,"explication":"Pourquoi cet actif correspond"}}, ...]\n'
            f"Types autorisés : ETF, Action, Crypto, Obligation. "
            f"score_risque = estimation 0-100. allocation_pct = allocation suggérée (total ~100%). "
            f"Tickers Yahoo Finance exacts obligatoires. "
            f"DISCLAIMER : à titre illustratif uniquement."
        )
        try:
            resp2 = req.post(
                'https://api.anthropic.com/v1/messages',
                headers=api_headers,
                json={
                    'model':    'claude-haiku-4-5-20251001',
                    'max_tokens': 1200,
                    'messages': [{'role': 'user', 'content': assets_prompt}],
                },
                timeout=25,
            )
            if resp2.status_code == 200:
                raw = resp2.json()['content'][0]['text'].strip()
                # Extraire le JSON même si du texte entoure
                start = raw.find('[')
                end   = raw.rfind(']') + 1
                if start >= 0 and end > start:
                    asset_suggestions = _json.loads(raw[start:end])
        except Exception as e:
            print(f"Anthropic assets error: {e}")

    # ── Sauvegarde ────────────────────────────────────────────────────────────
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
        asset_suggestions=asset_suggestions,
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

# ── Barre ticker Bloomberg (cache mémoire 15 min) ─────────────────────────────
import time as _time
_TICKER_BAR_CACHE = {'data': None, 'ts': 0}

TICKER_BAR_ITEMS = [
    ('S&P 500',    'SPY'),
    ('NASDAQ',     'QQQ'),
    ('CAC 40',     '^FCHI'),
    ('MSCI World', 'IWDA.AS'),
    ('VWCE',       'VWCE.DE'),
    ('DAX',        '^GDAXI'),
    ('Dow Jones',  '^DJI'),
    ('Nikkei',     '^N225'),
    ('Or',         'GC=F'),
    ('Pétrole',    'CL=F'),
    ('BTC',        'BTC-USD'),
    ('ETH',        'ETH-USD'),
    ('EUR/USD',    'EURUSD=X'),
    ('Apple',      'AAPL'),
    ('NVIDIA',     'NVDA'),
    ('Microsoft',  'MSFT'),
    ('Amazon',     'AMZN'),
    ('Tesla',      'TSLA'),
    ('Stoxx 600',  '^STOXX'),
]

@main_bp.route('/api/ticker-bar')
def ticker_bar_api():
    import yfinance as _yf
    import threading
    now = _time.time()
    if _TICKER_BAR_CACHE['data'] and now - _TICKER_BAR_CACHE['ts'] < 900:
        return jsonify(_TICKER_BAR_CACHE['data'])

    results = []
    lock = threading.Lock()

    def fetch_one(name, ticker):
        try:
            fi    = _yf.Ticker(ticker).fast_info
            # fast_info est un objet (pas un dict) en yfinance 0.2+ → getattr obligatoire
            price = float(getattr(fi, 'last_price', None) or getattr(fi, 'regularMarketPrice', None) or 0)
            prev  = float(getattr(fi, 'previous_close', None) or getattr(fi, 'regularMarketPreviousClose', None) or 0)
            if price <= 0:
                return
            chg = round((price - prev) / prev * 100, 2) if prev > 0 else 0.0
            with lock:
                results.append({'name': name, 'ticker': ticker,
                                'price': round(price, 2), 'change_pct': chg})
        except Exception as e:
            print(f"[ticker-bar] {ticker}: {e}")

    threads = []
    for name, ticker in TICKER_BAR_ITEMS:
        t = threading.Thread(target=fetch_one, args=(name, ticker), daemon=True)
        threads.append((t, ticker))
        t.start()

    for t, _ in threads:
        t.join(timeout=5)

    # Trier dans l'ordre original
    order = {ticker: i for i, (name, ticker) in enumerate(TICKER_BAR_ITEMS)}
    results.sort(key=lambda x: order.get(x['ticker'], 999))

    data = {'items': results}
    if results:
        _TICKER_BAR_CACHE['data'] = data
        _TICKER_BAR_CACHE['ts']   = now
    return jsonify(data)

# ── API search-assets (Explorer) avec mots-clés français ─────────────────────
_FR_KEYWORDS = {
    'or physique':        ['IGLN.L', 'PHAU.L', 'GLD'],
    'matieres premieres': ['GLD', 'SLV', 'DJP', 'PDBC'],
    'matieres':           ['GLD', 'SLV', 'DJP', 'PDBC'],
    'or':                 ['GLD', 'GOLD', 'IAU', 'IGLN.L'],
    'argent':             ['SLV', 'SIVR'],
    'russie':             ['ERUS', 'RSX'],
    'chine':              ['MCHI', 'FXI', 'CNYA.L'],
    'inde':               ['INDA', 'NDIA.L', '5MVL.L'],
    'japon':              ['EWJ', 'JPNE.L'],
    'europe':             ['VGK', 'IEUR.AS', 'EXW1.DE'],
    'usa':                ['SPY', 'QQQ', 'VOO', 'IVV'],
    'etats-unis':         ['SPY', 'QQQ', 'VOO', 'IVV'],
    'amerique':           ['SPY', 'QQQ', 'VOO', 'IVV'],
    'tech':               ['QQQ', 'VGT', 'XLK'],
    'technologie':        ['QQQ', 'VGT', 'XLK'],
    'energie':            ['XLE', 'VDE', 'IEGY.L'],
    'sante':              ['XLV', 'IBB', 'HEAL.L'],
    'immobilier':         ['VNQ', 'IPRP.AS', 'EPRA.AS'],
    'dividende':          ['VYM', 'SCHD', 'SDY', 'VHYL.L'],
    'dividendes':         ['VYM', 'SCHD', 'SDY', 'VHYL.L'],
    'islamique':          ['HLAL', 'ISDU.L', 'WSRI.L'],
    'halal':              ['HLAL', 'ISDU.L', 'WSRI.L'],
    'bitcoin':            ['BTC-USD'],
    'btc':                ['BTC-USD'],
    'ethereum':           ['ETH-USD'],
    'eth':                ['ETH-USD'],
    'crypto':             ['BTC-USD', 'ETH-USD', 'SOL-USD', 'BNB-USD'],
    'obligations':        ['AGG', 'BND', 'AGGH.L', 'IEAG.AS'],
    'emergents':          ['EIMI.AS', 'VWO', 'IEMG'],
    'emerging':           ['EIMI.AS', 'VWO', 'IEMG'],
    'monde':              ['IWDA.AS', 'VWCE.DE', 'SWRD.SW'],
    'world':              ['IWDA.AS', 'VWCE.DE', 'SWRD.SW'],
    'global':             ['IWDA.AS', 'VWCE.DE', 'SWRD.SW'],
    'sp500':              ['SPY', 'VOO', 'CSPX.AS'],
    's&p':                ['SPY', 'VOO', 'CSPX.AS'],
    'sp 500':             ['SPY', 'VOO', 'CSPX.AS'],
    'nasdaq':             ['QQQ', 'EQQQ.L', 'CNDX.L'],
    'cac':                ['CAC.PA', 'EXI1.DE'],
    'cac40':              ['CAC.PA', 'EXI1.DE'],
    'france':             ['CAC.PA', 'EXI1.DE'],
    'levier':             ['TQQQ', 'UPRO', 'LQQ.PA', 'CL2.PA'],
    'leverage':           ['TQQQ', 'UPRO', 'LQQ.PA', 'CL2.PA'],
}

def _normalize(s):
    """Minuscules + retire accents pour matching."""
    import unicodedata
    s = s.lower().strip()
    return ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')

@main_bp.route('/api/search-assets')
def search_assets():
    import requests as _req
    q = request.args.get('q', '').strip()
    if len(q) < 2:
        return jsonify({'results': []})

    q_norm = _normalize(q)

    # 1. Chercher dans la table de mots-clés français
    hardcoded_tickers = []
    
    for kw, tickers in _FR_KEYWORDS.items():
        if _normalize(kw) == q_norm or q_norm in _normalize(kw) or _normalize(kw) in q_norm:
            for t in tickers:
                if t not in hardcoded_tickers:
                    hardcoded_tickers.append(t)

    # Construire des objets résultat pour les tickers hardcodés
    hardcoded_results = []
    for t in hardcoded_tickers:
        curr = 'EUR' if any(x in t for x in ['.AS', '.DE', '.PA', '.L', '.SW']) else 'USD'
        if '-USD' in t:
            curr = 'USD'
        hardcoded_results.append({
            'ticker':   t,
            'name':     t,
            'type':     'ETF',
            'exchange': '',
            'currency': curr,
            '_hardcoded': True,
        })

    # 2. Chercher via Yahoo Finance
    yf_results = []
    try:
        url = (f'https://query1.finance.yahoo.com/v1/finance/search'
               f'?q={q}&quotesCount=8&newsCount=0&listsCount=0')
        headers = {'User-Agent': 'Mozilla/5.0'}
        r = _req.get(url, headers=headers, timeout=5)
        data = r.json()
        for q_item in data.get('quotes', []):
            symbol = q_item.get('symbol', '')
            name   = q_item.get('longname') or q_item.get('shortname') or symbol
            qtype  = q_item.get('quoteType', '')
            exch   = q_item.get('exchDisp') or q_item.get('exchange', '')
            curr   = q_item.get('currency', '')
            if not curr:
                if any(x in symbol for x in ['.PA', '.AS', '.DE', '.L', '.SW']):
                    curr = 'EUR'
                elif '-USD' in symbol:
                    curr = 'USD'
                else:
                    curr = 'USD'
            # Éviter doublons avec hardcoded
            if symbol not in hardcoded_tickers:
                yf_results.append({
                    'ticker':   symbol,
                    'name':     name,
                    'type':     qtype,
                    'exchange': exch,
                    'currency': curr,
                })
    except Exception as e:
        print(f"Search assets error: {e}")

    combined = hardcoded_results + yf_results
    # Retirer _hardcoded des objets finaux
    for r in combined:
        r.pop('_hardcoded', None)

    return jsonify({'results': combined})

# ── Route démo ────────────────────────────────────────────────────────────────
DEMO_EMAIL    = 'demo@portfoliotrack.com'
DEMO_PASSWORD = 'DemoPortfolio2024!'

@main_bp.route('/demo')
def demo():
    import traceback
    from werkzeug.security import generate_password_hash

    try:
        # 1. Créer le compte démo s'il n'existe pas
        user_row = get_user_by_email(DEMO_EMAIL)
        if not user_row:
            print(f"[demo] Création du compte démo {DEMO_EMAIL}")
            ok = create_user(DEMO_EMAIL, generate_password_hash(DEMO_PASSWORD))
            print(f"[demo] create_user → {ok}")
            user_row = get_user_by_email(DEMO_EMAIL)

        if not user_row:
            print("[demo] Impossible de récupérer le compte démo après création")
            flash('Impossible de charger le compte démo.', 'error')
            return redirect(url_for('main.landing'))

        demo_id = user_row['id']
        print(f"[demo] Compte démo trouvé id={demo_id}")

        # 2. Ajouter des actifs fictifs si le portfolio est insuffisant
        existing = get_user_assets(demo_id)
        print(f"[demo] Actifs existants : {len(existing)}")

        if len(existing) < 10:
            # Wipe existing assets and recreate enriched demo
            from database import delete_asset as _del_asset
            for a in existing:
                try:
                    _del_asset(demo_id, a['id'])
                except Exception:
                    pass

            # 14 actifs répartis sur 3 enveloppes
            # (ticker, name, type, currency, isin, envelope, workspace)
            demo_assets = [
                # PEA
                ('IWDA.AS',  'iShares Core MSCI World ETF',            'ETF',    'EUR', 'IE00B4L5Y983', 'PEA',            'perso'),
                ('PAEEM.PA', 'Amundi MSCI Emerging Markets ETF',        'ETF',    'EUR', 'LU1681045370', 'PEA',            'perso'),
                ('ESE.PA',   'Amundi S&P 500 ESG ETF',                  'ETF',    'EUR', 'LU1992086910', 'PEA',            'perso'),
                ('PANX.PA',  'Amundi Nasdaq-100 ETF',                   'ETF',    'EUR', 'LU1681038243', 'PEA',            'perso'),
                ('PVAL.PA',  'Amundi MSCI World Value ETF',             'ETF',    'EUR', 'LU1681048055', 'PEA',            'perso'),
                ('CW8.PA',   'Amundi MSCI World ETF',                   'ETF',    'EUR', 'LU1681043599', 'PEA',            'perso'),
                ('LYYA.PA',  'Amundi Core Global Aggregate Bond ETF',   'ETF',    'EUR', 'LU1829220216', 'PEA',            'perso'),
                # CTO
                ('AAPL',     'Apple Inc.',                              'Action', 'USD', 'US0378331005', 'CTO',            'perso'),
                ('NVDA',     'NVIDIA Corporation',                      'Action', 'USD', 'US67066G1040', 'CTO',            'perso'),
                ('MSFT',     'Microsoft Corporation',                   'Action', 'USD', 'US5949181045', 'CTO',            'perso'),
                ('BTC-USD',  'Bitcoin',                                 'Crypto', 'USD', '',             'CTO',            'perso'),
                ('ETH-USD',  'Ethereum',                                'Crypto', 'USD', '',             'CTO',            'perso'),
                # Assurance Vie
                ('GLD',      'SPDR Gold Shares',                        'ETF',    'USD', 'US78463V1070', 'Assurance Vie',  'perso'),
                ('AGGH.L',   'iShares Core Global Aggregate Bond ETF',  'ETF',    'USD', 'IE00BDBRDM35', 'Assurance Vie',  'perso'),
            ]
            demo_purchases = {
                'IWDA.AS':  [('2021-06-01', 40.0, 68.20), ('2022-01-15', 30.0, 60.10), ('2022-09-10', 25.0, 63.80), ('2023-04-05', 20.0, 75.50)],
                'PAEEM.PA': [('2022-02-01', 60.0, 22.10), ('2022-10-15', 40.0, 19.40), ('2023-05-20', 30.0, 24.80)],
                'ESE.PA':   [('2022-03-10', 20.0, 18.50), ('2023-01-20', 15.0, 20.30), ('2023-08-10', 10.0, 22.90)],
                'PANX.PA':  [('2021-11-15', 15.0, 41.20), ('2022-06-20', 20.0, 29.80), ('2023-03-10', 10.0, 38.60)],
                'PVAL.PA':  [('2022-04-01', 25.0, 31.40), ('2023-02-14', 15.0, 34.10)],
                'CW8.PA':   [('2022-07-01', 10.0, 372.50), ('2023-01-09', 5.0, 389.20)],
                'LYYA.PA':  [('2022-05-10', 50.0, 8.20), ('2022-11-01', 30.0, 7.90)],
                'AAPL':     [('2022-03-01', 10.0, 162.50), ('2022-10-14', 5.0, 138.40), ('2023-06-01', 8.0, 178.20)],
                'NVDA':     [('2022-10-01', 5.0, 125.60), ('2023-01-25', 3.0, 195.30), ('2023-05-30', 2.0, 390.20)],
                'MSFT':     [('2022-02-10', 6.0, 295.00), ('2023-03-15', 4.0, 265.80)],
                'BTC-USD':  [('2021-11-01', 0.05, 58000.0), ('2022-06-20', 0.10, 20100.0), ('2023-01-10', 0.08, 17200.0)],
                'ETH-USD':  [('2022-01-01', 0.50, 3500.0), ('2022-07-01', 0.80, 1050.0), ('2023-02-01', 0.30, 1620.0)],
                'GLD':      [('2022-03-15', 8.0, 185.40), ('2022-11-10', 5.0, 162.80), ('2023-06-01', 3.0, 187.50)],
                'AGGH.L':   [('2022-04-20', 30.0, 52.30), ('2022-12-05', 20.0, 47.80)],
            }

            for ticker, name, atype, currency, isin, envelope, workspace in demo_assets:
                result = add_asset(demo_id, ticker, name, atype, currency, isin, envelope, workspace)
                print(f"[demo] add_asset({ticker}) → {result}")

            assets_refreshed = get_user_assets(demo_id)
            asset_map = {}
            for a in assets_refreshed:
                try:
                    asset_map[a['ticker']] = a['id']
                except Exception:
                    asset_map[a[2]] = a[0]
            print(f"[demo] asset_map = {asset_map}")

            for ticker, purchases in demo_purchases.items():
                asset_id = asset_map.get(ticker)
                if not asset_id:
                    print(f"[demo] asset_id introuvable pour {ticker}")
                    continue
                rows = [
                    {'asset_id': asset_id, 'date': d, 'shares': q, 'price_per_share': p}
                    for d, q, p in purchases
                ]
                add_purchases_bulk(demo_id, rows)
                print(f"[demo] {len(rows)} achats insérés pour {ticker}")

            # Profil investisseur démo : score 62 → Dynamique
            try:
                save_profil_investisseur(
                    user_id=demo_id,
                    score_global=62,
                    nom_profil='Dynamique',
                    scores_axes={1: 70, 2: 75, 3: 65, 4: 60, 5: 55, 6: 50},
                    recommandation=(
                        "**Profil Dynamique** — Score global : 62/100\n\n"
                        "Ton profil indique une bonne tolérance au risque et un horizon long terme favorable à une exposition "
                        "majoritaire en actions. Les enveloppes fiscales PEA et Assurance Vie sont particulièrement adaptées.\n\n"
                        "**Enveloppes recommandées :** PEA (exonération fiscale après 5 ans), "
                        "Assurance Vie (flexibilité et transmission), CTO (accès international).\n\n"
                        "**ETF illustratifs :** IWDA.AS (monde), PANX.PA (Nasdaq), PAEEM.PA (émergents). "
                        "*Ceci n'est pas un conseil en investissement. Tout investissement comporte un risque de perte en capital.*"
                    ),
                )
            except Exception as e:
                print(f"[demo] save_profil_investisseur error: {e}")

            # Objectif DCA : 500 €/mois
            try:
                set_dca_goal(demo_id, 500.0)
            except Exception as e:
                print(f"[demo] set_dca_goal error: {e}")

            # Marquer onboarding comme complété
            try:
                set_onboarding_completed(demo_id)
            except Exception as e:
                print(f"[demo] set_onboarding_completed error: {e}")

        # 3. Connecter l'utilisateur démo
        user_obj = get_user_object(demo_id)
        if not user_obj:
            print("[demo] get_user_object a retourné None")
            flash('Erreur de connexion au compte démo.', 'error')
            return redirect(url_for('main.landing'))

        login_user(user_obj)
        print(f"[demo] login_user OK pour {DEMO_EMAIL}")
        return redirect(url_for('main.dashboard'))

    except Exception:
        traceback.print_exc()
        flash('Une erreur est survenue lors du chargement de la démo.', 'error')
        return redirect(url_for('main.landing'))


@main_bp.route('/demo/logout')
def demo_logout():
    logout_user()
    return redirect(url_for('main.landing'))


# ── Workspace ─────────────────────────────────────────────────────────────────
@main_bp.route('/api/workspace', methods=['POST'])
@login_required
def set_workspace():
    data = request.get_json(silent=True) or {}
    ws = data.get('workspace', 'all')
    if ws not in ('all', 'perso', 'pro'):
        ws = 'all'
    session['workspace'] = ws
    return jsonify({'success': True, 'workspace': ws})


# ── Coach IA ──────────────────────────────────────────────────────────────────
@main_bp.route('/coach')
@login_required
def coach():
    if 'coach_messages' not in session:
        session['coach_messages'] = []

    summary = get_portfolio_summary(current_user.id)
    last_profil = get_last_profil_investisseur(current_user.id)

    portfolio_ctx = {
        'total_value': summary.get('total_value', 0),
        'total_invested': summary.get('total_invested', 0),
        'total_unrealized': summary.get('total_unrealized', 0),
        'unrealized_pct': summary.get('unrealized_pct', 0),
        'nb_assets': len([a for a in summary.get('assets', []) if not a.get('fully_sold')]),
        'profil': last_profil.get('nom_profil') if last_profil else None,
        'profil_score': last_profil.get('score_global') if last_profil else None,
        'assets': [
            {'ticker': a['ticker'], 'name': a['name'], 'type': a['asset_type'],
             'value': a.get('current_value', 0), 'gain_pct': a.get('unrealized_pct', 0)}
            for a in summary.get('assets', []) if not a.get('fully_sold', False)
        ]
    }

    return render_template('coach.html',
                           messages=session.get('coach_messages', []),
                           portfolio_ctx=portfolio_ctx)

@main_bp.route('/coach/message', methods=['POST'])
@login_required
def coach_message():
    import os, requests as req

    data = request.get_json(silent=True) or {}
    user_msg = data.get('message', '').strip()
    if not user_msg:
        return jsonify({'error': 'Message vide'}), 400

    summary = get_portfolio_summary(current_user.id)
    last_profil = get_last_profil_investisseur(current_user.id)

    portfolio_ctx = {
        'total_value': round(summary.get('total_value', 0), 2),
        'total_invested': round(summary.get('total_invested', 0), 2),
        'total_unrealized': round(summary.get('total_unrealized', 0), 2),
        'unrealized_pct': round(summary.get('unrealized_pct', 0), 2),
        'profil': last_profil.get('nom_profil') if last_profil else 'Non défini',
        'profil_score': last_profil.get('score_global') if last_profil else None,
        'assets': [
            {'ticker': a['ticker'], 'name': a['name'], 'type': a['asset_type'],
             'value': a.get('current_value', 0), 'gain_pct': a.get('unrealized_pct', 0)}
            for a in summary.get('assets', []) if not a.get('fully_sold', False)
        ]
    }

    system_prompt = (
        "Tu es un coach financier pédagogique spécialisé dans l'analyse de portefeuille "
        "et la gestion du risque. Tu réponds UNIQUEMENT en lien avec le portefeuille, "
        "le profil de risque, et les investissements de l'utilisateur. "
        "Tu ne fais pas de réponses générales sur la finance mondiale. "
        "Tu analyses les données concrètes du portefeuille fournies et tu réponds directement à la question posée. "
        "Règles de format strictes : pas de markdown (pas de ##, pas de **, pas de tirets de liste), "
        "paragraphes courts séparés par des sauts de ligne, maximum 3 paragraphes, "
        "langage accessible et direct. "
        "Si la question ne concerne pas le portefeuille ou les investissements, "
        "recentre poliment sur ce sujet. "
        f"Voici les données du portefeuille : {json.dumps(portfolio_ctx, ensure_ascii=False)}."
    )

    if 'coach_messages' not in session:
        session['coach_messages'] = []

    conv = list(session.get('coach_messages', []))
    conv.append({'role': 'user', 'content': user_msg})

    api_messages = conv[-10:]

    api_key = os.environ.get('ANTHROPIC_API_KEY', '')
    assistant_reply = ''

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
                    'max_tokens': 1000,
                    'system': system_prompt,
                    'messages': api_messages,
                },
                timeout=30,
            )
            if resp.status_code == 200:
                assistant_reply = resp.json()['content'][0]['text']
            else:
                assistant_reply = f"Erreur API ({resp.status_code}). Vérifie ta clé ANTHROPIC_API_KEY."
        except Exception as e:
            assistant_reply = f"Erreur de connexion à l'API : {str(e)}"
    else:
        assistant_reply = ("Je n'ai pas accès à l'API Anthropic pour le moment. "
                           "Configure la variable ANTHROPIC_API_KEY pour activer le Coach IA.")

    conv.append({'role': 'assistant', 'content': assistant_reply})
    session['coach_messages'] = conv[-20:]
    session.modified = True

    return jsonify({'reply': assistant_reply})

@main_bp.route('/coach/clear', methods=['POST'])
@login_required
def coach_clear():
    session['coach_messages'] = []
    session.modified = True
    return jsonify({'success': True})


# ── Bilan PDF ─────────────────────────────────────────────────────────────────
@main_bp.route('/bilan-pdf')
@login_required
def bilan_pdf():
    try:
        from reportlab.lib.pagesizes import A4
        from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
        from reportlab.lib.units import cm
        from reportlab.lib import colors
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable
        from reportlab.lib.enums import TA_CENTER, TA_LEFT, TA_RIGHT
        import io as _io
        import datetime as _dt
    except ImportError:
        flash('ReportLab non installé. Installe-le avec : pip install reportlab', 'error')
        return redirect(url_for('main.dashboard'))

    summary = get_portfolio_summary(current_user.id)
    last_profil = get_last_profil_investisseur(current_user.id)

    risk_data = {}
    for a in summary.get('assets', []):
        if not a.get('fully_sold', False):
            ticker = a.get('ticker', '')
            if ticker and ticker not in risk_data:
                risk_data[ticker] = get_risk_score(ticker, a.get('asset_type', 'Autre'))

    portfolio_risk_score = None
    weighted_risk_sum = 0.0
    total_risk_weight = 0.0
    for a in summary.get('assets', []):
        if not a.get('fully_sold', False):
            ri = risk_data.get(a.get('ticker', '')) or {}
            val = float(a.get('current_value') or 0)
            if val > 0 and ri.get('score') is not None:
                weighted_risk_sum += ri['score'] * val
                total_risk_weight += val
    if total_risk_weight > 0:
        portfolio_risk_score = round(weighted_risk_sum / total_risk_weight)

    profil_risk_score = last_profil.get('score_global') if last_profil else None
    coherence = None
    if portfolio_risk_score is not None and profil_risk_score is not None:
        coherence = max(0, 100 - abs(portfolio_risk_score - profil_risk_score))

    buf = _io.BytesIO()
    doc = SimpleDocTemplate(buf, pagesize=A4,
                             rightMargin=2*cm, leftMargin=2*cm,
                             topMargin=2*cm, bottomMargin=2.5*cm)

    NAVY   = colors.HexColor('#1a2a3a')
    BLUE   = colors.HexColor('#4a7a9b')
    LIGHT  = colors.HexColor('#f0f4f8')
    GREEN  = colors.HexColor('#16a34a')
    RED    = colors.HexColor('#dc2626')
    GRAY   = colors.HexColor('#6b7280')
    WHITE  = colors.white

    styles = getSampleStyleSheet()
    title_style = ParagraphStyle('Title', parent=styles['Normal'],
                                  fontSize=20, textColor=NAVY, spaceAfter=4,
                                  fontName='Helvetica-Bold')
    sub_style   = ParagraphStyle('Sub', parent=styles['Normal'],
                                  fontSize=9, textColor=GRAY, spaceAfter=12)
    h2_style    = ParagraphStyle('H2', parent=styles['Normal'],
                                  fontSize=12, textColor=NAVY, spaceBefore=14, spaceAfter=6,
                                  fontName='Helvetica-Bold')
    body_style  = ParagraphStyle('Body', parent=styles['Normal'],
                                  fontSize=8.5, textColor=colors.HexColor('#374151'), leading=13)
    small_style = ParagraphStyle('Small', parent=styles['Normal'],
                                  fontSize=7, textColor=GRAY, leading=11)
    disclaimer  = ParagraphStyle('Disc', parent=styles['Normal'],
                                  fontSize=7, textColor=GRAY, leading=11, spaceBefore=20,
                                  borderPad=6, borderWidth=0.5, borderColor=GRAY, borderRadius=4)

    story = []
    now_str = _dt.datetime.now().strftime('%d/%m/%Y à %H:%M')
    username = current_user.email.split('@')[0]

    story.append(Paragraph("PortfolioTrack", title_style))
    story.append(Paragraph(f"Bilan de portefeuille · {username} · Généré le {now_str}", sub_style))
    story.append(HRFlowable(width='100%', thickness=1, color=BLUE, spaceAfter=14))

    story.append(Paragraph("Résumé", h2_style))
    total_val = summary.get('total_value', 0)
    total_inv = summary.get('total_invested', 0)
    total_unr = summary.get('total_unrealized', 0)
    unr_pct   = summary.get('unrealized_pct', 0)

    kpi_data = [
        ['Valeur totale', 'Total investi', 'Plus-value latente', 'Performance'],
        [f"{total_val:,.2f} €", f"{total_inv:,.2f} €",
         f"{'+' if total_unr >= 0 else ''}{total_unr:,.2f} €",
         f"{'+' if unr_pct >= 0 else ''}{unr_pct:.2f}%"],
    ]
    kpi_table = Table(kpi_data, colWidths=[4.2*cm]*4)
    kpi_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), NAVY),
        ('TEXTCOLOR',  (0,0), (-1,0), WHITE),
        ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE',   (0,0), (-1,0), 8),
        ('BACKGROUND', (0,1), (-1,1), LIGHT),
        ('FONTSIZE',   (0,1), (-1,1), 10),
        ('FONTNAME',   (0,1), (-1,1), 'Helvetica-Bold'),
        ('ALIGN',      (0,0), (-1,-1), 'CENTER'),
        ('VALIGN',     (0,0), (-1,-1), 'MIDDLE'),
        ('ROWBACKGROUNDS', (0,0), (-1,-1), [NAVY, LIGHT]),
        ('GRID',       (0,0), (-1,-1), 0.5, colors.HexColor('#d1d5db')),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('TEXTCOLOR', (0,1), (0,1), NAVY),
        ('TEXTCOLOR', (1,1), (1,1), NAVY),
        ('TEXTCOLOR', (2,1), (2,1), GREEN if total_unr >= 0 else RED),
        ('TEXTCOLOR', (3,1), (3,1), GREEN if unr_pct >= 0 else RED),
    ]))
    story.append(kpi_table)
    story.append(Spacer(1, 0.4*cm))

    story.append(Paragraph("Actifs", h2_style))
    asset_header = ['Ticker', 'Nom', 'Valeur (€)', 'Perf. (%)', 'Risque', 'Enveloppe']
    asset_rows = [asset_header]
    for a in sorted(summary.get('assets', []), key=lambda x: x.get('current_value', 0), reverse=True):
        if a.get('fully_sold', False):
            continue
        ri = risk_data.get(a.get('ticker', '')) or {}
        score = ri.get('score', '—')
        pct = a.get('unrealized_pct', 0)
        asset_rows.append([
            a.get('ticker', ''),
            a.get('name', '')[:35],
            f"{a.get('current_value', 0):,.2f}",
            f"{'+' if pct >= 0 else ''}{pct:.1f}%",
            str(score) if score != '—' else '—',
            a.get('envelope', '') or '—',
        ])

    col_w = [2.2*cm, 6.5*cm, 2.8*cm, 2.2*cm, 1.8*cm, 2.2*cm]
    a_table = Table(asset_rows, colWidths=col_w)
    a_style = [
        ('BACKGROUND', (0,0), (-1,0), NAVY),
        ('TEXTCOLOR',  (0,0), (-1,0), WHITE),
        ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE',   (0,0), (-1,-1), 7.5),
        ('ALIGN',      (2,0), (-1,-1), 'RIGHT'),
        ('ALIGN',      (0,0), (1,-1), 'LEFT'),
        ('GRID',       (0,0), (-1,-1), 0.3, colors.HexColor('#e5e7eb')),
        ('TOPPADDING', (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [WHITE, LIGHT]),
    ]
    for i, a in enumerate(summary.get('assets', []), start=1):
        if a.get('fully_sold', False):
            continue
        pct = a.get('unrealized_pct', 0)
        col = GREEN if pct >= 0 else RED
        a_style.append(('TEXTCOLOR', (3, i), (3, i), col))
    a_table.setStyle(TableStyle(a_style))
    story.append(a_table)

    if portfolio_risk_score is not None or coherence is not None:
        story.append(Paragraph("Cohérence & Risque", h2_style))
        coh_rows = []
        if portfolio_risk_score is not None:
            coh_rows.append(['Score de risque portefeuille', f"{portfolio_risk_score}/100"])
        if profil_risk_score is not None:
            coh_rows.append(['Score de risque profil investisseur', f"{profil_risk_score}/100"])
        if last_profil:
            coh_rows.append(['Profil investisseur', last_profil.get('nom_profil', '—')])
        if coherence is not None:
            coh_rows.append(['Score de cohérence', f"{coherence}/100"])
        if coh_rows:
            c_table = Table(coh_rows, colWidths=[8*cm, 4*cm])
            c_table.setStyle(TableStyle([
                ('FONTSIZE', (0,0), (-1,-1), 8.5),
                ('GRID', (0,0), (-1,-1), 0.3, colors.HexColor('#e5e7eb')),
                ('TOPPADDING', (0,0), (-1,-1), 5),
                ('BOTTOMPADDING', (0,0), (-1,-1), 5),
                ('ROWBACKGROUNDS', (0,0), (-1,-1), [WHITE, LIGHT]),
            ]))
            story.append(c_table)

    by_type = {}
    for a in summary.get('assets', []):
        if not a.get('fully_sold', False) and a.get('current_value', 0) > 0:
            t = a.get('asset_type', 'Autre')
            by_type[t] = by_type.get(t, 0) + a.get('current_value', 0)

    if by_type:
        story.append(Paragraph("Allocation par type d'actif", h2_style))
        alloc_rows = [['Type', 'Valeur (€)', '% du portefeuille']]
        total_v = sum(by_type.values())
        for t, v in sorted(by_type.items(), key=lambda x: -x[1]):
            pct_alloc = (v / total_v * 100) if total_v > 0 else 0
            alloc_rows.append([t, f"{v:,.2f}", f"{pct_alloc:.1f}%"])
        al_table = Table(alloc_rows, colWidths=[5*cm, 4*cm, 4*cm])
        al_table.setStyle(TableStyle([
            ('BACKGROUND', (0,0), (-1,0), NAVY),
            ('TEXTCOLOR',  (0,0), (-1,0), WHITE),
            ('FONTNAME',   (0,0), (-1,0), 'Helvetica-Bold'),
            ('FONTSIZE',   (0,0), (-1,-1), 8.5),
            ('GRID',       (0,0), (-1,-1), 0.3, colors.HexColor('#e5e7eb')),
            ('TOPPADDING', (0,0), (-1,-1), 5),
            ('BOTTOMPADDING', (0,0), (-1,-1), 5),
            ('ROWBACKGROUNDS', (0,1), (-1,-1), [WHITE, LIGHT]),
            ('ALIGN', (1,0), (-1,-1), 'RIGHT'),
        ]))
        story.append(al_table)

    disclaimer_text = (
        "<b>Avertissement légal :</b> Ce document est généré à titre informatif uniquement. "
        "Les données proviennent de Yahoo Finance et peuvent comporter un décalage. "
        "Ce bilan ne constitue pas un conseil en investissement. "
        "PortfolioTrack ne peut être tenu responsable des décisions prises sur la base de ce document. "
        "Consultez un conseiller financier agréé pour toute décision d'investissement."
    )
    story.append(Paragraph(disclaimer_text, disclaimer))

    doc.build(story)
    buf.seek(0)
    filename = f"bilan_portefeuille_{_dt.datetime.now().strftime('%Y%m%d')}.pdf"
    return Response(
        buf.getvalue(),
        mimetype='application/pdf',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )


# ── Actifs Alternatifs ────────────────────────────────────────────────────────
ALTERNATIVE_CATEGORIES = [
    ('private_equity', 'Private Equity', 55),
    ('art', "Oeuvres d'art", 40),
    ('vehicule', 'Véhicule de collection', 35),
    ('metaux', 'Métaux précieux physiques', 30),
    ('foret', 'Forêt / Vignes', 20),
    ('immobilier', 'Immobilier direct', 25),
    ('autre', 'Autre', 40),
]

@main_bp.route('/actifs-alternatifs', methods=['GET', 'POST'])
@login_required
def actifs_alternatifs():
    if request.method == 'POST':
        action = request.form.get('action', 'add')

        if action == 'add':
            name             = request.form.get('name', '').strip()
            category         = request.form.get('category', 'autre')
            acquisition_value = float(request.form.get('acquisition_value', 0) or 0)
            current_value    = float(request.form.get('current_value', 0) or 0)
            acquisition_date = request.form.get('acquisition_date', '') or None
            notes            = request.form.get('notes', '').strip()
            workspace        = request.form.get('workspace', 'perso')

            if not name:
                flash('Le nom est obligatoire.', 'error')
            else:
                add_alternative_asset(current_user.id, name, category,
                                       acquisition_value, current_value,
                                       acquisition_date, notes, workspace)
                flash(f'{name} ajouté', 'success')

        elif action == 'delete':
            asset_id = int(request.form.get('asset_id', 0))
            delete_alternative_asset(asset_id, current_user.id)
            flash('Actif supprimé.', 'success')

        elif action == 'update':
            asset_id         = int(request.form.get('asset_id', 0))
            name             = request.form.get('name', '').strip()
            category         = request.form.get('category', 'autre')
            acquisition_value = float(request.form.get('acquisition_value', 0) or 0)
            current_value    = float(request.form.get('current_value', 0) or 0)
            acquisition_date = request.form.get('acquisition_date', '') or None
            notes            = request.form.get('notes', '').strip()
            workspace        = request.form.get('workspace', 'perso')
            update_alternative_asset(asset_id, current_user.id, name, category,
                                      acquisition_value, current_value,
                                      acquisition_date, notes, workspace)
            flash('Actif mis à jour', 'success')

        return redirect(url_for('main.actifs_alternatifs'))

    assets = get_alternative_assets(current_user.id)
    assets = [dict(a) for a in assets]

    cat_risk = {c[0]: c[2] for c in ALTERNATIVE_CATEGORIES}
    for a in assets:
        a['risk_score'] = cat_risk.get(a.get('category', 'autre'), 40)
        a['gain'] = round(float(a.get('current_value', 0)) - float(a.get('acquisition_value', 0)), 2)
        av = float(a.get('acquisition_value', 0))
        a['gain_pct'] = round(a['gain'] / av * 100, 2) if av > 0 else 0

    total_acquisition = round(sum(float(a.get('acquisition_value', 0)) for a in assets), 2)
    total_current     = round(sum(float(a.get('current_value', 0)) for a in assets), 2)
    total_gain        = round(total_current - total_acquisition, 2)

    return render_template('actifs_alternatifs.html',
                            assets=assets,
                            categories=ALTERNATIVE_CATEGORIES,
                            total_acquisition=total_acquisition,
                            total_current=total_current,
                            total_gain=total_gain,
                            active_workspace=session.get('workspace', 'all'))


# ── Enregistrement blueprint + lancement ──────────────────────────────────────
app.register_blueprint(main_bp)

init_db()
populate_asset_catalog()

if __name__ == '__main__':
    app.run(debug=True)
