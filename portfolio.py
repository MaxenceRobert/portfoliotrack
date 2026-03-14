import yfinance as yf
from datetime import datetime, date
from database import (
    get_user_assets, get_purchases_by_asset,
    get_sales_by_asset, get_dca_goal, get_all_purchases,
    get_dividends_by_asset, get_total_dividends
)

def get_current_price(ticker):
    try:
        data  = yf.Ticker(ticker)
        price = data.fast_info['last_price']
        return round(float(price), 4)
    except Exception as e:
        print(f"Erreur prix {ticker}: {e}")
        return None

def get_current_price_with_timestamp(ticker):
    try:
        data      = yf.Ticker(ticker)
        price     = data.fast_info['last_price']
        now       = datetime.now().strftime('%d/%m/%Y à %H:%M')
        return round(float(price), 4), now
    except Exception as e:
        print(f"Erreur prix {ticker}: {e}")
        return None, None
def get_benchmark_curve(start_date, end_date, purchases=None, ticker='IWDA.AS'):
    """Simule un investissement DCA équivalent dans le benchmark."""
    try:
        from datetime import datetime, timedelta
        if hasattr(start_date, 'strftime'):
            start_date = start_date.strftime('%Y-%m-%d')
        if hasattr(end_date, 'strftime'):
            end_date = end_date.strftime('%Y-%m-%d')

        end_dt   = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
        end_str  = end_dt.strftime('%Y-%m-%d')

        data = yf.download(ticker, start=start_date, end=end_str,
                           progress=False, auto_adjust=True)
        if data.empty:
            return [], []

        closes = data['Close'].squeeze().dropna()
        price_series = {d.strftime('%Y-%m-%d'): float(v)
                        for d, v in zip(closes.index, closes)}

        if not purchases:
            # Pas de DCA fourni — normalisation simple
            base   = float(closes.iloc[0])
            dates  = [d.strftime('%Y-%m-%d') for d in closes.index]
            values = [round(float(v) / base * 100, 4) for v in closes]
            return dates, values

        # Simulation DCA : pour chaque achat, on achète des parts du benchmark
        benchmark_shares = 0.0
        purchase_index   = 0
        sorted_purchases = sorted(purchases, key=lambda p: p['date'])

        dates_out  = []
        values_out = []

        all_dates = sorted(price_series.keys())
        for d in all_dates:
            # On traite tous les achats dont la date <= d
            while purchase_index < len(sorted_purchases):
                p = sorted_purchases[purchase_index]
                p_date = p['date'] if isinstance(p['date'], str) else p['date'].strftime('%Y-%m-%d')
                if p_date <= d:
                    # Trouve le prix du benchmark à la date d'achat
                    buy_price = price_series.get(p_date)
                    if buy_price is None:
                        # Prend le prix le plus proche
                        close_dates = [x for x in all_dates if x <= p_date]
                        if close_dates:
                            buy_price = price_series[close_dates[-1]]
                    if buy_price and buy_price > 0:
                        benchmark_shares += p['total_cost'] / buy_price
                    purchase_index += 1
                else:
                    break

            if benchmark_shares > 0:
                dates_out.append(d)
                values_out.append(round(benchmark_shares * price_series[d], 2))

        return dates_out, values_out

    except Exception as e:
        print(f"Erreur benchmark {ticker}: {e}")
        return [], []
def calc_asset_stats(asset, purchases, sales):
    """
    Calcule les stats complètes d'un actif en tenant compte
    des achats ET des ventes (parts restantes, plus-value réalisée).
    """
    if not purchases:
        return None

    # ── Parts et coût moyen pondéré ─────────────────────────────────────────
    total_shares_bought = sum(p['shares'] for p in purchases)
    total_invested      = sum(p['total_cost'] for p in purchases)
    total_fees          = sum(p['fees'] or 0 for p in purchases)
    avg_price           = total_invested / total_shares_bought if total_shares_bought else 0

    # ── Parts vendues et plus-value réalisée ────────────────────────────────
    total_shares_sold   = sum(s['shares'] for s in sales)
    total_proceeds      = sum(s['total_proceeds'] for s in sales)
    total_fees_sales    = sum(s['fees'] or 0 for s in sales)

    # Coût d'acquisition des parts vendues (au prix moyen d'achat)
    cost_of_sold        = round(total_shares_sold * avg_price, 4)
    realized_gain       = round(total_proceeds - cost_of_sold - total_fees_sales, 2)

    # ── Parts restantes en portefeuille ─────────────────────────────────────
    shares_held         = round(total_shares_bought - total_shares_sold, 6)
    invested_held       = round(shares_held * avg_price, 2)

    if shares_held <= 0:
        # Actif entièrement vendu
        return {
            'asset_id':       asset['id'],
            'isin':           asset['isin'],
            'ticker':         asset['ticker'],
            'name':           asset['name'],
            'currency':       asset['currency'],
            'asset_type':     asset['asset_type'],
            'shares_held':    0,
            'total_invested': round(total_invested, 2),
            'total_fees':     round(total_fees + total_fees_sales, 2),
            'avg_price':      round(avg_price, 4),
            'current_price':  None,
            'current_value':  0,
            'unrealized_gain': 0,
            'unrealized_pct': 0,
            'realized_gain':  realized_gain,
            'fully_sold':     True,
        }

    # ── Prix actuel et plus-value latente ───────────────────────────────────
    current_price, price_timestamp = get_current_price_with_timestamp(asset['ticker'])
    if current_price is None:
        return None

    current_value      = round(shares_held * current_price, 2)
    unrealized_gain    = round(current_value - invested_held, 2)
    unrealized_pct     = round((unrealized_gain / invested_held) * 100, 2) if invested_held else 0

    # Dividendes
    from database import get_dividends_by_asset
    divs               = get_dividends_by_asset(asset['id'], asset['user_id'])
    dividends_received = round(sum(d['amount'] for d in divs), 2)
    total_return       = round(unrealized_gain + realized_gain + dividends_received, 2)
    total_return_pct   = round((total_return / total_invested) * 100, 2) if total_invested else 0

    return {
        'asset_id':        asset['id'],
        'isin':            asset['isin'],
        'ticker':          asset['ticker'],
        'name':            asset['name'],
        'currency':        asset['currency'],
        'asset_type':      asset['asset_type'],
        'shares_held':     shares_held,
        'total_invested':  round(total_invested, 2),
        'total_fees':      round(total_fees + total_fees_sales, 2),
        'avg_price':       round(avg_price, 4),
        'current_price':   current_price,
        'price_timestamp': price_timestamp,
        'current_value':   current_value,
        'unrealized_gain': unrealized_gain,
        'unrealized_pct':  unrealized_pct,
        'realized_gain':   realized_gain,
        'dividends_received': dividends_received,
        'total_return':       total_return,
        'total_return_pct':   total_return_pct,
        'fully_sold':      False,
    }

def get_portfolio_summary(user_id):
    """Résumé complet du portefeuille avec stats par actif et totaux globaux."""
    assets     = get_user_assets(user_id)
    stats_list = []
    total_inv  = 0
    total_val  = 0
    total_real = 0

    # Grouper par type pour l'affichage
    by_type = {}
    for asset in assets:
        purchases = get_purchases_by_asset(asset['id'], user_id)
        sales     = get_sales_by_asset(asset['id'], user_id)
        stats     = calc_asset_stats(asset, purchases, sales)
        if not stats:
            continue
        stats_list.append(stats)
        total_inv  += stats['total_invested']
        total_val  += stats['current_value']
        total_real += stats['realized_gain']
        t = stats['asset_type']
        by_type.setdefault(t, []).append(stats)

    total_unrealized     = round(total_val - sum(
        s['shares_held'] * s['avg_price'] for s in stats_list if not s['fully_sold']
    ), 2)
    total_unrealized_pct = round(
        (total_unrealized / total_inv * 100) if total_inv else 0, 2
    )

    # Allocation par actif (pour camembert)
    allocation = [
        {'ticker': s['ticker'], 'value': s['current_value'], 'type': s['asset_type']}
        for s in stats_list if not s['fully_sold'] and s['current_value'] > 0
    ]

    # Objectif DCA
    dca_goal   = get_dca_goal(user_id)
    dca_target = dca_goal['monthly_target'] if dca_goal else 0

    # Investi ce mois-ci
    this_month     = date.today().strftime('%Y-%m')
    all_purchases  = get_all_purchases(user_id)
    invested_month = sum(
        p['total_cost'] for p in all_purchases
        if p['date'].startswith(this_month)
    )
    dca_progress = round((invested_month / dca_target * 100), 1) if dca_target else 0

    total_dividends = get_total_dividends(user_id)

    return {
        'assets':            stats_list,
        'by_type':           by_type,
        'allocation':        allocation,
        'total_invested':    round(total_inv, 2),
        'total_value':       round(total_val, 2),
        'total_unrealized':  total_unrealized,
        'unrealized_pct':    total_unrealized_pct,
        'total_realized':    round(total_real, 2),
        'total_dividends':   total_dividends,
        'dca_target':        dca_target,
        'invested_month':    round(invested_month, 2),
        'dca_progress':      dca_progress,
    }

def get_chart_data(asset, purchases, sales):
    """
    Courbes historiques pour un actif :
    - valeur investie cumulée nette (achats - ventes)
    - valeur marché au prix actuel
    """
    if not purchases:
        return [], [], []

    current_price = get_current_price(asset['ticker'])
    if current_price is None:
        return [], [], []

    # Fusionner achats et ventes sur une timeline
    events = []
    for p in purchases:
        events.append({'date': p['date'], 'type': 'buy',
                       'shares': p['shares'], 'cost': p['total_cost']})
    for s in sales:
        events.append({'date': s['date'], 'type': 'sell',
                       'shares': s['shares'], 'cost': s['total_proceeds']})
    events.sort(key=lambda x: x['date'])

    dates          = []
    invested_curve = []
    market_curve   = []
    cum_shares     = 0
    cum_invested   = 0

    for e in events:
        if e['type'] == 'buy':
            cum_shares   += e['shares']
            cum_invested += e['cost']
        else:
            cum_shares   -= e['shares']
            cum_invested -= e['cost']
        dates.append(e['date'])
        invested_curve.append(round(max(cum_invested, 0), 2))
        market_curve.append(round(max(cum_shares, 0) * current_price, 2))

    return dates, invested_curve, market_curve
def get_portfolio_chart_data(user_id):
    """Courbe globale du portefeuille — somme de tous les actifs."""
    from database import get_user_assets, get_purchases_by_asset, get_sales_by_asset

    assets   = get_user_assets(user_id)
    all_events = []

    for asset in assets:
        purchases = get_purchases_by_asset(asset['id'], user_id)
        sales     = get_sales_by_asset(asset['id'], user_id)
        price     = get_current_price(asset['ticker'])
        if price is None:
            continue
        for p in purchases:
            all_events.append({
                'date':   p['date'],
                'type':   'buy',
                'cost':   p['total_cost'],
                'shares': p['shares'],
                'price':  price,
                'ticker': asset['ticker']
            })
        for s in sales:
            all_events.append({
                'date':   s['date'],
                'type':   'sell',
                'cost':   s['total_proceeds'],
                'shares': s['shares'],
                'price':  price,
                'ticker': asset['ticker']
            })

    if not all_events:
        return [], [], []

    all_events.sort(key=lambda x: x['date'])

    # Cumul investi et valeur marché par date
    dates          = []
    invested_curve = []
    market_curve   = []
    cum_invested   = 0
    cum_shares_by_ticker = {}

    for e in all_events:
        ticker = e['ticker']
        if ticker not in cum_shares_by_ticker:
            cum_shares_by_ticker[ticker] = {'shares': 0, 'price': e['price']}

        if e['type'] == 'buy':
            cum_invested += e['cost']
            cum_shares_by_ticker[ticker]['shares'] += e['shares']
        else:
            cum_invested -= e['cost']
            cum_shares_by_ticker[ticker]['shares'] -= e['shares']

        market_total = sum(
            max(v['shares'], 0) * v['price']
            for v in cum_shares_by_ticker.values()
        )

        dates.append(e['date'])
        invested_curve.append(round(max(cum_invested, 0), 2))
        market_curve.append(round(market_total, 2))

    return dates, invested_curve, market_curve