import yfinance as yf
from datetime import datetime, date, timedelta
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
        data  = yf.Ticker(ticker)
        price = data.fast_info['last_price']
        now   = datetime.now().strftime('%d/%m/%Y à %H:%M')
        return round(float(price), 4), now
    except Exception as e:
        print(f"Erreur prix {ticker}: {e}")
        return None, None


def get_benchmark_curve(start_date, end_date, purchases=None, ticker='IWDA.AS'):
    """Simule un investissement DCA équivalent dans le benchmark."""
    try:
        if hasattr(start_date, 'strftime'):
            start_date = start_date.strftime('%Y-%m-%d')
        if hasattr(end_date, 'strftime'):
            end_date = end_date.strftime('%Y-%m-%d')

        end_dt  = datetime.strptime(end_date, '%Y-%m-%d') + timedelta(days=1)
        end_str = end_dt.strftime('%Y-%m-%d')

        data = yf.download(ticker, start=start_date, end=end_str,
                           progress=False, auto_adjust=True)
        if data.empty:
            return [], []

        closes = data['Close'].squeeze().dropna()
        price_series = {d.strftime('%Y-%m-%d'): float(v)
                        for d, v in zip(closes.index, closes)}

        if not purchases:
            base   = float(closes.iloc[0])
            dates  = [d.strftime('%Y-%m-%d') for d in closes.index]
            values = [round(float(v) / base * 100, 4) for v in closes]
            return dates, values

        benchmark_shares = 0.0
        purchase_index   = 0
        sorted_purchases = sorted(purchases, key=lambda p: p['date'])

        dates_out  = []
        values_out = []

        all_dates = sorted(price_series.keys())
        for d in all_dates:
            while purchase_index < len(sorted_purchases):
                p = sorted_purchases[purchase_index]
                p_date = p['date'] if isinstance(p['date'], str) else p['date'].strftime('%Y-%m-%d')
                if p_date <= d:
                    buy_price = price_series.get(p_date)
                    if buy_price is None:
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
    if not purchases:
        return None

    total_shares_bought = sum(p['shares'] for p in purchases)
    total_invested      = sum(p['total_cost'] for p in purchases)
    total_fees          = sum(p['fees'] or 0 for p in purchases)
    avg_price           = total_invested / total_shares_bought if total_shares_bought else 0

    total_shares_sold  = sum(s['shares'] for s in sales)
    total_proceeds     = sum(s['total_proceeds'] for s in sales)
    total_fees_sales   = sum(s['fees'] or 0 for s in sales)

    cost_of_sold  = round(total_shares_sold * avg_price, 4)
    realized_gain = round(total_proceeds - cost_of_sold - total_fees_sales, 2)

    shares_held   = round(total_shares_bought - total_shares_sold, 6)
    invested_held = round(shares_held * avg_price, 2)

    if shares_held <= 0:
        return {
            'asset_id':        asset['id'],
            'isin':            asset['isin'],
            'ticker':          asset['ticker'],
            'name':            asset['name'],
            'currency':        asset['currency'],
            'asset_type':      asset['asset_type'],
            'envelope':        asset.get('envelope') or '',
            'shares_held':     0,
            'total_invested':  round(total_invested, 2),
            'total_fees':      round(total_fees + total_fees_sales, 2),
            'avg_price':       round(avg_price, 4),
            'current_price':   None,
            'current_value':   0,
            'unrealized_gain': 0,
            'unrealized_pct':  0,
            'realized_gain':   realized_gain,
            'fully_sold':      True,
        }

    current_price, price_timestamp = get_current_price_with_timestamp(asset['ticker'])
    if current_price is None:
        return None

    current_value   = round(shares_held * current_price, 2)
    unrealized_gain = round(current_value - invested_held, 2)
    unrealized_pct  = round((unrealized_gain / invested_held) * 100, 2) if invested_held else 0

    divs               = get_dividends_by_asset(asset['id'], asset['user_id'])
    dividends_received = round(sum(d['amount'] for d in divs), 2)
    total_return       = round(unrealized_gain + realized_gain + dividends_received, 2)
    total_return_pct   = round((total_return / total_invested) * 100, 2) if total_invested else 0

    return {
        'asset_id':           asset['id'],
        'isin':               asset['isin'],
        'ticker':             asset['ticker'],
        'name':               asset['name'],
        'currency':           asset['currency'],
        'asset_type':         asset['asset_type'],
        'envelope':           asset.get('envelope') or '',
        'shares_held':        shares_held,
        'total_invested':     round(total_invested, 2),
        'total_fees':         round(total_fees + total_fees_sales, 2),
        'avg_price':          round(avg_price, 4),
        'current_price':      current_price,
        'price_timestamp':    price_timestamp,
        'current_value':      current_value,
        'unrealized_gain':    unrealized_gain,
        'unrealized_pct':     unrealized_pct,
        'realized_gain':      realized_gain,
        'dividends_received': dividends_received,
        'total_return':       total_return,
        'total_return_pct':   total_return_pct,
        'fully_sold':         False,
    }


def get_portfolio_summary(user_id):
    assets     = get_user_assets(user_id)
    stats_list = []
    total_inv  = 0
    total_val  = 0
    total_real = 0
    by_type    = {}

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

    total_unrealized = round(total_val - sum(
        s['shares_held'] * s['avg_price'] for s in stats_list if not s['fully_sold']
    ), 2)
    total_unrealized_pct = round(
        (total_unrealized / total_inv * 100) if total_inv else 0, 2
    )

    allocation = [
        {'ticker': s['ticker'], 'value': s['current_value'], 'type': s['asset_type']}
        for s in stats_list if not s['fully_sold'] and s['current_value'] > 0
    ]

    dca_goal   = get_dca_goal(user_id)
    dca_target = dca_goal['monthly_target'] if dca_goal else 0

    this_month     = date.today().strftime('%Y-%m')
    all_purchases  = get_all_purchases(user_id)
    invested_month = sum(
        p['total_cost'] for p in all_purchases
        if p['date'].startswith(this_month)
    )
    dca_progress    = round((invested_month / dca_target * 100), 1) if dca_target else 0
    total_dividends = get_total_dividends(user_id)

    return {
        'assets':           stats_list,
        'by_type':          by_type,
        'allocation':       allocation,
        'total_invested':   round(total_inv, 2),
        'total_value':      round(total_val, 2),
        'total_unrealized': total_unrealized,
        'unrealized_pct':   total_unrealized_pct,
        'total_realized':   round(total_real, 2),
        'total_dividends':  total_dividends,
        'dca_target':       dca_target,
        'invested_month':   round(invested_month, 2),
        'dca_progress':     dca_progress,
    }


def get_chart_data(asset, purchases, sales):
    """Génère une courbe investi vs. valeur marché avec historique journalier."""
    if not purchases:
        return [], [], []

    # Collecte toutes les transactions
    events = []
    earliest = None
    for p in purchases:
        events.append({'date': p['date'], 'type': 'buy',
                       'shares': p['shares'], 'cost': p['total_cost']})
        if earliest is None or p['date'] < earliest:
            earliest = p['date']
    for s in sales:
        events.append({'date': s['date'], 'type': 'sell',
                       'shares': s['shares'], 'cost': s['total_proceeds']})
    events.sort(key=lambda x: x['date'])

    # Télécharge l'historique journalier
    price_map = {}
    try:
        hist = yf.download(
            asset['ticker'],
            start=earliest,
            end=(date.today() + timedelta(days=1)).strftime('%Y-%m-%d'),
            progress=False, auto_adjust=True
        )
        if not hist.empty:
            closes = hist['Close'].squeeze().dropna()
            price_map = {d.strftime('%Y-%m-%d'): float(v)
                         for d, v in zip(closes.index, closes)}
    except Exception as e:
        print(f"Erreur historique chart {asset['ticker']}: {e}")

    # Fallback : si pas d'historique, utilise les dates de transaction + prix actuel
    if not price_map:
        current_price = get_current_price(asset['ticker'])
        if current_price is None:
            return [], [], []
        dates_out      = []
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
            dates_out.append(e['date'])
            invested_curve.append(round(max(cum_invested, 0), 2))
            market_curve.append(round(max(cum_shares, 0) * current_price, 2))
        return dates_out, invested_curve, market_curve

    # Génère courbe journalière
    all_dates      = sorted(price_map.keys())
    dates_out      = []
    invested_curve = []
    market_curve   = []

    cum_shares   = 0.0
    cum_invested = 0.0
    evt_idx      = 0

    for d in all_dates:
        while evt_idx < len(events) and events[evt_idx]['date'] <= d:
            e = events[evt_idx]
            if e['type'] == 'buy':
                cum_shares   += e['shares']
                cum_invested += e['cost']
            else:
                cum_shares   -= e['shares']
                cum_invested -= e['cost']
            evt_idx += 1

        if cum_invested <= 0:
            continue

        price = price_map[d]
        dates_out.append(d)
        invested_curve.append(round(max(cum_invested, 0), 2))
        market_curve.append(round(max(cum_shares, 0) * price, 2))

    return dates_out, invested_curve, market_curve


def get_portfolio_chart_data(user_id):
    """Génère la courbe portefeuille global avec historique journalier."""
    assets = get_user_assets(user_id)
    if not assets:
        return [], [], []

    # Collecte toutes les transactions
    all_txns = []
    for asset in assets:
        purchases = get_purchases_by_asset(asset['id'], user_id)
        sales     = get_sales_by_asset(asset['id'], user_id)
        for p in purchases:
            all_txns.append({
                'date': p['date'], 'ticker': asset['ticker'],
                'shares': p['shares'], 'cost': p['total_cost']
            })
        for s in sales:
            all_txns.append({
                'date': s['date'], 'ticker': asset['ticker'],
                'shares': -s['shares'], 'cost': -s['total_proceeds']
            })

    if not all_txns:
        return [], [], []

    all_txns.sort(key=lambda x: x['date'])
    earliest   = all_txns[0]['date']
    today_str  = date.today().strftime('%Y-%m-%d')
    tickers    = list(set(t['ticker'] for t in all_txns))

    # Télécharge l'historique journalier pour chaque ticker
    price_history = {}
    for ticker in tickers:
        try:
            hist = yf.download(
                ticker,
                start=earliest,
                end=(date.today() + timedelta(days=1)).strftime('%Y-%m-%d'),
                progress=False, auto_adjust=True
            )
            if not hist.empty:
                closes = hist['Close'].squeeze().dropna()
                price_history[ticker] = {
                    d.strftime('%Y-%m-%d'): float(v)
                    for d, v in zip(closes.index, closes)
                }
        except Exception as e:
            print(f"Erreur historique portefeuille {ticker}: {e}")
            # Fallback : prix actuel seulement
            price = get_current_price(ticker)
            if price:
                price_history[ticker] = {today_str: price}

    if not price_history:
        return [], [], []

    # Toutes les dates de trading disponibles
    all_dates = sorted(set().union(*[set(v.keys()) for v in price_history.values()]))

    dates_out      = []
    invested_out   = []
    market_out     = []

    cum_shares   = {t: 0.0 for t in tickers}
    cum_invested = 0.0
    last_price   = {t: 0.0 for t in tickers}
    txn_idx      = 0

    for d in all_dates:
        # Applique les transactions jusqu'à cette date
        while txn_idx < len(all_txns) and all_txns[txn_idx]['date'] <= d:
            txn = all_txns[txn_idx]
            cum_shares[txn['ticker']] += txn['shares']
            cum_invested += txn['cost']
            txn_idx += 1

        if cum_invested <= 0:
            continue

        # Met à jour les derniers prix connus
        for ticker in tickers:
            if ticker in price_history and d in price_history[ticker]:
                last_price[ticker] = price_history[ticker][d]

        # Calcule la valeur de marché
        market_val = sum(
            max(cum_shares.get(t, 0), 0) * last_price[t]
            for t in tickers
            if last_price[t] > 0
        )

        if market_val > 0:
            dates_out.append(d)
            invested_out.append(round(max(cum_invested, 0), 2))
            market_out.append(round(market_val, 2))

    return dates_out, invested_out, market_out


def get_ticker_info(ticker):
    """Récupère les infos et l'historique d'un ticker Yahoo Finance."""
    try:
        t    = yf.Ticker(ticker)
        info = t.info

        hist = t.history(period='1y', auto_adjust=True)
        if hist.empty:
            return None

        dates  = [d.strftime('%Y-%m-%d') for d in hist.index]
        closes = [round(float(v), 4) for v in hist['Close']]

        return {
            'ticker':              ticker.upper(),
            'name':                info.get('longName') or info.get('shortName') or ticker,
            'currency':            info.get('currency', ''),
            'sector':              info.get('sector') or info.get('category', '—'),
            'market_cap':          info.get('marketCap'),
            'pe_ratio':            info.get('trailingPE'),
            'dividend_yield':      info.get('dividendYield'),
            'current_price':       round(float(info.get('regularMarketPrice') or closes[-1]), 4),
            'previous_close':      info.get('previousClose'),
            'fifty_two_week_high': info.get('fiftyTwoWeekHigh'),
            'fifty_two_week_low':  info.get('fiftyTwoWeekLow'),
            'exchange':            info.get('exchange', ''),
            'asset_type':          info.get('quoteType', ''),
            'dates':               dates,
            'closes':              closes,
        }
    except Exception as e:
        print(f"Erreur get_ticker_info {ticker}: {e}")
        return None


def get_ticker_history(ticker, period='1y'):
    """Récupère uniquement l'historique pour un ticker et une période donnée."""
    try:
        t    = yf.Ticker(ticker)
        hist = t.history(period=period, auto_adjust=True)
        if hist.empty:
            return [], []
        dates  = [d.strftime('%Y-%m-%d') for d in hist.index]
        closes = [round(float(v), 4) for v in hist['Close']]
        return dates, closes
    except Exception as e:
        print(f"Erreur historique {ticker}: {e}")
        return [], []
