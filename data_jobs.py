"""
Collecte automatique de données fondamentales pour le screener et les scores qualité.
Tourne en arrière-plan via APScheduler, sans contexte Flask requis.
"""

import time
import logging
import yfinance as yf

from database import get_db, is_postgres, placeholder
from universe import UNIVERSE_ALL

log = logging.getLogger("data_jobs")


# ── Fetch ─────────────────────────────────────────────────────────────────────

def fetch_fundamentals(ticker: str) -> dict | None:
    """
    Récupère les données fondamentales d'un ticker via yfinance.
    Retourne un dict prêt pour save_fundamentals_to_db, ou None si échec.
    """
    try:
        t = yf.Ticker(ticker)
        info = t.info or {}

        if not info.get("symbol"):
            return None

        # ── Prix & marché ──────────────────────────────────────────────────
        current_price   = info.get("currentPrice") or info.get("regularMarketPrice")
        week52_high     = info.get("fiftyTwoWeekHigh")
        week52_low      = info.get("fiftyTwoWeekLow")
        market_cap      = info.get("marketCap")
        shares_out      = info.get("sharesOutstanding")
        currency        = info.get("currency", "USD")
        sector          = info.get("sector", "")
        industry        = info.get("industry", "")
        country         = info.get("country", "")
        name            = info.get("shortName") or info.get("longName") or ticker

        # ── Valorisation ──────────────────────────────────────────────────
        pe_trailing     = info.get("trailingPE")
        pe_forward      = info.get("forwardPE")
        pb_ratio        = info.get("priceToBook")
        ps_ratio        = info.get("priceToSalesTrailing12Months")
        ev_ebitda       = info.get("enterpriseToEbitda")
        peg_ratio       = info.get("pegRatio")

        # ── Croissance ────────────────────────────────────────────────────
        revenue_growth  = info.get("revenueGrowth")        # YoY
        earnings_growth = info.get("earningsGrowth")       # YoY

        # ── Rentabilité ───────────────────────────────────────────────────
        roe             = info.get("returnOnEquity")
        roa             = info.get("returnOnAssets")
        profit_margin   = info.get("profitMargins")
        operating_margin= info.get("operatingMargins")
        gross_margin    = info.get("grossMargins")

        # ── Bilan ─────────────────────────────────────────────────────────
        debt_to_equity  = info.get("debtToEquity")
        current_ratio   = info.get("currentRatio")
        quick_ratio     = info.get("quickRatio")
        total_debt      = info.get("totalDebt")
        total_cash      = info.get("totalCash")

        # ── Dividendes ────────────────────────────────────────────────────
        dividend_yield  = info.get("dividendYield")
        payout_ratio    = info.get("payoutRatio")

        # ── FCF ───────────────────────────────────────────────────────────
        free_cashflow   = info.get("freeCashflow")
        operating_cf    = info.get("operatingCashflow")

        # ── Analystes ─────────────────────────────────────────────────────
        target_mean     = info.get("targetMeanPrice")
        analyst_upside  = None
        if target_mean and current_price and current_price > 0:
            analyst_upside = (target_mean - current_price) / current_price

        # ── Métriques dérivées ────────────────────────────────────────────
        price_vs_52w_high = None
        if current_price and week52_high and week52_high > 0:
            price_vs_52w_high = current_price / week52_high  # 1.0 = au plus haut

        price_to_fcf = None
        if current_price and free_cashflow and shares_out and shares_out > 0:
            fcf_per_share = free_cashflow / shares_out
            if fcf_per_share > 0:
                price_to_fcf = current_price / fcf_per_share

        quality_score = calculate_quality_score({
            "pe_trailing": pe_trailing,
            "pe_forward": pe_forward,
            "pb_ratio": pb_ratio,
            "revenue_growth": revenue_growth,
            "earnings_growth": earnings_growth,
            "roe": roe,
            "profit_margin": profit_margin,
            "debt_to_equity": debt_to_equity,
            "current_ratio": current_ratio,
            "free_cashflow": free_cashflow,
        })

        return {
            "ticker": ticker,
            "name": name,
            "sector": sector,
            "industry": industry,
            "country": country,
            "currency": currency,
            "current_price": current_price,
            "market_cap": market_cap,
            "week52_high": week52_high,
            "week52_low": week52_low,
            "price_vs_52w_high": price_vs_52w_high,
            "pe_trailing": pe_trailing,
            "pe_forward": pe_forward,
            "pb_ratio": pb_ratio,
            "ps_ratio": ps_ratio,
            "ev_ebitda": ev_ebitda,
            "peg_ratio": peg_ratio,
            "revenue_growth": revenue_growth,
            "earnings_growth": earnings_growth,
            "roe": roe,
            "roa": roa,
            "profit_margin": profit_margin,
            "operating_margin": operating_margin,
            "gross_margin": gross_margin,
            "debt_to_equity": debt_to_equity,
            "current_ratio": current_ratio,
            "quick_ratio": quick_ratio,
            "total_debt": total_debt,
            "total_cash": total_cash,
            "free_cashflow": free_cashflow,
            "operating_cashflow": operating_cf,
            "price_to_fcf": price_to_fcf,
            "dividend_yield": dividend_yield,
            "payout_ratio": payout_ratio,
            "target_mean_price": target_mean,
            "analyst_upside": analyst_upside,
            "quality_score": quality_score,
        }

    except Exception as e:
        log.warning("fetch_fundamentals(%s) error: %s", ticker, e)
        return None


# ── Score qualité ─────────────────────────────────────────────────────────────

def calculate_quality_score(d: dict) -> int:
    """
    Score 0–100 sur 4 axes × 25 pts.

    Axe 1 — Valorisation (PE, PB)
    Axe 2 — Croissance (rev_growth, earnings_growth)
    Axe 3 — Rentabilité (ROE, profit_margin)
    Axe 4 — Bilan (D/E, current_ratio, FCF positif)
    """
    score = 0

    # ── Axe 1 : Valorisation (25 pts) ─────────────────────────────────────
    val_score = 0
    pe = d.get("pe_trailing") or d.get("pe_forward")
    if pe is not None and pe > 0:
        if pe < 15:
            val_score += 15
        elif pe < 25:
            val_score += 10
        elif pe < 35:
            val_score += 5
    else:
        val_score += 8  # neutre si donnée absente

    pb = d.get("pb_ratio")
    if pb is not None and pb > 0:
        if pb < 1.5:
            val_score += 10
        elif pb < 3:
            val_score += 7
        elif pb < 5:
            val_score += 4
    else:
        val_score += 5

    score += min(25, val_score)

    # ── Axe 2 : Croissance (25 pts) ───────────────────────────────────────
    growth_score = 0
    rev_g = d.get("revenue_growth")
    if rev_g is not None:
        if rev_g > 0.20:
            growth_score += 13
        elif rev_g > 0.10:
            growth_score += 10
        elif rev_g > 0.05:
            growth_score += 7
        elif rev_g > 0:
            growth_score += 4
    else:
        growth_score += 6

    earn_g = d.get("earnings_growth")
    if earn_g is not None:
        if earn_g > 0.20:
            growth_score += 12
        elif earn_g > 0.10:
            growth_score += 9
        elif earn_g > 0.05:
            growth_score += 6
        elif earn_g > 0:
            growth_score += 3
    else:
        growth_score += 6

    score += min(25, growth_score)

    # ── Axe 3 : Rentabilité (25 pts) ──────────────────────────────────────
    rent_score = 0
    roe = d.get("roe")
    if roe is not None:
        if roe > 0.25:
            rent_score += 13
        elif roe > 0.15:
            rent_score += 10
        elif roe > 0.08:
            rent_score += 6
        elif roe > 0:
            rent_score += 3
    else:
        rent_score += 6

    margin = d.get("profit_margin")
    if margin is not None:
        if margin > 0.20:
            rent_score += 12
        elif margin > 0.10:
            rent_score += 9
        elif margin > 0.05:
            rent_score += 6
        elif margin > 0:
            rent_score += 3
    else:
        rent_score += 6

    score += min(25, rent_score)

    # ── Axe 4 : Bilan (25 pts) ────────────────────────────────────────────
    bilan_score = 0
    dte = d.get("debt_to_equity")
    if dte is not None:
        if dte < 30:
            bilan_score += 10
        elif dte < 80:
            bilan_score += 7
        elif dte < 150:
            bilan_score += 4
    else:
        bilan_score += 5

    cr = d.get("current_ratio")
    if cr is not None:
        if cr > 2:
            bilan_score += 8
        elif cr > 1.5:
            bilan_score += 6
        elif cr > 1:
            bilan_score += 3
    else:
        bilan_score += 4

    fcf = d.get("free_cashflow")
    if fcf is not None:
        if fcf > 0:
            bilan_score += 7

    score += min(25, bilan_score)

    return min(100, max(0, score))


# ── Sauvegarde DB ─────────────────────────────────────────────────────────────

def save_fundamentals_to_db(data: dict) -> None:
    conn = get_db()
    c = conn.cursor()
    p = placeholder

    cols = [
        "ticker", "name", "sector", "industry", "country", "currency",
        "current_price", "market_cap", "week52_high", "week52_low", "price_vs_52w_high",
        "pe_trailing", "pe_forward", "pb_ratio", "ps_ratio", "ev_ebitda", "peg_ratio",
        "revenue_growth", "earnings_growth",
        "roe", "roa", "profit_margin", "operating_margin", "gross_margin",
        "debt_to_equity", "current_ratio", "quick_ratio", "total_debt", "total_cash",
        "free_cashflow", "operating_cashflow", "price_to_fcf",
        "dividend_yield", "payout_ratio",
        "target_mean_price", "analyst_upside",
        "quality_score",
    ]
    values = [data.get(c) for c in cols]

    if is_postgres():
        set_clause = ", ".join(
            f"{col} = EXCLUDED.{col}" for col in cols if col != "ticker"
        )
        placeholders = ", ".join([p(i + 1) for i in range(len(cols))])
        sql = f"""
            INSERT INTO fundamentals ({', '.join(cols)})
            VALUES ({placeholders})
            ON CONFLICT (ticker) DO UPDATE SET {set_clause},
                updated_at = NOW()
        """
    else:
        placeholders = ", ".join(["?" for _ in cols])
        sql = f"INSERT OR REPLACE INTO fundamentals ({', '.join(cols)}) VALUES ({placeholders})"

    c.execute(sql, values)
    conn.commit()
    conn.close()


# ── Job principal ─────────────────────────────────────────────────────────────

def refresh_all_fundamentals() -> None:
    """
    Parcourt UNIVERSE_ALL et met à jour les fondamentaux de chaque ticker.
    Appelé par le scheduler — ~160 tickers × ~1s = ~3 min.
    """
    log.info("refresh_all_fundamentals: démarrage (%d tickers)", len(UNIVERSE_ALL))
    ok, failed = 0, 0

    for ticker in UNIVERSE_ALL:
        try:
            data = fetch_fundamentals(ticker)
            if data:
                save_fundamentals_to_db(data)
                ok += 1
            else:
                failed += 1
        except Exception as e:
            log.warning("refresh_all_fundamentals: %s → %s", ticker, e)
            failed += 1
        time.sleep(0.5)

    log.info("refresh_all_fundamentals: terminé — %d ok, %d échoués", ok, failed)
