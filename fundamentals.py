"""STEP 5b: ファンダメンタルズ [MUST: F-01~F-05] + CAN-SLIM NICE条件"""
import numpy as np
import yfinance as yf

from bigchange import analyze_bigchange_news
from scanner import UNIVERSE


def _extract_income_series(df, revenue_keys=None, earnings_keys=None):
    """DataFrameから売上・利益のSeriesを抽出するヘルパー。"""
    if df is None or df.empty:
        return None, None
    rev_keys = revenue_keys or ['Total Revenue', 'Operating Revenue']
    earn_keys = earnings_keys or ['Net Income', 'Operating Income']
    revenue = None
    earnings = None
    for k in rev_keys:
        if k in df.index:
            s = df.loc[k].dropna()
            if len(s) >= 2:
                revenue = s
                break
    for k in earn_keys:
        if k in df.index:
            s = df.loc[k].dropna()
            if len(s) >= 2:
                earnings = s
                break
    return revenue, earnings


def _yoy_growth(series):
    """時系列データ（新しい順）から直近の前年同期比成長率を計算する。"""
    if series is None or len(series) < 2:
        return None, None

    dates = sorted(series.index, reverse=True)
    vals = [series[d] for d in dates]

    if len(dates) >= 2:
        gap_days = abs((dates[0] - dates[1]).days)
    else:
        gap_days = 365

    if gap_days < 200 and len(vals) >= 5:
        current, year_ago = vals[0], vals[4]
        if year_ago is not None and year_ago != 0 and not np.isnan(year_ago) and not np.isnan(current):
            return (current - year_ago) / abs(year_ago), "四半期YoY"
        return None, None
    else:
        current, prev = vals[0], vals[1]
        if prev is not None and prev != 0 and not np.isnan(prev) and not np.isnan(current):
            return (current - prev) / abs(prev), "年次YoY"
        return None, None


def evaluate_earnings_acceleration(quarterly_data):
    """四半期データから成長加速・減速を判定する。"""
    if quarterly_data is None or len(quarterly_data) < 5:
        return "unknown"

    values = quarterly_data.values
    if len(values) < 5:
        return "unknown"

    growth_rates = []
    for i in range(min(3, len(values) - 4)):
        current = values[i]
        year_ago = values[i + 4] if i + 4 < len(values) else None
        if year_ago is not None and year_ago != 0:
            gr = (current - year_ago) / abs(year_ago)
            growth_rates.append(gr)

    if len(growth_rates) < 2:
        return "unknown"

    if growth_rates[0] > growth_rates[1]:
        return "accelerating"
    elif abs(growth_rates[0] - growth_rates[1]) < 0.05:
        return "stable"
    else:
        return "decelerating"


def evaluate_earnings_acceleration_annual(annual_data):
    """年次データから成長加速・減速を判定する（四半期データ不足時のフォールバック）。"""
    if annual_data is None or len(annual_data) < 3:
        return "unknown"

    dates = sorted(annual_data.index, reverse=True)
    vals = [annual_data[d] for d in dates]

    growth_rates = []
    for i in range(min(2, len(vals) - 1)):
        current, prev = vals[i], vals[i + 1]
        if prev is not None and prev != 0 and not np.isnan(prev) and not np.isnan(current):
            growth_rates.append((current - prev) / abs(prev))

    if len(growth_rates) < 2:
        return "unknown"

    if growth_rates[0] > growth_rates[1]:
        return "accelerating"
    elif abs(growth_rates[0] - growth_rates[1]) < 0.05:
        return "stable"
    else:
        return "decelerating"


def get_fundamentals(ticker):
    """ファンダメンタルズデータを取得し、MUST条件を評価する。"""
    try:
        t = yf.Ticker(ticker)
        info = t.info

        quarterly_earnings = None
        quarterly_revenue = None
        annual_earnings = None
        annual_revenue = None

        try:
            qf = t.quarterly_income_stmt
            if qf is not None and not qf.empty:
                quarterly_revenue, quarterly_earnings = _extract_income_series(qf)
        except Exception:
            pass

        try:
            af = t.income_stmt
            if af is not None and not af.empty:
                annual_revenue, annual_earnings = _extract_income_series(af)
        except Exception:
            pass

        earnings_growth = info.get("earningsGrowth")
        revenue_growth = info.get("revenueGrowth")
        eg_source = "info" if earnings_growth is not None else None
        rg_source = "info" if revenue_growth is not None else None

        if earnings_growth is None:
            eg, eg_label = _yoy_growth(quarterly_earnings)
            if eg is not None:
                earnings_growth = eg
                eg_source = eg_label
        if earnings_growth is None:
            eg, eg_label = _yoy_growth(annual_earnings)
            if eg is not None:
                earnings_growth = eg
                eg_source = eg_label

        if revenue_growth is None:
            rg, rg_label = _yoy_growth(quarterly_revenue)
            if rg is not None:
                revenue_growth = rg
                rg_source = rg_label
        if revenue_growth is None:
            rg, rg_label = _yoy_growth(annual_revenue)
            if rg is not None:
                revenue_growth = rg
                rg_source = rg_label

        earnings_acceleration = evaluate_earnings_acceleration(quarterly_earnings)
        if earnings_acceleration == "unknown":
            earnings_acceleration = evaluate_earnings_acceleration_annual(annual_earnings)
        revenue_acceleration = evaluate_earnings_acceleration(quarterly_revenue)
        if revenue_acceleration == "unknown":
            revenue_acceleration = evaluate_earnings_acceleration_annual(annual_revenue)

        market_cap = info.get("marketCap")
        roe = info.get("returnOnEquity")
        pe = info.get("trailingPE")
        forward_pe = info.get("forwardPE")
        profit_margins = info.get("profitMargins")
        shares = info.get("sharesOutstanding")

        f01_pass = earnings_growth is not None and earnings_growth >= 0.20
        f01_score = 0
        if earnings_growth is not None:
            if earnings_growth >= 0.60:
                f01_score = 3
            elif earnings_growth >= 0.40:
                f01_score = 2
            elif earnings_growth >= 0.20:
                f01_score = 1

        f02_pass = revenue_growth is not None and revenue_growth >= 0.10
        f02_score = 0
        if revenue_growth is not None:
            if revenue_growth >= 0.50:
                f02_score = 3
            elif revenue_growth >= 0.25:
                f02_score = 2
            elif revenue_growth >= 0.10:
                f02_score = 1

        f03_score = 0
        if earnings_acceleration == "accelerating":
            f03_score = 2
        elif earnings_acceleration == "stable":
            f03_score = 1

        f04_score = 0
        f04_label = "N/A"
        if market_cap is not None:
            mc_oku = market_cap / 1e8
            if mc_oku < 300:
                f04_score = 3
                f04_label = "小型"
            elif mc_oku < 1000:
                f04_score = 2
                f04_label = "中型"
            else:
                f04_score = 1
                f04_label = "大型"

        name = UNIVERSE.get(ticker, "")
        bigchange = analyze_bigchange_news(ticker, name)

        profitable = pe is not None and pe > 0

        must_score = f01_score + f02_score + f03_score + f04_score

        return {
            "market_cap": market_cap,
            "market_cap_label": f04_label,
            "pe_trailing": pe,
            "pe_forward": forward_pe,
            "roe": roe,
            "revenue_growth": revenue_growth,
            "earnings_growth": earnings_growth,
            "profit_margins": profit_margins,
            "shares_outstanding": shares,
            "profitable": profitable,
            "earnings_acceleration": earnings_acceleration,
            "revenue_acceleration": revenue_acceleration,
            "eg_source": eg_source,
            "rg_source": rg_source,
            "f01_pass": f01_pass, "f01_score": f01_score,
            "f02_pass": f02_pass, "f02_score": f02_score,
            "f03_score": f03_score,
            "f04_score": f04_score, "f04_label": f04_label,
            "sector": info.get("sector"),
            "industry": info.get("industry"),
            "bigchange": bigchange,
            "must_fund_score": must_score,
            "must_fund_pass": f01_pass and f02_pass,
        }
    except Exception as e:
        return {"error": str(e), "must_fund_pass": False, "must_fund_score": 0}


def evaluate_canslim_nice(fund_data):
    """CAN-SLIM補完条件 [NICE] のスコアリング。"""
    score = 0
    details = {}

    roe = fund_data.get("roe")
    if roe is not None:
        if roe >= 0.25:
            score += 3
            details["CS-04_ROE"] = f"{roe*100:.1f}% (25%以上 +3pt)"
        elif roe >= 0.17:
            score += 2
            details["CS-04_ROE"] = f"{roe*100:.1f}% (17%以上 +2pt)"
        else:
            details["CS-04_ROE"] = f"{roe*100:.1f}% (17%未満)"
    else:
        details["CS-04_ROE"] = "N/A"

    shares = fund_data.get("shares_outstanding")
    if shares is not None:
        if shares <= 25_000_000:
            score += 2
            details["CS-06_Shares"] = f"{shares/1e6:.1f}M (少 +2pt)"
        elif shares <= 100_000_000:
            score += 1
            details["CS-06_Shares"] = f"{shares/1e6:.1f}M (+1pt)"
        else:
            details["CS-06_Shares"] = f"{shares/1e6:.1f}M"
    else:
        details["CS-06_Shares"] = "N/A"

    return {"score": score, "details": details}
