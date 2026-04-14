"""STEP 2: 一括データ取得 + 52週新高値スキャン"""
import yfinance as yf
import pandas as pd

from universe import load_universe

UNIVERSE = load_universe()


def scan_new_highs():
    print(f"\n{'=' * 70}")
    print("STEP 2: 52週新高値スキャン")
    print(f"{'=' * 70}")
    print(f"  ユニバース: {len(UNIVERSE)}銘柄")
    print(f"  データ取得中（一括ダウンロード）...")

    tickers = list(UNIVERSE.keys())
    data = yf.download(tickers, period="2y", auto_adjust=True, progress=False, threads=True)

    if data.empty:
        print("  データ取得失敗")
        return [], data, {"new_high_recent": 0, "new_high_prev": 0, "m03_increasing": False}

    print(f"  データ取得完了: {data.index[0].strftime('%Y-%m-%d')} ~ {data.index[-1].strftime('%Y-%m-%d')}")

    new_high_stocks = []
    no_data_count = 0
    checked_count = 0
    new_high_count_recent = 0
    new_high_count_prev = 0

    for ticker in tickers:
        try:
            if isinstance(data.columns, pd.MultiIndex):
                closes = data['Close'][ticker].dropna()
            else:
                closes = data['Close'].dropna()

            if len(closes) < 60:
                no_data_count += 1
                continue

            checked_count += 1
            current = closes.iloc[-1]
            lookback = min(252, len(closes) - 1)
            prev_high = closes.iloc[-(lookback+1):-1].max()

            if lookback > 10:
                recent_5d_high = closes.iloc[-6:-1].max()
                prev_5d_high = closes.iloc[-11:-6].max() if len(closes) > 15 else None
                lb5 = min(252, len(closes) - 6)
                if lb5 > 0:
                    ph5 = closes.iloc[max(0, -6-lb5):-6].max()
                    if recent_5d_high >= ph5:
                        new_high_count_recent += 1
                if prev_5d_high is not None and len(closes) > 15:
                    lb10 = min(252, len(closes) - 11)
                    if lb10 > 0:
                        ph10 = closes.iloc[max(0, -11-lb10):-11].max()
                        if prev_5d_high >= ph10:
                            new_high_count_prev += 1

            if current >= prev_high:
                low_52w = closes.tail(min(252, len(closes))).min()
                above_low_pct = (current - low_52w) / low_52w * 100

                new_high_stocks.append({
                    "ticker": ticker,
                    "name": UNIVERSE[ticker],
                    "close": current,
                    "prev_high_52w": prev_high,
                    "diff_pct": (current - prev_high) / prev_high * 100,
                    "low_52w": low_52w,
                    "above_low_pct": above_low_pct,
                    "data_days": len(closes),
                })
        except Exception:
            no_data_count += 1
            continue

    print(f"  チェック完了: {checked_count}銘柄（データ不足スキップ: {no_data_count}）")
    print(f"  52週新高値更新: {len(new_high_stocks)}銘柄")
    print(f"  [MUST] M-03/M-04 新高値銘柄数: 直近5日={new_high_count_recent}, 前5日={new_high_count_prev}", end="")
    if new_high_count_recent >= new_high_count_prev:
        print(" → 増加傾向 OK")
    else:
        print(" → 減少傾向 [注意]")

    if new_high_stocks:
        print(f"\n  {'コード':<8} {'銘柄名':<20} {'終値':>10} {'52週高値':>10} {'差':>8}")
        print(f"  {'─'*58}")
        for s in sorted(new_high_stocks, key=lambda x: -x['diff_pct']):
            code = s['ticker'].replace('.T', '')
            print(f"  {code:<8} {s['name']:<20} {s['close']:>10,.0f} {s['prev_high_52w']:>10,.0f} {s['diff_pct']:>+7.1f}%")

    return new_high_stocks, data, {
        "new_high_recent": new_high_count_recent,
        "new_high_prev": new_high_count_prev,
        "m03_increasing": new_high_count_recent >= new_high_count_prev,
    }
