"""STEP 1: 市場環境チェック（日経平均 ^N225）"""
import yfinance as yf
import numpy as np

NIKKEI_TICKER = "^N225"


def check_market(nikkei_data=None):
    """市場環境判定。M-01(MUST), M-03/M-04(MUST), M-05(NICE), M-06(NICE)"""
    print("=" * 70)
    print("STEP 1: 市場環境チェック（日経平均 ^N225）")
    print("=" * 70)

    if nikkei_data is None:
        t = yf.Ticker(NIKKEI_TICKER)
        df = t.history(period="2y", auto_adjust=True)
    else:
        df = nikkei_data

    if df.empty:
        print("  日経平均データ取得失敗")
        return None

    close = df['Close'].iloc[-1]
    ma200 = df['Close'].rolling(200).mean().iloc[-1]
    deviation = (close - ma200) / ma200 * 100
    date = df.index[-1].strftime("%Y-%m-%d")

    m01 = close > ma200

    recent = df.tail(26)
    dist_days = 0
    for i in range(1, len(recent)):
        pct = (recent.iloc[i]['Close'] - recent.iloc[i-1]['Close']) / recent.iloc[i-1]['Close'] * 100
        vol_up = recent.iloc[i]['Volume'] > recent.iloc[i-1]['Volume']
        if pct <= -0.2 and vol_up:
            dist_days += 1

    ftd = detect_follow_through_day(df)

    print(f"  データ最終日: {date}")
    print(f"  日経平均終値: {close:,.0f}円")
    print(f"  200日移動平均: {ma200:,.0f}円")
    print(f"  乖離率: {deviation:+.1f}%")
    print(f"  [MUST] M-01（指数 > 200日MA）: {'PASS 買い可' if m01 else 'FAIL 新規買い停止'}")
    print(f"  [NICE] M-05 ディストリビューションデー（直近25日）: {dist_days}日", end="")
    if dist_days >= 4:
        print(f" [警告]")
    else:
        print(f" OK")
    print(f"  [NICE] M-06 フォロースルーデー: {'検出あり' if ftd['detected'] else '該当なし'}")
    if ftd['detected']:
        print(f"         日付: {ftd['date']}, 上昇率: {ftd['gain_pct']:+.1f}%")

    return {
        "date": date, "close": close, "ma200": ma200,
        "deviation_pct": deviation,
        "m01_buy": m01,
        "distribution_days": dist_days,
        "m05_warning": dist_days >= 4,
        "ftd": ftd,
    }


def detect_follow_through_day(df):
    """M-06 [NICE]: フォロースルーデー検出
    下落後の反発開始から4~7日目に、前日比+1.5%以上を出来高増加で上昇"""
    closes = df['Close'].values
    volumes = df['Volume'].values
    n = len(closes)

    if n < 30:
        return {"detected": False}

    lookback = min(60, n - 1)
    recent = df.tail(lookback)

    recent_closes = recent['Close'].values
    min_idx = np.argmin(recent_closes)

    for day_offset in range(4, min(8, len(recent_closes) - min_idx)):
        idx = min_idx + day_offset
        if idx >= len(recent_closes) or idx < 1:
            continue
        gain = (recent_closes[idx] - recent_closes[idx-1]) / recent_closes[idx-1] * 100
        vol_cur = recent['Volume'].values[idx]
        vol_prev = recent['Volume'].values[idx-1]
        if gain >= 1.5 and vol_cur > vol_prev:
            return {
                "detected": True,
                "date": recent.index[idx].strftime("%Y-%m-%d"),
                "gain_pct": gain,
            }

    return {"detected": False}
