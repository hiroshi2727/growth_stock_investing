"""STEP 3/4/6: テクニカル系判定（TT、ボックスブレイク、VCP、RS、出来高）"""
import numpy as np


def check_trend_template(closes):
    """ミネルヴィニ・トレンドテンプレート（全8条件）。
    DUKE。メソッドでもステージ2確認として必須。"""
    if closes is None or len(closes) < 200:
        return None

    close = closes.iloc[-1]
    ma50 = closes.rolling(50).mean().iloc[-1]
    ma150 = closes.rolling(150).mean().iloc[-1]
    ma200 = closes.rolling(200).mean().iloc[-1]
    ma200_1m = closes.rolling(200).mean().iloc[-23] if len(closes) >= 223 else None

    high_52w = closes.tail(252).max()
    low_52w = closes.tail(252).min()
    above_low = (close - low_52w) / low_52w * 100
    below_high = (high_52w - close) / high_52w * 100

    results = {}
    results["TT-01"] = close > ma150
    results["TT-02"] = close > ma200
    results["TT-03"] = ma150 > ma200
    results["TT-04"] = (ma200 > ma200_1m) if ma200_1m is not None else None
    results["TT-05"] = (ma50 > ma150) and (ma50 > ma200)
    results["TT-06"] = close > ma50
    results["TT-07"] = above_low >= 25
    results["TT-08"] = below_high <= 25

    pass_count = sum(1 for v in results.values() if v is not None and bool(v))
    total = sum(1 for v in results.values() if v is not None)
    all_pass = (pass_count == total) and total >= 7

    return {
        "results": results,
        "pass_count": pass_count,
        "total": total,
        "all_pass": all_pass,
        "close": close,
        "ma50": ma50, "ma150": ma150, "ma200": ma200,
        "high_52w": high_52w, "low_52w": low_52w,
        "above_low_pct": above_low, "below_high_pct": below_high,
    }


def detect_box_breakout(closes, volumes):
    """DUKE。メソッドの核心: ボックス圏からのブレイクアウトを検出する。

    BP-01: ボックス圏の形成（値幅 ≤ 15%）
    BP-02: 保ち合い期間（最低15営業日）
    BP-03: 保ち合いの値幅（狭いほど高評価）
    BP-04: ブレイク時の出来高（50日平均の1.5倍以上）
    BP-05: ブレイク初期（ボックス上限から5%以内）
    """
    if closes is None or len(closes) < 60 or volumes is None or len(volumes) < 60:
        return None

    current = closes.iloc[-1]
    avg_vol_50 = volumes.tail(50).mean()
    latest_vol = volumes.iloc[-1]
    vol_ratio = latest_vol / avg_vol_50 if avg_vol_50 > 0 else 0

    best_box = None
    best_score = 0

    for period in [15, 20, 25, 30, 40, 50, 60]:
        if len(closes) < period + 5:
            continue

        for offset in range(1, 6):
            if len(closes) < period + offset:
                continue

            box_data = closes.iloc[-(period + offset):-offset]
            box_high = box_data.max()
            box_low = box_data.min()
            box_range_pct = (box_high - box_low) / box_low * 100

            if box_range_pct > 15:
                continue

            bp01 = True

            if period >= 30:
                bp02_score = 3
            elif period >= 20:
                bp02_score = 2
            elif period >= 15:
                bp02_score = 1
            else:
                bp02_score = 0

            if box_range_pct <= 8:
                bp03_score = 3
            elif box_range_pct <= 12:
                bp03_score = 2
            elif box_range_pct <= 15:
                bp03_score = 1
            else:
                bp03_score = 0

            breakout = current > box_high

            if not breakout:
                continue

            chase_pct = (current - box_high) / box_high * 100
            bp05 = chase_pct <= 5.0

            breakout_vol_ok = False
            for d in range(-5, 0):
                if len(volumes) + d > 0 and len(closes) + d > 0:
                    if closes.iloc[d] > box_high:
                        v = volumes.iloc[d]
                        if v / avg_vol_50 >= 1.5:
                            breakout_vol_ok = True
                            break

            bp04 = breakout_vol_ok

            score = bp02_score + bp03_score
            if bp04:
                score += 3
            if bp05:
                score += 2

            if score > best_score:
                best_score = score
                best_box = {
                    "detected": True,
                    "box_high": float(box_high),
                    "box_low": float(box_low),
                    "box_range_pct": float(box_range_pct),
                    "box_period": period,
                    "chase_pct": float(chase_pct),
                    "bp01": bp01,
                    "bp02_score": bp02_score,
                    "bp03_score": bp03_score,
                    "bp04_volume_breakout": bp04,
                    "bp05_early_entry": bp05,
                    "vol_ratio": float(vol_ratio),
                    "score": score,
                }

    if best_box is None:
        return {
            "detected": False,
            "score": 0,
            "vol_ratio": float(vol_ratio),
        }

    return best_box


def calculate_relative_strength(ticker_closes, nikkei_closes):
    """RS-01/RS-02 [NICE]: レラティブストレングス計算。
    銘柄のリターンを日経平均のリターンと比較。"""
    if ticker_closes is None or nikkei_closes is None:
        return None
    if len(ticker_closes) < 252 or len(nikkei_closes) < 252:
        return None

    stock_ret_12m = (ticker_closes.iloc[-1] / ticker_closes.iloc[-252] - 1) * 100
    nikkei_ret_12m = (nikkei_closes.iloc[-1] / nikkei_closes.iloc[-252] - 1) * 100

    stock_ret_6m = (ticker_closes.iloc[-1] / ticker_closes.iloc[-126] - 1) * 100
    nikkei_ret_6m = (nikkei_closes.iloc[-1] / nikkei_closes.iloc[-126] - 1) * 100

    stock_ret_3m = (ticker_closes.iloc[-1] / ticker_closes.iloc[-63] - 1) * 100
    nikkei_ret_3m = (nikkei_closes.iloc[-1] / nikkei_closes.iloc[-63] - 1) * 100

    rs_raw = (stock_ret_3m - nikkei_ret_3m) * 0.4 + \
             (stock_ret_6m - nikkei_ret_6m) * 0.3 + \
             (stock_ret_12m - nikkei_ret_12m) * 0.3

    rs_3m_ago = None
    if len(ticker_closes) >= 315 and len(nikkei_closes) >= 315:
        s_ret_12m_old = (ticker_closes.iloc[-64] / ticker_closes.iloc[-315] - 1) * 100 if len(ticker_closes) >= 315 else None
        n_ret_12m_old = (nikkei_closes.iloc[-64] / nikkei_closes.iloc[-315] - 1) * 100 if len(nikkei_closes) >= 315 else None
        if s_ret_12m_old is not None and n_ret_12m_old is not None:
            rs_3m_ago = s_ret_12m_old - n_ret_12m_old

    rs_improving = None
    if rs_3m_ago is not None:
        rs_improving = rs_raw > rs_3m_ago

    return {
        "rs_raw": float(rs_raw),
        "stock_ret_12m": float(stock_ret_12m),
        "stock_ret_6m": float(stock_ret_6m),
        "stock_ret_3m": float(stock_ret_3m),
        "rs_improving": rs_improving,
    }


def detect_vcp(closes, volumes):
    """VCP-01~06 [NICE]: ボラティリティ収縮パターン検出。
    連続する収縮の振幅が段階的に減少しているパターンを探す。"""
    if closes is None or len(closes) < 60 or volumes is None or len(volumes) < 60:
        return None

    try:
        close_values = closes.values.astype(float)
        order = 5
        local_max_idx = []
        local_min_idx = []
        for i in range(order, len(close_values) - order):
            if all(close_values[i] > close_values[i-j] for j in range(1, order+1)) and \
               all(close_values[i] > close_values[i+j] for j in range(1, order+1)):
                local_max_idx.append(i)
            if all(close_values[i] < close_values[i-j] for j in range(1, order+1)) and \
               all(close_values[i] < close_values[i+j] for j in range(1, order+1)):
                local_min_idx.append(i)
    except Exception:
        return None

    if len(local_max_idx) < 2 or len(local_min_idx) < 2:
        return None

    cutoff = max(0, len(closes) - 120)
    recent_max = [(i, close_values[i]) for i in local_max_idx if i >= cutoff]
    recent_min = [(i, close_values[i]) for i in local_min_idx if i >= cutoff]

    if len(recent_max) < 2 or len(recent_min) < 1:
        return None

    contractions = []
    for i in range(len(recent_max) - 1):
        peak_val = recent_max[i][1]
        trough_val = None
        for mi, mv in recent_min:
            if recent_max[i][0] < mi < recent_max[i+1][0]:
                if trough_val is None or mv < trough_val:
                    trough_val = mv
        if trough_val is not None and peak_val > 0:
            drawdown = (peak_val - trough_val) / peak_val * 100
            contractions.append({
                "peak_idx": recent_max[i][0],
                "peak_val": peak_val,
                "trough_val": trough_val,
                "drawdown_pct": drawdown,
            })

    if len(contractions) < 2:
        return None

    vcp01 = len(contractions) >= 2

    vcp02 = True
    for i in range(1, len(contractions)):
        if contractions[i]["drawdown_pct"] > contractions[i-1]["drawdown_pct"] * 0.85:
            vcp02 = False
            break

    vol_values = volumes.values.astype(float)
    vcp03 = True
    for i in range(1, len(contractions)):
        prev_start = contractions[i-1]["peak_idx"]
        curr_start = contractions[i]["peak_idx"]
        if curr_start <= prev_start:
            continue
        prev_avg_vol = np.mean(vol_values[prev_start:curr_start]) if curr_start > prev_start else 0
        curr_end = contractions[i]["peak_idx"] + 10 if i == len(contractions) - 1 else contractions[i+1]["peak_idx"] if i + 1 < len(contractions) else len(vol_values)
        curr_end = min(curr_end, len(vol_values))
        curr_avg_vol = np.mean(vol_values[curr_start:curr_end]) if curr_end > curr_start else 0
        if prev_avg_vol > 0 and curr_avg_vol > prev_avg_vol:
            vcp03 = False
            break

    pivot = recent_max[-1][1] if recent_max else None

    vcp_score = 0
    if vcp01:
        vcp_score += 2
    if vcp02:
        vcp_score += 3
    if vcp03:
        vcp_score += 2

    return {
        "detected": vcp01 and vcp02,
        "contraction_count": len(contractions),
        "contractions": [{"drawdown_pct": c["drawdown_pct"]} for c in contractions],
        "vcp01": vcp01,
        "vcp02_decreasing": vcp02,
        "vcp03_vol_declining": vcp03,
        "pivot_point": float(pivot) if pivot else None,
        "score": vcp_score,
    }


def check_volume(volumes):
    if volumes is None or len(volumes) < 50:
        return None

    avg50 = volumes.tail(50).mean()
    latest = volumes.iloc[-1]
    ratio = latest / avg50 if avg50 > 0 else 0

    recent_ratios = []
    for i in range(-5, 0):
        v = volumes.iloc[i]
        recent_ratios.append(round(float(v / avg50) if avg50 > 0 else 0, 2))

    return {
        "latest": int(latest),
        "avg50": int(avg50),
        "ratio": float(ratio),
        "recent_5d": recent_ratios,
        "breakout": ratio >= 1.5,
    }
