#!/usr/bin/env python3
"""
新高値ブレイク投資法 スクリーニングスクリプト v2（優先度スコアリング版）

DUKE。メソッド = MUST（必須条件）
ミネルヴィニ / オニール = NICE（ボーナス加点）

判定フロー:
  STEP 1: 市場環境チェック (M-01~M-06)
  STEP 2: 52週新高値スキャン
  STEP 3: トレンドテンプレート (TT-01~08) [MUST]
  STEP 4: ボックスブレイク検出 (BP-01~05) [MUST]
  STEP 5: ファンダメンタルズ (F-01~F-05) [MUST]
  STEP 6: NICE条件（RS, VCP, CAN-SLIM補完）
  STEP 7: 総合スコアリング・Markdownレポート出力
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ========================================
# 銘柄ユニバース
# ========================================

UNIVERSE = {
    # === 電気機器・半導体 ===
    "6501.T": "日立製作所", "6502.T": "東芝", "6503.T": "三菱電機",
    "6504.T": "富士電機", "6506.T": "安川電機", "6594.T": "日本電産",
    "6645.T": "オムロン", "6701.T": "NEC", "6702.T": "富士通",
    "6723.T": "ルネサスエレクトロニクス", "6724.T": "セイコーエプソン",
    "6752.T": "パナソニック", "6753.T": "シャープ", "6758.T": "ソニーG",
    "6762.T": "TDK", "6770.T": "アルプスアルパイン", "6857.T": "アドバンテスト",
    "6861.T": "キーエンス", "6902.T": "デンソー", "6920.T": "レーザーテック",
    "6923.T": "スタンレー電気", "6954.T": "ファナック", "6971.T": "京セラ",
    "6976.T": "太陽誘電", "7735.T": "SCREENホールディングス",
    "7741.T": "HOYA", "7751.T": "キヤノン", "7752.T": "リコー",
    "8035.T": "東京エレクトロン", "6526.T": "ソシオネクスト",
    "285A.T": "キオクシア",
    # === 機械 ===
    "6103.T": "オークマ", "6113.T": "アマダ", "6135.T": "牧野フライス製作所",
    "6141.T": "DMG森精機", "6301.T": "小松製作所", "6302.T": "住友重機械",
    "6305.T": "日立建機", "6326.T": "クボタ", "6361.T": "荏原製作所",
    "6367.T": "ダイキン工業", "6471.T": "日本精工", "7004.T": "日立造船",
    "7011.T": "三菱重工業", "7012.T": "川崎重工業", "7013.T": "IHI",
    # === 自動車・輸送用機器 ===
    "7201.T": "日産自動車", "7202.T": "いすゞ自動車", "7203.T": "トヨタ自動車",
    "7211.T": "三菱自動車", "7261.T": "マツダ", "7267.T": "ホンダ",
    "7269.T": "スズキ", "7270.T": "SUBARU", "7309.T": "シマノ",
    # === 鉄鋼・非鉄金属 ===
    "3436.T": "SUMCO", "5401.T": "日本製鉄", "5406.T": "神戸製鋼所",
    "5411.T": "JFEホールディングス", "5713.T": "住友金属鉱山",
    "5714.T": "DOWAホールディングス", "5801.T": "古河電気工業",
    "5802.T": "住友電気工業", "5803.T": "フジクラ", "5805.T": "SWCC",
    # === 化学・素材 ===
    "3401.T": "帝人", "3402.T": "東レ", "3407.T": "旭化成",
    "4004.T": "レゾナック", "4005.T": "住友化学", "4021.T": "日産化学",
    "4042.T": "東ソー", "4043.T": "トクヤマ", "4063.T": "信越化学工業",
    "4183.T": "三井化学", "4188.T": "三菱ケミカルG",
    "4208.T": "UBE", "4452.T": "花王", "4901.T": "富士フイルム",
    "4911.T": "資生堂", "6988.T": "日東電工",
    # === 電子材料・特殊化学 ===
    "4626.T": "太陽ホールディングス",
    # === 医薬品 ===
    "4502.T": "武田薬品", "4503.T": "アステラス製薬", "4506.T": "住友ファーマ",
    "4507.T": "塩野義製薬", "4519.T": "中外製薬", "4523.T": "エーザイ",
    "4528.T": "小野薬品", "4568.T": "第一三共",
    # === 食品 ===
    "2002.T": "日清製粉G", "2269.T": "明治ホールディングス",
    "2501.T": "サッポロHD", "2502.T": "アサヒグループHD",
    "2503.T": "キリンHD", "2801.T": "キッコーマン", "2802.T": "味の素",
    "2871.T": "ニチレイ", "2914.T": "日本たばこ産業",
    # === 銀行・金融 ===
    "8301.T": "日本銀行", "8306.T": "三菱UFJ", "8308.T": "りそなHD",
    "8309.T": "三井住友トラスト", "8316.T": "三井住友FG",
    "8331.T": "千葉銀行", "8354.T": "ふくおかFG",
    "8411.T": "みずほFG", "8473.T": "SBI HD",
    "8591.T": "オリックス", "8604.T": "野村HD", "8630.T": "SOMPO HD",
    "8725.T": "MS&AD", "8750.T": "第一生命HD", "8766.T": "東京海上HD",
    # === 商社 ===
    "8001.T": "伊藤忠商事", "8002.T": "丸紅", "8015.T": "豊田通商",
    "8031.T": "三井物産", "8053.T": "住友商事", "8058.T": "三菱商事",
    # === 通信・IT ===
    "9432.T": "NTT", "9433.T": "KDDI", "9434.T": "ソフトバンク",
    "9984.T": "ソフトバンクG", "4684.T": "オービック",
    "4307.T": "野村総合研究所", "3697.T": "SHIFT",
    "4385.T": "メルカリ", "4689.T": "Zホールディングス",
    # === 不動産・建設 ===
    "1801.T": "大成建設", "1802.T": "大林組", "1803.T": "清水建設",
    "1808.T": "長谷工コーポレーション", "1812.T": "鹿島建設",
    "1878.T": "大東建託", "1925.T": "大和ハウス工業",
    "1928.T": "積水ハウス", "3003.T": "ヒューリック",
    "8801.T": "三井不動産", "8802.T": "三菱地所", "8830.T": "住友不動産",
    # === 運輸・物流 ===
    "9020.T": "JR東日本", "9021.T": "JR西日本", "9022.T": "JR東海",
    "9064.T": "ヤマトHD", "9101.T": "日本郵船", "9104.T": "商船三井",
    "9107.T": "川崎汽船", "9201.T": "JAL", "9202.T": "ANA HD",
    # === 電力・ガス ===
    "9501.T": "東京電力HD", "9502.T": "中部電力", "9503.T": "関西電力",
    "9531.T": "東京ガス", "9532.T": "大阪ガス",
    # === 小売・サービス ===
    "3099.T": "三越伊勢丹HD", "3382.T": "セブン&アイ",
    "8233.T": "高島屋", "8252.T": "丸井グループ",
    "8267.T": "イオン", "9602.T": "東宝", "9983.T": "ファーストリテイリング",
    "7532.T": "パン・パシフィック",
    # === 防災・セキュリティ ===
    "6745.T": "ホーチキ", "9735.T": "セコム", "2331.T": "綜合警備保障",
    # === その他製造 ===
    "3104.T": "富士紡ホールディングス", "7272.T": "ヤマハ発動機",
    "7731.T": "ニコン", "7733.T": "オリンパス", "7762.T": "シチズン時計",
    "7832.T": "バンダイナムコHD", "7974.T": "任天堂",
    # === 新興・中小型（成長株候補） ===
    "4071.T": "プラスアルファ・コンサルティング",
    "186A.T": "アストロスケールHD",
    "9612.T": "ラックランド",
    "3923.T": "ラクス", "4443.T": "Sansan",
    "4478.T": "フリー", "7342.T": "ウェルスナビ",
    "4480.T": "メドレー", "4485.T": "JTOWER",
    "2158.T": "FRONTEO", "3769.T": "GMOPG",
    "4434.T": "サーバーワークス", "6532.T": "ベイカレント",
    "4816.T": "東映アニメーション", "9749.T": "富士ソフト",
    "3962.T": "チェンジHD", "4011.T": "ヘッドウォータース",
    "6035.T": "IRジャパンHD",
    # === インフラ・ケーブル ===
    "1951.T": "エクシオグループ", "1959.T": "九電工",
    "1963.T": "日揮HD", "6366.T": "千代田化工建設",
    # === ガラス・セメント ===
    "5201.T": "AGC", "5232.T": "住友大阪セメント", "5233.T": "太平洋セメント",
    "5332.T": "TOTO",
    # === ゴム・繊維 ===
    "5101.T": "横浜ゴム", "5108.T": "ブリヂストン",
    # === 紙パルプ ===
    "3861.T": "王子HD", "3863.T": "日本製紙",
    # === 陸運 ===
    "9005.T": "東急", "9007.T": "小田急電鉄", "9008.T": "京王電鉄",
    "9009.T": "京成電鉄",
}

NIKKEI_TICKER = "^N225"


# ========================================
# STEP 1: 市場環境チェック
# ========================================

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

    # M-01 [MUST]: 指数 > 200日MA
    m01 = close > ma200

    # M-05 [NICE]: ディストリビューションデー
    recent = df.tail(26)
    dist_days = 0
    for i in range(1, len(recent)):
        pct = (recent.iloc[i]['Close'] - recent.iloc[i-1]['Close']) / recent.iloc[i-1]['Close'] * 100
        vol_up = recent.iloc[i]['Volume'] > recent.iloc[i-1]['Volume']
        if pct <= -0.2 and vol_up:
            dist_days += 1

    # M-06 [NICE]: フォロースルーデー判定
    ftd = detect_follow_through_day(df)

    # M-03/M-04 [MUST]: 新高値銘柄数トレンド（後で計算）
    # ここではプレースホルダー

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

    # 直近60日間で下落→反発パターンを探す
    lookback = min(60, n - 1)
    recent = df.tail(lookback)

    # 直近の安値（反発起点）を検出
    recent_closes = recent['Close'].values
    min_idx = np.argmin(recent_closes)

    # 安値後、4~7日目で条件を満たす日を探す
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


# ========================================
# STEP 2: 一括データ取得 + 52週新高値スキャン
# ========================================

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
        return [], data

    print(f"  データ取得完了: {data.index[0].strftime('%Y-%m-%d')} ~ {data.index[-1].strftime('%Y-%m-%d')}")

    new_high_stocks = []
    no_data_count = 0
    checked_count = 0
    # 新高値銘柄数の推移を記録（M-03/M-04用）
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

            # M-03/M-04用: 直近5日と前5日の新高値数を比較
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


# ========================================
# STEP 3: トレンドテンプレート (TT-01~08) [MUST]
# ========================================

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


# ========================================
# STEP 4: ボックスブレイク検出 (BP-01~05) [MUST]
# ========================================

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

    # 様々な期間でボックス圏を探索
    for period in [15, 20, 25, 30, 40, 50, 60]:
        if len(closes) < period + 5:
            continue

        # ブレイク前のN日間をボックス圏候補とする
        # 直近数日はブレイク後の可能性があるので除外
        for offset in range(1, 6):
            if len(closes) < period + offset:
                continue

            box_data = closes.iloc[-(period + offset):-offset]
            box_high = box_data.max()
            box_low = box_data.min()
            box_range_pct = (box_high - box_low) / box_low * 100

            if box_range_pct > 15:
                continue

            # BP-01: ボックス圏が存在する
            bp01 = True

            # BP-02: 保ち合い期間のスコア
            if period >= 30:
                bp02_score = 3
            elif period >= 20:
                bp02_score = 2
            elif period >= 15:
                bp02_score = 1
            else:
                bp02_score = 0

            # BP-03: 値幅の狭さスコア
            if box_range_pct <= 8:
                bp03_score = 3
            elif box_range_pct <= 12:
                bp03_score = 2
            elif box_range_pct <= 15:
                bp03_score = 1
            else:
                bp03_score = 0

            # 現在値がボックス上限を上抜けているか
            breakout = current > box_high

            if not breakout:
                continue

            # BP-05: ブレイク初期（ボックス上限からの乖離 ≤ 5%）
            chase_pct = (current - box_high) / box_high * 100
            bp05 = chase_pct <= 5.0

            # BP-04: ブレイク時の出来高
            # ブレイク日（ボックス上限を超えた最初の日）付近の出来高を確認
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


# ========================================
# STEP 5a: ビッグチェンジ分析 (F-05) [MUST / SEMI]
# ========================================

# BC-01~BC-07 カテゴリ別キーワード辞書
BIGCHANGE_KEYWORDS = {
    "BC-01": {
        "label": "新製品・新サービスの大ヒット",
        "keywords": [
            "新製品", "新サービス", "新機能", "ヒット", "大ヒット", "爆発的",
            "new product", "new service", "launch", "released", "hit product",
            "過去最高", "記録更新", "好調", "急成長", "シェア拡大",
        ],
    },
    "BC-02": {
        "label": "新業態・新事業への進出",
        "keywords": [
            "新事業", "新業態", "新セグメント", "事業転換", "参入",
            "new business", "new segment", "pivot", "expansion",
            "DX", "デジタル", "AI", "クラウド", "SaaS",
        ],
    },
    "BC-03": {
        "label": "新経営陣による大変革",
        "keywords": [
            "社長交代", "CEO", "経営刷新", "新社長", "新経営",
            "management change", "new CEO", "leadership",
            "構造改革", "中期経営計画", "ガバナンス改革",
        ],
    },
    "BC-04": {
        "label": "全国展開・海外進出",
        "keywords": [
            "海外進出", "海外展開", "グローバル", "全国展開", "北米",
            "overseas", "global expansion", "international",
            "アジア", "欧州", "米国", "中国", "現地法人",
        ],
    },
    "BC-05": {
        "label": "M&A（合併・買収）",
        "keywords": [
            "M&A", "買収", "合併", "TOB", "子会社化", "統合",
            "acquisition", "merger", "takeover",
            "資本提携", "業務提携", "戦略的提携",
        ],
    },
    "BC-06": {
        "label": "規制緩和・制度変更",
        "keywords": [
            "規制緩和", "制度変更", "法改正", "認可", "承認",
            "regulation", "deregulation", "approval",
            "補助金", "助成金", "国策", "政策",
        ],
    },
    "BC-07": {
        "label": "業界構造の変化",
        "keywords": [
            "寡占", "競合撤退", "市場創出", "業界再編",
            "industry", "market leader", "consolidation",
            "シェア首位", "独占", "技術革新", "破壊的",
        ],
    },
}


def analyze_bigchange_news(ticker, name):
    """F-05 [MUST/SEMI]: yfinanceのニュースAPIからビッグチェンジ候補を検出し、
    BC-01~BC-07カテゴリ別に分類・要旨を生成する。

    Returns:
        dict: {
            "news_found": int,         # 取得ニュース数
            "matches": [               # ビッグチェンジ候補ニュース
                {"title": str, "category": str, "category_label": str, "link": str, "date": str},
                ...
            ],
            "summary": str,            # 報告用の要旨テキスト
            "has_candidate": bool,     # ビッグチェンジ候補が1件以上あるか
            "categories_hit": list,    # ヒットしたカテゴリIDのリスト
        }
    """
    result = {
        "news_found": 0,
        "matches": [],
        "summary": "",
        "has_candidate": False,
        "categories_hit": [],
    }

    try:
        t = yf.Ticker(ticker)
        news_items = t.news
        if not news_items:
            result["summary"] = f"{name}に関するニュースが取得できませんでした。IR資料・ニュースサイトで直接確認してください。"
            return result

        result["news_found"] = len(news_items)

        # 各ニュースをBC-01~07のキーワードと照合
        categories_hit = set()
        matched_news = []

        for item in news_items:
            title = item.get("title", "")
            # yfinance v0.2+ のニュース構造に対応
            link = item.get("link", "") or item.get("url", "")
            pub_date = ""
            if "providerPublishTime" in item:
                try:
                    pub_date = datetime.fromtimestamp(item["providerPublishTime"]).strftime("%Y-%m-%d")
                except Exception:
                    pass
            elif "publishedDate" in item:
                pub_date = str(item["publishedDate"])[:10]

            # content内のtitleも確認（yfinance新API形式）
            content = item.get("content", {})
            if isinstance(content, dict):
                title = content.get("title", title) or title
                if not link and content.get("canonicalUrl"):
                    link = content["canonicalUrl"].get("url", "")
                if not pub_date and content.get("pubDate"):
                    pub_date = str(content["pubDate"])[:10]

            title_lower = title.lower()

            for bc_id, bc_info in BIGCHANGE_KEYWORDS.items():
                for kw in bc_info["keywords"]:
                    if kw.lower() in title_lower:
                        categories_hit.add(bc_id)
                        matched_news.append({
                            "title": title,
                            "category": bc_id,
                            "category_label": bc_info["label"],
                            "link": link,
                            "date": pub_date,
                        })
                        break  # 1ニュースにつき1カテゴリのみマッチ

        # 重複タイトルを除去
        seen_titles = set()
        unique_matches = []
        for m in matched_news:
            if m["title"] not in seen_titles:
                seen_titles.add(m["title"])
                unique_matches.append(m)

        result["matches"] = unique_matches
        result["categories_hit"] = sorted(categories_hit)
        result["has_candidate"] = len(unique_matches) > 0

        # --- 報告用の要旨テキストを生成 ---
        summary_parts = []
        summary_parts.append(f"**{name}（{ticker}）のビッグチェンジ調査報告**\n")
        summary_parts.append(f"yfinanceニュースAPI経由で{result['news_found']}件のニュースを取得、"
                             f"BC-01~07キーワードとの照合を実施。\n")

        if not unique_matches:
            summary_parts.append(f"ビッグチェンジに該当する明確なニュースは検出されませんでした。"
                                 f"ただし、ニュースAPIのカバレッジには限界があるため、"
                                 f"以下のキーワードでIR資料・ニュースサイトを別途確認することを推奨します。\n")
            summary_parts.append(f"- 検索推奨: 「{name} 新製品」「{name} M&A」「{name} 海外進出」「{name} 新事業」\n")
        else:
            # カテゴリ別にグルーピング
            by_category = {}
            for m in unique_matches:
                cat = m["category"]
                if cat not in by_category:
                    by_category[cat] = []
                by_category[cat].append(m)

            summary_parts.append(f"**{len(unique_matches)}件のビッグチェンジ候補を検出**（"
                                 f"カテゴリ: {', '.join(result['categories_hit'])}）\n")

            for cat_id in sorted(by_category.keys()):
                cat_label = BIGCHANGE_KEYWORDS[cat_id]["label"]
                items = by_category[cat_id]
                summary_parts.append(f"\n**{cat_id}: {cat_label}**\n")
                for m in items[:3]:  # カテゴリあたり最大3件
                    date_str = f"（{m['date']}）" if m['date'] else ""
                    summary_parts.append(f"- {m['title']}{date_str}\n")

            summary_parts.append(f"\n上記ニュースの内容を精査し、"
                                 f"株価上昇の背景にある構造的な変革（ビッグチェンジ）かどうかを最終判断してください。"
                                 f"一時的なニュースや既に織り込み済みの材料は除外する必要があります。\n")

        result["summary"] = "".join(summary_parts)

    except Exception as e:
        result["summary"] = f"{name}のニュース取得中にエラーが発生しました: {e}\nIR資料・ニュースサイトで直接確認してください。"

    return result


# ========================================
# STEP 5b: ファンダメンタルズ [MUST: F-01~F-05]
# ========================================

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
    """時系列データ（新しい順）から直近の前年同期比成長率を計算する。
    四半期データ: index[0] vs index[4]
    年次データ: index[0] vs index[1]
    戻り値: (growth_rate: float, period_label: str) or (None, None)
    """
    if series is None or len(series) < 2:
        return None, None

    # 日付間隔から四半期 or 年次を自動判定
    dates = sorted(series.index, reverse=True)  # 新しい順
    vals = [series[d] for d in dates]

    if len(dates) >= 2:
        gap_days = abs((dates[0] - dates[1]).days)
    else:
        gap_days = 365

    if gap_days < 200 and len(vals) >= 5:
        # 四半期データ: 4期前と比較（前年同期比）
        current, year_ago = vals[0], vals[4]
        if year_ago is not None and year_ago != 0 and not np.isnan(year_ago) and not np.isnan(current):
            return (current - year_ago) / abs(year_ago), "四半期YoY"
        return None, None
    else:
        # 年次データ: 直近年 vs 前年
        current, prev = vals[0], vals[1]
        if prev is not None and prev != 0 and not np.isnan(prev) and not np.isnan(current):
            return (current - prev) / abs(prev), "年次YoY"
        return None, None


def get_fundamentals(ticker):
    """ファンダメンタルズデータを取得し、MUST条件を評価する。
    データ取得の優先順位:
      1. info.earningsGrowth / revenueGrowth（Yahoo Finance算出値）
      2. quarterly_income_stmt から前年同期比を自力計算
      3. income_stmt（年次）から前年比を自力計算
    """
    try:
        t = yf.Ticker(ticker)
        info = t.info

        # --- 財務データ取得（複数ソース） ---
        quarterly_earnings = None
        quarterly_revenue = None
        annual_earnings = None
        annual_revenue = None

        # ソース1: 四半期損益計算書
        try:
            qf = t.quarterly_income_stmt
            if qf is not None and not qf.empty:
                quarterly_revenue, quarterly_earnings = _extract_income_series(qf)
        except Exception:
            pass

        # ソース2: 年次損益計算書
        try:
            af = t.income_stmt
            if af is not None and not af.empty:
                annual_revenue, annual_earnings = _extract_income_series(af)
        except Exception:
            pass

        # --- 成長率の決定（フォールバック戦略） ---
        # 優先度: info値 > 四半期YoY自力計算 > 年次YoY自力計算
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

        # F-03: 利益成長加速判定（四半期 → 年次フォールバック）
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

        # --- MUST条件評価 ---

        # F-01: 利益成長 +20%以上
        f01_pass = earnings_growth is not None and earnings_growth >= 0.20
        f01_score = 0
        if earnings_growth is not None:
            if earnings_growth >= 0.60:
                f01_score = 3
            elif earnings_growth >= 0.40:
                f01_score = 2
            elif earnings_growth >= 0.20:
                f01_score = 1

        # F-02: 売上成長 +10%以上
        f02_pass = revenue_growth is not None and revenue_growth >= 0.10
        f02_score = 0
        if revenue_growth is not None:
            if revenue_growth >= 0.50:
                f02_score = 3
            elif revenue_growth >= 0.25:
                f02_score = 2
            elif revenue_growth >= 0.10:
                f02_score = 1

        # F-03: 成長加速
        f03_score = 0
        if earnings_acceleration == "accelerating":
            f03_score = 2
        elif earnings_acceleration == "stable":
            f03_score = 1

        # F-04: 時価総額（中小型優先）
        f04_score = 0
        f04_label = "N/A"
        if market_cap is not None:
            mc_oku = market_cap / 1e8  # 億円換算
            if mc_oku < 300:
                f04_score = 3
                f04_label = "小型"
            elif mc_oku < 1000:
                f04_score = 2
                f04_label = "中型"
            else:
                f04_score = 1
                f04_label = "大型"

        # F-05: ビッグチェンジ（yfinanceニュース + キーワード分析）
        name = UNIVERSE.get(ticker, "")
        bigchange = analyze_bigchange_news(ticker, name)

        # 黒字判定
        profitable = pe is not None and pe > 0

        must_score = f01_score + f02_score + f03_score + f04_score
        must_pass_count = sum([f01_pass, f02_pass, True])  # F-03は推奨、F-04は加点
        must_total = 2  # F-01, F-02が必須

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
            # MUST判定
            "f01_pass": f01_pass, "f01_score": f01_score,
            "f02_pass": f02_pass, "f02_score": f02_score,
            "f03_score": f03_score,
            "f04_score": f04_score, "f04_label": f04_label,
            "bigchange": bigchange,
            "must_fund_score": must_score,
            "must_fund_pass": f01_pass and f02_pass,
        }
    except Exception as e:
        return {"error": str(e), "must_fund_pass": False, "must_fund_score": 0}


def evaluate_earnings_acceleration(quarterly_data):
    """四半期データから成長加速・減速を判定する。
    前年同期比の成長率が四半期ごとに加速しているかを見る。"""
    if quarterly_data is None or len(quarterly_data) < 5:
        return "unknown"

    # quarterly_dataは新しい順。前年同期比を計算するには4四半期前のデータが必要
    values = quarterly_data.values
    if len(values) < 5:
        return "unknown"

    # 成長率を計算（直近 vs 4期前、1期前 vs 5期前）
    growth_rates = []
    for i in range(min(3, len(values) - 4)):
        current = values[i]
        year_ago = values[i + 4] if i + 4 < len(values) else None
        if year_ago is not None and year_ago != 0:
            gr = (current - year_ago) / abs(year_ago)
            growth_rates.append(gr)

    if len(growth_rates) < 2:
        return "unknown"

    # 加速判定: 直近の成長率 > 1期前の成長率
    if growth_rates[0] > growth_rates[1]:
        return "accelerating"
    elif abs(growth_rates[0] - growth_rates[1]) < 0.05:
        return "stable"
    else:
        return "decelerating"


def evaluate_earnings_acceleration_annual(annual_data):
    """年次データから成長加速・減速を判定する（四半期データ不足時のフォールバック）。
    直近3年分のYoY成長率を比較し、加速しているかを判定する。"""
    if annual_data is None or len(annual_data) < 3:
        return "unknown"

    dates = sorted(annual_data.index, reverse=True)
    vals = [annual_data[d] for d in dates]

    # 直近3年分のYoY成長率
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


# ========================================
# STEP 6: NICE条件
# ========================================

def calculate_relative_strength(ticker_closes, nikkei_closes):
    """RS-01/RS-02 [NICE]: レラティブストレングス計算。
    銘柄のリターンを日経平均のリターンと比較。"""
    if ticker_closes is None or nikkei_closes is None:
        return None
    if len(ticker_closes) < 252 or len(nikkei_closes) < 252:
        return None

    # 12ヶ月リターン
    stock_ret_12m = (ticker_closes.iloc[-1] / ticker_closes.iloc[-252] - 1) * 100
    nikkei_ret_12m = (nikkei_closes.iloc[-1] / nikkei_closes.iloc[-252] - 1) * 100

    # 6ヶ月リターン
    stock_ret_6m = (ticker_closes.iloc[-1] / ticker_closes.iloc[-126] - 1) * 100
    nikkei_ret_6m = (nikkei_closes.iloc[-1] / nikkei_closes.iloc[-126] - 1) * 100

    # 3ヶ月リターン
    stock_ret_3m = (ticker_closes.iloc[-1] / ticker_closes.iloc[-63] - 1) * 100
    nikkei_ret_3m = (nikkei_closes.iloc[-1] / nikkei_closes.iloc[-63] - 1) * 100

    # RS = 加重平均（直近に重み）: 3M×40% + 6M×30% + 12M×30%
    rs_raw = (stock_ret_3m - nikkei_ret_3m) * 0.4 + \
             (stock_ret_6m - nikkei_ret_6m) * 0.3 + \
             (stock_ret_12m - nikkei_ret_12m) * 0.3

    # RS推移（RS-02）: 3ヶ月前のRS vs 現在のRS
    rs_3m_ago = None
    if len(ticker_closes) >= 315 and len(nikkei_closes) >= 315:  # 252 + 63
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

    # ローカル高値・安値を検出（前後5日で比較、scipy不要の実装）
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

    # 直近120日以内のピーク・トラフを使用
    cutoff = max(0, len(closes) - 120)
    recent_max = [(i, close_values[i]) for i in local_max_idx if i >= cutoff]
    recent_min = [(i, close_values[i]) for i in local_min_idx if i >= cutoff]

    if len(recent_max) < 2 or len(recent_min) < 1:
        return None

    # 収縮幅を計算
    contractions = []
    for i in range(len(recent_max) - 1):
        peak_val = recent_max[i][1]
        # このピークと次のピークの間のトラフを探す
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

    # VCP-01: 収縮回数
    vcp01 = len(contractions) >= 2

    # VCP-02: 各収縮が前回より小さい
    vcp02 = True
    for i in range(1, len(contractions)):
        if contractions[i]["drawdown_pct"] > contractions[i-1]["drawdown_pct"] * 0.85:
            vcp02 = False
            break

    # VCP-03: 出来高が収縮とともに減少
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

    # VCP-04: ピボットポイント（最後の収縮の高値）
    pivot = recent_max[-1][1] if recent_max else None

    # VCP判定スコア
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


def evaluate_canslim_nice(fund_data):
    """CAN-SLIM補完条件 [NICE] のスコアリング。"""
    score = 0
    details = {}

    # CS-04: ROE 17%以上
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

    # CS-06: 発行済株式数（少ないほど良い）
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


# ========================================
# STEP 7: 総合スコアリング
# ========================================

def calculate_total_score(stock):
    """MUST達成率とNICEボーナスを統合した総合スコアを計算する。"""
    must_checks = []
    must_score = 0
    nice_score = 0

    # --- MUST条件 ---

    # TT (全8条件パス)
    tt = stock.get("tt")
    if tt and tt["all_pass"]:
        must_checks.append(("TT(ステージ2)", True))
        must_score += 10
    else:
        must_checks.append(("TT(ステージ2)", False))

    # ボックスブレイク
    bp = stock.get("box_breakout")
    if bp and bp.get("detected"):
        must_checks.append(("BP(ボックスブレイク)", True))
        must_score += bp.get("score", 0)
        if bp.get("bp04_volume_breakout"):
            must_checks.append(("BP-04(出来高)", True))
        else:
            must_checks.append(("BP-04(出来高)", False))
        if bp.get("bp05_early_entry"):
            must_checks.append(("BP-05(初期エントリー)", True))
        else:
            must_checks.append(("BP-05(初期エントリー)", False))
    else:
        must_checks.append(("BP(ボックスブレイク)", False))

    # ファンダメンタルズ
    fund = stock.get("fund", {})
    if fund.get("f01_pass"):
        must_checks.append(("F-01(利益+20%)", True))
        must_score += fund.get("f01_score", 0)
    else:
        must_checks.append(("F-01(利益+20%)", False))

    if fund.get("f02_pass"):
        must_checks.append(("F-02(売上+10%)", True))
        must_score += fund.get("f02_score", 0)
    else:
        must_checks.append(("F-02(売上+10%)", False))

    must_score += fund.get("f03_score", 0)  # 加速ボーナス
    must_score += fund.get("f04_score", 0)  # 時価総額ボーナス

    # F-05(ビッグチェンジ)はSEMI判定なのでスコアに含めるがpass/failは保留
    must_checks.append(("F-05(ビッグチェンジ)", "SEMI"))

    # MUST達成率
    must_pass = sum(1 for _, v in must_checks if v is True)
    must_fail = sum(1 for _, v in must_checks if v is False)
    must_semi = sum(1 for _, v in must_checks if v == "SEMI")
    must_total = must_pass + must_fail
    must_rate = (must_pass / must_total * 100) if must_total > 0 else 0

    # --- NICE条件 ---

    # RS
    rs = stock.get("rs")
    if rs:
        rs_raw = rs.get("rs_raw", 0)
        if rs_raw >= 20:
            nice_score += 3
        elif rs_raw >= 10:
            nice_score += 2
        elif rs_raw >= 0:
            nice_score += 1
        if rs.get("rs_improving"):
            nice_score += 1

    # VCP
    vcp = stock.get("vcp")
    if vcp and vcp.get("detected"):
        nice_score += vcp.get("score", 0)

    # CAN-SLIM
    canslim = stock.get("canslim_nice", {})
    nice_score += canslim.get("score", 0)

    # --- 推奨判定 ---
    if must_rate >= 100:
        recommendation = "推奨"
    elif must_rate >= 80:
        recommendation = "条件付き監視"
    else:
        recommendation = "対象外"

    total_score = must_score + nice_score

    return {
        "must_checks": must_checks,
        "must_pass": must_pass,
        "must_fail": must_fail,
        "must_semi": must_semi,
        "must_rate": must_rate,
        "must_score": must_score,
        "nice_score": nice_score,
        "total_score": total_score,
        "recommendation": recommendation,
    }


# ========================================
# 出来高チェック（既存拡張）
# ========================================

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


# ========================================
# Markdownレポート生成
# ========================================

def generate_markdown_report(market, new_highs, qualified, all_scored, nh_trend, today):
    """結果をMarkdownレポートファイルに出力する。"""
    lines = []
    L = lines.append

    L(f"# 新高値ブレイク投資法 スクリーニングレポート v2")
    L(f"**実行日: {today}**")
    L(f"**優先度体系: MUST（DUKE。メソッド）+ NICE（ミネルヴィニ/オニール）**")
    L("")
    L("---")
    L("")

    # 市場環境
    L("## 1. 市場環境判定")
    L("")
    if market:
        L("| 条件ID | 判定項目 | 結果 | 優先度 | 判定 |")
        L("|--------|----------|------|--------|------|")
        m01_str = f"日経平均 {market['close']:,.0f}円 {'>' if market['m01_buy'] else '<'} 200日MA {market['ma200']:,.0f}円（乖離{market['deviation_pct']:+.1f}%）"
        L(f"| M-01 | 指数 vs 200日MA | {m01_str} | **MUST** | {'PASS' if market['m01_buy'] else 'FAIL'} |")
        nh_str = f"直近5日={nh_trend['new_high_recent']}, 前5日={nh_trend['new_high_prev']}"
        L(f"| M-03/04 | 新高値銘柄数推移 | {nh_str} | **MUST** | {'PASS(増加)' if nh_trend['m03_increasing'] else 'FAIL(減少)'} |")
        L(f"| M-05 | ディストリビューションデー | 直近25日中 **{market['distribution_days']}日** | NICE | {'警告' if market['m05_warning'] else 'OK'} |")
        ftd = market.get("ftd", {})
        ftd_str = f"検出: {ftd['date']} ({ftd['gain_pct']:+.1f}%)" if ftd.get("detected") else "該当なし"
        L(f"| M-06 | フォロースルーデー | {ftd_str} | NICE | {'検出' if ftd.get('detected') else '-'} |")
        L("")
        if not market['m01_buy']:
            L("> **MUST FAIL: 日経平均が200日MAを下回っています。新規買いは停止を推奨。**")
            L("")
        if market['m05_warning']:
            L(f"> **NICE警告: ディストリビューションデー{market['distribution_days']}日（閾値4日）。ポジション縮小を検討。**")
            L("")

    # 新高値スキャン
    L("---")
    L("")
    L(f"## 2. 52週新高値スキャン結果（{len(new_highs)}銘柄）")
    L("")
    if new_highs:
        L("| コード | 銘柄名 | 終値 | 52週高値比 |")
        L("|--------|--------|-----:|----------:|")
        for s in sorted(new_highs, key=lambda x: -x['diff_pct']):
            code = s['ticker'].replace('.T', '')
            L(f"| {code} | {s['name']} | {s['close']:,.0f} | {s['diff_pct']:+.1f}% |")
    else:
        L("52週新高値更新銘柄なし。")
    L("")

    # 総合スコアリング結果
    L("---")
    L("")
    L("## 3. 総合スコアリング結果")
    L("")

    # 推奨 / 条件付き監視 / 対象外 に分類
    recommended = [s for s in all_scored if s['scoring']['recommendation'] == '推奨']
    watchlist = [s for s in all_scored if s['scoring']['recommendation'] == '条件付き監視']
    excluded = [s for s in all_scored if s['scoring']['recommendation'] == '対象外']

    for label, stocks, emoji in [
        ("推奨（MUST全クリア）", recommended, ""),
        ("条件付き監視（MUST 80%以上）", watchlist, ""),
        ("参考（MUST未達）", excluded, ""),
    ]:
        L(f"### {label}（{len(stocks)}銘柄）")
        L("")
        if not stocks:
            L("該当なし。")
            L("")
            continue

        for s in sorted(stocks, key=lambda x: -x['scoring']['total_score']):
            code = s['ticker'].replace('.T', '')
            sc = s['scoring']
            tt = s.get('tt', {})
            bp = s.get('box_breakout', {})
            fund = s.get('fund', {})
            rs = s.get('rs')
            vcp = s.get('vcp')
            vol = s.get('vol', {})

            L(f"#### 【{code}】{s['name']}")
            L("")
            L(f"**総合スコア: {sc['total_score']}pt（MUST: {sc['must_score']}pt + NICE: {sc['nice_score']}pt）**")
            L(f"**判定: {sc['recommendation']}** | MUST達成率: {sc['must_rate']:.0f}%（{sc['must_pass']}/{sc['must_pass']+sc['must_fail']}）")
            L("")

            # 株価情報
            L("| 項目 | 値 |")
            L("|------|-----|")
            L(f"| 終値 | {s['close']:,.0f}円 |")
            L(f"| 52週高値比 | {s['diff_pct']:+.1f}% |")
            L(f"| 52週安値比 | +{s['above_low_pct']:.0f}% |")
            if tt:
                L(f"| MA | 50日={tt['ma50']:,.0f} / 150日={tt['ma150']:,.0f} / 200日={tt['ma200']:,.0f} |")
            L("")

            # MUST条件詳細
            L("**MUST条件（DUKE。メソッド）:**")
            L("")
            L("| 条件 | 結果 | 詳細 |")
            L("|------|------|------|")

            # TT
            tt_str = f"{tt['pass_count']}/{tt['total']}" if tt else "N/A"
            tt_pass = "PASS" if tt and tt['all_pass'] else "FAIL"
            tt_fails = ""
            if tt and not tt['all_pass']:
                fails = [k for k, v in tt['results'].items() if not v]
                tt_fails = f" 未達: {', '.join(fails)}"
            L(f"| TT（ステージ2） | {tt_pass} | {tt_str}{tt_fails} |")

            # ボックスブレイク
            if bp and bp.get('detected'):
                bp_str = f"期間{bp['box_period']}日, 値幅{bp['box_range_pct']:.1f}%, 上限{bp['box_high']:,.0f}円"
                bp04_str = "出来高OK" if bp.get('bp04_volume_breakout') else "出来高不足"
                bp05_str = f"乖離{bp['chase_pct']:.1f}%" if bp.get('bp05_early_entry') else f"乖離{bp['chase_pct']:.1f}%(追いかけ注意)"
                L(f"| BP（ボックスブレイク） | PASS | {bp_str} |")
                L(f"| BP-04（出来高） | {'PASS' if bp.get('bp04_volume_breakout') else 'FAIL'} | {bp04_str}（倍率{bp.get('vol_ratio', 0):.2f}x） |")
                L(f"| BP-05（初期エントリー） | {'PASS' if bp.get('bp05_early_entry') else 'WARN'} | {bp05_str} |")
            else:
                L(f"| BP（ボックスブレイク） | FAIL | ボックス圏未検出 |")

            # ファンダメンタルズ
            eg = fund.get('earnings_growth')
            rg = fund.get('revenue_growth')
            eg_src = fund.get('eg_source', '')
            rg_src = fund.get('rg_source', '')
            eg_str = f"{eg*100:+.1f}%（{eg_src}）" if eg is not None and eg_src else ("N/A" if eg is None else f"{eg*100:+.1f}%")
            rg_str = f"{rg*100:+.1f}%（{rg_src}）" if rg is not None and rg_src else ("N/A" if rg is None else f"{rg*100:+.1f}%")
            L(f"| F-01（利益成長+20%） | {'PASS' if fund.get('f01_pass') else 'FAIL'} | {eg_str} |")
            L(f"| F-02（売上成長+10%） | {'PASS' if fund.get('f02_pass') else 'FAIL'} | {rg_str} |")
            accel = fund.get('earnings_acceleration', 'unknown')
            accel_label = {"accelerating": "加速", "stable": "横ばい", "decelerating": "減速", "unknown": "不明"}
            L(f"| F-03（成長加速） | {accel_label.get(accel, '不明')} | +{fund.get('f03_score', 0)}pt |")

            mc = fund.get('market_cap')
            mc_str = f"{mc/1e8:,.0f}億円 ({fund.get('f04_label', 'N/A')})" if mc else "N/A"
            L(f"| F-04（時価総額） | {fund.get('f04_label', 'N/A')} | {mc_str} |")
            bigchange = fund.get('bigchange', {})
            bc_hits = bigchange.get('categories_hit', [])
            if bigchange.get('has_candidate'):
                bc_count = len(bigchange.get('matches', []))
                L(f"| F-05（ビッグチェンジ） | 候補あり | {bc_count}件検出（{', '.join(bc_hits)}）→ 下記報告参照 |")
            else:
                L(f"| F-05（ビッグチェンジ） | 要確認 | ニュースからの自動検出なし → 下記報告参照 |")
            L("")

            # NICE条件詳細
            L("**NICE条件（ミネルヴィニ/オニール）:**")
            L("")
            L("| 条件 | 結果 | 詳細 |")
            L("|------|------|------|")

            # RS
            if rs:
                rs_raw = rs.get('rs_raw', 0)
                L(f"| RS（レラティブストレングス） | RS={rs_raw:+.1f} | 12M={rs['stock_ret_12m']:+.1f}%, 6M={rs['stock_ret_6m']:+.1f}%, 3M={rs['stock_ret_3m']:+.1f}% |")
                rs_trend = "改善中" if rs.get('rs_improving') else ("悪化" if rs.get('rs_improving') is False else "N/A")
                L(f"| RS推移 | {rs_trend} | - |")
            else:
                L(f"| RS | N/A | データ不足 |")

            # VCP
            if vcp and vcp.get('detected'):
                contractions_str = " → ".join([f"{c['drawdown_pct']:.1f}%" for c in vcp['contractions']])
                L(f"| VCP | 検出 (+{vcp['score']}pt) | 収縮{vcp['contraction_count']}回: {contractions_str} |")
            else:
                L(f"| VCP | 未検出 | - |")

            # CAN-SLIM補完
            canslim = s.get('canslim_nice', {})
            for key, val in canslim.get('details', {}).items():
                L(f"| {key} | - | {val} |")

            # ROE / PER
            roe = fund.get('roe')
            pe = fund.get('pe_trailing')
            if roe is not None:
                L(f"| ROE | {roe*100:.1f}% | {'17%以上' if roe >= 0.17 else '17%未満'} |")
            if pe is not None:
                L(f"| PER | {pe:.1f}x | - |")

            L("")

            # ビッグチェンジ調査報告
            bigchange = fund.get('bigchange', {})
            bc_summary = bigchange.get('summary', '')
            if bc_summary:
                L("**F-05 ビッグチェンジ調査報告:**")
                L("")
                for line in bc_summary.strip().split('\n'):
                    L(line)
                L("")

            L("---")
            L("")

    # TT未達銘柄（参考）
    tt_near = [s for s in new_highs if s not in [x for x in all_scored] and s.get('tt') and s['tt']['pass_count'] >= 7]
    if tt_near:
        L(f"## 参考: 新高値更新中だがTT 7/8以上（{len(tt_near)}銘柄）")
        L("")
        L("| コード | 銘柄名 | TT | 終値 |")
        L("|--------|--------|----|-----:|")
        for s in tt_near:
            code = s['ticker'].replace('.T', '')
            L(f"| {code} | {s['name']} | {s['tt']['pass_count']}/{s['tt']['total']} | {s['close']:,.0f} |")
        L("")

    # 注意事項
    L("---")
    L("")
    L("## 注意事項")
    L("")
    L("- データソース: yfinance（Yahoo Finance）。株価は取得時点の最新値。")
    L("- 売上・利益成長率はyfinanceのデータ。直近四半期の数値とは異なる場合あり。")
    L("- F-01/F-02はyfinance info値→四半期損益計算書→年次損益計算書の優先順でフォールバック取得。括弧内にデータソースを表示。")
    L("- F-05（ビッグチェンジ）はyfinanceニュースからBC-01~07キーワードで自動検出。最終判断は人間が行う。")
    L("- ボックスブレイク(BP)の検出は自動アルゴリズムによる近似。目視確認を推奨。")
    L("- VCPパターンの検出も自動アルゴリズムによる近似。")
    L("- 本レポートは投資助言ではありません。投資判断は自己責任で行ってください。")
    if market and market.get('m05_warning'):
        L(f"- **ディストリビューションデー{market['distribution_days']}日: ポジション縮小・慎重姿勢を推奨。**")

    return "\n".join(lines)


# ========================================
# メイン処理
# ========================================

def main():
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"{'#' * 70}")
    print(f"# 新高値ブレイク投資法 スクリーニング v2（優先度スコアリング版）")
    print(f"# MUST = DUKE。メソッド / NICE = ミネルヴィニ・オニール")
    print(f"# 実行日: {today}")
    print(f"{'#' * 70}")

    # --- STEP 1: 市場環境 ---
    market = check_market()

    # --- STEP 2: 新高値スキャン ---
    new_highs, data, nh_trend = scan_new_highs()

    if not new_highs:
        print("\n  52週新高値更新銘柄なし。スクリーニング終了。")
        return

    # 日経平均データ取得（RS計算用）
    nikkei_closes = None
    try:
        if isinstance(data.columns, pd.MultiIndex):
            # 日経平均を別途取得
            nk = yf.Ticker(NIKKEI_TICKER)
            nk_df = nk.history(period="2y", auto_adjust=True)
            if not nk_df.empty:
                nikkei_closes = nk_df['Close'].dropna()
        else:
            nikkei_closes = data['Close'].dropna()
    except Exception:
        pass

    # --- STEP 3 & 4 & 5 & 6: 個別銘柄評価 ---
    print(f"\n{'=' * 70}")
    print(f"STEP 3-6: 個別銘柄評価（新高値{len(new_highs)}銘柄）")
    print(f"{'=' * 70}")

    all_scored = []

    for stock in new_highs:
        ticker = stock['ticker']
        code = ticker.replace('.T', '')

        try:
            if isinstance(data.columns, pd.MultiIndex):
                closes = data['Close'][ticker].dropna()
                volumes = data['Volume'][ticker].dropna()
            else:
                closes = data['Close'].dropna()
                volumes = data['Volume'].dropna()

            closes = closes.replace([np.inf, -np.inf], np.nan).dropna()
            volumes = volumes.replace([np.inf, -np.inf], np.nan).dropna()
        except Exception:
            print(f"    {code}: データ取得エラー")
            continue

        # STEP 3: トレンドテンプレート [MUST]
        tt = check_trend_template(closes)
        stock['tt'] = tt

        if tt:
            fails = [k for k, v in tt['results'].items() if v is None or not bool(v)]
            status = 'PASS' if tt['all_pass'] else f'FAIL({",".join(fails)})'
            print(f"    {code} {stock['name']}: TT {tt['pass_count']}/{tt['total']} {status}", end="")
        else:
            print(f"    {code} {stock['name']}: TT データ不足", end="")
            continue

        # STEP 4: ボックスブレイク [MUST]
        bp = detect_box_breakout(closes, volumes)
        stock['box_breakout'] = bp
        if bp and bp.get('detected'):
            print(f" | BP:検出(期間{bp['box_period']}日,幅{bp['box_range_pct']:.1f}%)", end="")
        else:
            print(f" | BP:未検出", end="")

        # 出来高
        vol = check_volume(volumes)
        stock['vol'] = vol

        # STEP 5: ファンダメンタルズ + ビッグチェンジ [MUST]
        fund = get_fundamentals(ticker)
        stock['fund'] = fund
        f01 = "OK" if fund.get('f01_pass') else "NG"
        f02 = "OK" if fund.get('f02_pass') else "NG"
        eg_src = fund.get('eg_source', '-')
        rg_src = fund.get('rg_source', '-')
        eg_val = fund.get('earnings_growth')
        rg_val = fund.get('revenue_growth')
        eg_detail = f"{eg_val*100:+.0f}%[{eg_src}]" if eg_val is not None else "N/A"
        rg_detail = f"{rg_val*100:+.0f}%[{rg_src}]" if rg_val is not None else "N/A"
        bc = fund.get('bigchange', {})
        bc_str = f"BC:{len(bc.get('matches', []))}件" if bc.get('has_candidate') else "BC:未検出"
        print(f" | F-01:{f01}({eg_detail}) F-02:{f02}({rg_detail}) {bc_str}", end="")

        # STEP 6: NICE条件
        # RS
        rs = calculate_relative_strength(closes, nikkei_closes)
        stock['rs'] = rs

        # VCP
        vcp = detect_vcp(closes, volumes)
        stock['vcp'] = vcp

        # CAN-SLIM
        canslim = evaluate_canslim_nice(fund)
        stock['canslim_nice'] = canslim

        # 総合スコア
        scoring = calculate_total_score(stock)
        stock['scoring'] = scoring
        print(f" | Score:{scoring['total_score']}pt ({scoring['recommendation']})")

        all_scored.append(stock)

    # --- 最終サマリー ---
    print(f"\n{'=' * 70}")
    print("最終サマリー（総合スコア順）")
    print(f"{'=' * 70}")

    all_scored.sort(key=lambda x: -x['scoring']['total_score'])

    recommended = [s for s in all_scored if s['scoring']['recommendation'] == '推奨']
    watchlist = [s for s in all_scored if s['scoring']['recommendation'] == '条件付き監視']

    def print_group(label, stocks):
        if not stocks:
            print(f"\n  {label}: なし")
            return
        print(f"\n  {label}:")
        print(f"  {'コード':<8} {'銘柄名':<18} {'総合':>5} {'MUST':>5} {'NICE':>5} {'達成率':>6} {'判定':<8}")
        print(f"  {'─'*60}")
        for s in stocks:
            code = s['ticker'].replace('.T', '')
            sc = s['scoring']
            print(f"  {code:<8} {s['name']:<18} {sc['total_score']:>5} {sc['must_score']:>5} {sc['nice_score']:>5} {sc['must_rate']:>5.0f}% {sc['recommendation']:<8}")

    print_group("推奨（MUST全クリア）", recommended)
    print_group("条件付き監視（MUST 80%以上）", watchlist)

    # TT未達の新高値銘柄
    tt_near = [s for s in new_highs if s.get('tt') and not s['tt']['all_pass'] and s['tt']['pass_count'] >= 6]
    if tt_near:
        print(f"\n  参考: TT 6/8以上の新高値銘柄（{len(tt_near)}銘柄）:")
        for s in tt_near:
            code = s['ticker'].replace('.T', '')
            print(f"    {code} {s['name']}: TT {s['tt']['pass_count']}/{s['tt']['total']}")

    # --- Markdownレポート出力 ---
    report = generate_markdown_report(market, new_highs, recommended + watchlist, all_scored, nh_trend, today)
    report_filename = f"screening_report_{today.replace('-', '')}.md"
    with open(report_filename, "w", encoding="utf-8") as f:
        f.write(report)
    print(f"\n  Markdownレポート出力: {report_filename}")

    print(f"\n{'=' * 70}")
    print("注意事項")
    print(f"{'=' * 70}")
    print("  - MUST条件（DUKE。メソッド）が全てPASSの銘柄のみ「推奨」と判定。")
    print("  - NICE条件（ミネルヴィニ/オニール）はボーナス加点。スコアが高いほど多角的に有望。")
    print("  - F-05（ビッグチェンジ）はyfinanceニュースから候補を自動検出。最終判断は人間が行う。")
    print("  - ボックスブレイク・VCPの検出はアルゴリズムによる近似。チャートの目視確認を推奨。")
    if market and market.get('m05_warning'):
        print(f"  - [!] ディストリビューションデー{market['distribution_days']}日: 慎重姿勢を推奨。")


if __name__ == "__main__":
    main()
