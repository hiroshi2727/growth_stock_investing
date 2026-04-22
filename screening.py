#!/usr/bin/env python3
"""
新高値ブレイク投資法 スクリーニングスクリプト v2（優先度スコアリング版）

DUKE。メソッド = MUST（必須条件）
ミネルヴィニ / オニール = NICE（ボーナス加点）

判定フロー:
  STEP 1: 市場環境チェック (M-01~M-06)           → market.py
  STEP 2: 52週新高値スキャン                       → scanner.py
  STEP 3: トレンドテンプレート (TT-01~08) [MUST]    → trend.py
  STEP 4: ボックスブレイク検出 (BP-01~05) [MUST]    → trend.py
  STEP 5: ファンダメンタルズ (F-01~F-05) [MUST]     → fundamentals.py / bigchange.py
  STEP 6: NICE条件（RS, VCP, CAN-SLIM補完）         → trend.py / fundamentals.py
  STEP 7: 総合スコアリング・Markdownレポート出力     → scoring.py / report.py
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import warnings
from datetime import datetime

import numpy as np
import pandas as pd
import yfinance as yf

warnings.filterwarnings('ignore')

from market import NIKKEI_TICKER, check_market
from scanner import scan_new_highs
from trend import (
    check_trend_template,
    check_volume,
    detect_box_breakout,
)
from fundamentals import get_fundamentals
from scoring import calculate_total_score
from report import generate_markdown_report


def main():
    today = datetime.now().strftime("%Y-%m-%d")
    print(f"{'#' * 70}")
    print(f"# 新高値ブレイク投資法 スクリーニング v2（優先度スコアリング版）")
    print(f"# MUST = DUKE。メソッド / NICE = ミネルヴィニ・オニール")
    print(f"# 実行日: {today}")
    print(f"{'#' * 70}")

    market = check_market()

    new_highs, data, nh_trend = scan_new_highs()

    if not new_highs:
        print("\n  52週新高値更新銘柄なし。スクリーニング終了。")
        return

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

        tt = check_trend_template(closes)
        stock['tt'] = tt

        if tt:
            fails = [k for k, v in tt['results'].items() if v is None or not bool(v)]
            status = 'PASS' if tt['all_pass'] else f'FAIL({",".join(fails)})'
            print(f"    {code} {stock['name']}: TT {tt['pass_count']}/{tt['total']} {status}", end="")
        else:
            print(f"    {code} {stock['name']}: TT データ不足", end="")
            continue

        bp = detect_box_breakout(closes, volumes)
        stock['box_breakout'] = bp
        if bp and bp.get('detected'):
            print(f" | BP:検出(期間{bp['box_period']}日,幅{bp['box_range_pct']:.1f}%)", end="")
        else:
            print(f" | BP:未検出", end="")

        vol = check_volume(volumes)
        stock['vol'] = vol

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
        print(f" | F-01:{f01}({eg_detail}) F-02:{f02}({rg_detail})", end="")

        scoring = calculate_total_score(stock)
        stock['scoring'] = scoring
        print(f" | Score:{scoring['total_score']}pt ({scoring['recommendation']})")

        all_scored.append(stock)

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
        print(f"  {'コード':<8} {'銘柄名':<18} {'スコア':>6} {'達成率':>6} {'判定':<8}")
        print(f"  {'─'*55}")
        for s in stocks:
            code = s['ticker'].replace('.T', '')
            sc = s['scoring']
            print(f"  {code:<8} {s['name']:<18} {sc['must_score']:>6} {sc['must_rate']:>5.0f}% {sc['recommendation']:<8}")

    print_group("推奨（MUST全クリア）", recommended)
    print_group("条件付き監視（MUST 80%以上）", watchlist)

    tt_near = [s for s in new_highs if s.get('tt') and not s['tt']['all_pass'] and s['tt']['pass_count'] >= 6]
    if tt_near:
        print(f"\n  参考: TT 6/8以上の新高値銘柄（{len(tt_near)}銘柄）:")
        for s in tt_near:
            code = s['ticker'].replace('.T', '')
            print(f"    {code} {s['name']}: TT {s['tt']['pass_count']}/{s['tt']['total']}")

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
