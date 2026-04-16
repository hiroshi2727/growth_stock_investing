#!/usr/bin/env python3
"""
業種別パフォーマンス分析スクリプト

JPX上場銘柄一覧を使い、17業種ごとに以下を集計:
  - 直近3年の単年株価上昇率（Y1:23→24, Y2:24→25, Y3:25→26）
  - 3年累積上昇率
  - 利益率（純利益率）中央値
  - 33業種→17業種マッピング表示
"""

import sys
import io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import warnings
warnings.filterwarnings('ignore')

from pathlib import Path
from datetime import datetime, date
import numpy as np
import pandas as pd
import yfinance as yf
import xlrd

DATA_DIR = Path(__file__).resolve().parent / "data"
JPX_FILE = DATA_DIR / "data_j.xls"

PRIME   = "プライム（内国株式）"
STANDARD = "スタンダード（内国株式）"
GROWTH  = "グロース（内国株式）"

# 直近3年の区切り日
# Y1: 2023-04-01 → 2024-03-31
# Y2: 2024-04-01 → 2025-03-31
# Y3: 2025-04-01 → 2026-04-14 (直近)
PERIODS = [
    ("Y1(23→24)", "2023-04-03", "2024-03-29"),
    ("Y2(24→25)", "2024-04-01", "2025-03-31"),
    ("Y3(25→26)", "2025-04-01", "2026-04-14"),
]

BATCH_SIZE = 200
# セクターごとのサンプル上限（速度調整用。Noneで全銘柄）
MAX_PER_SECTOR = None  # プライムのみで実行するので全銘柄


def parse_jpx():
    book = xlrd.open_workbook(str(JPX_FILE))
    sheet = book.sheet_by_index(0)
    rows = [sheet.row_values(i) for i in range(sheet.nrows)]
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df = df.rename(columns={
        "コード": "code",
        "銘柄名": "name",
        "市場・商品区分": "segment",
        "33業種コード": "sec33_code",
        "33業種区分": "sec33_name",
        "17業種コード": "sec17_code",
        "17業種区分": "sec17_name",
        "規模区分": "size",
    })
    df = df[df["segment"].isin([PRIME, STANDARD, GROWTH])].copy()
    df["code"] = df["code"].apply(
        lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x).strip()
    )
    df["ticker"] = df["code"] + ".T"
    df["sec17_code"] = df["sec17_code"].apply(lambda x: int(x) if isinstance(x, float) else x)
    df["sec33_code"] = df["sec33_code"].apply(lambda x: int(x) if isinstance(x, float) else x)
    return df[["ticker","code","name","segment","sec33_code","sec33_name","sec17_code","sec17_name","size"]].reset_index(drop=True)


def batch_download_prices(tickers, start, end):
    """バッチでOHLCVダウンロード、Closeのみ返す"""
    result = {}
    for i in range(0, len(tickers), BATCH_SIZE):
        batch = tickers[i:i+BATCH_SIZE]
        print(f"  価格DL バッチ {i//BATCH_SIZE+1}/{(len(tickers)-1)//BATCH_SIZE+1}: {len(batch)}銘柄", flush=True)
        try:
            data = yf.download(
                batch, start=start, end=end,
                interval="1d", group_by="ticker",
                threads=True, progress=False, auto_adjust=True,
            )
            for tk in batch:
                try:
                    if len(batch) > 1:
                        s = data["Close"][tk].dropna()
                    else:
                        s = data["Close"].dropna()
                    if len(s) >= 5:
                        result[tk] = s
                except Exception:
                    pass
        except Exception as e:
            print(f"    バッチDLエラー: {e}", flush=True)
    return result


def calc_return(prices_dict, start_date, end_date):
    """各銘柄のstart→endのリターンを計算"""
    returns = {}
    for tk, s in prices_dict.items():
        try:
            s = s.sort_index()
            # 開始: start_date以降の最初のデータ
            s_start = s[s.index >= pd.Timestamp(start_date)]
            s_end   = s[s.index <= pd.Timestamp(end_date)]
            if len(s_start) < 1 or len(s_end) < 1:
                continue
            p_start = float(s_start.iloc[0])
            p_end   = float(s_end.iloc[-1])
            if p_start > 0:
                returns[tk] = (p_end - p_start) / p_start
        except Exception:
            pass
    return returns


def fetch_profit_margins(tickers, sample_n=30):
    """サンプリングして純利益率を取得"""
    margins = {}
    sample = tickers[:sample_n]
    for tk in sample:
        try:
            info = yf.Ticker(tk).info
            pm = info.get("profitMargins")
            if pm is not None and not np.isnan(pm):
                margins[tk] = float(pm)
        except Exception:
            pass
    return margins


def main():
    print("=" * 70)
    print("業種別パフォーマンス分析")
    print(f"実行日: {date.today()}")
    print("=" * 70)

    # 1. JPXデータ読み込み
    listing = parse_jpx()
    print(f"\nJPX銘柄数: {len(listing)} (プライム:{int((listing.segment==PRIME).sum())}, "
          f"スタンダード:{int((listing.segment==STANDARD).sum())}, グロース:{int((listing.segment==GROWTH).sum())})")

    # プライムのみに絞る（速度と代表性のバランス）
    prime = listing[listing["segment"] == PRIME].copy()
    print(f"プライム銘柄のみ使用: {len(prime)}銘柄")

    tickers = prime["ticker"].tolist()

    # 2. 3年分の価格データを一括DL（2023-04-01〜2026-04-15）
    print("\n[STEP 1] 過去3年分の価格データをダウンロード中...")
    all_prices = batch_download_prices(tickers, "2023-04-01", "2026-04-15")
    print(f"  取得成功: {len(all_prices)}銘柄")

    # 3. 各期間のリターンを計算
    print("\n[STEP 2] 各年度リターンを計算中...")
    period_returns = {}
    for label, s_date, e_date in PERIODS:
        ret = calc_return(all_prices, s_date, e_date)
        period_returns[label] = ret
        print(f"  {label}: {len(ret)}銘柄のリターン算出")

    # 4. 17業種ごとに集計
    print("\n[STEP 3] 17業種別に集計中...")
    sec17_map = prime.drop_duplicates("sec17_code")[["sec17_code","sec17_name"]].sort_values("sec17_code")

    rows = []
    for _, row in sec17_map.iterrows():
        code17 = row["sec17_code"]
        name17 = row["sec17_name"]
        sub = prime[prime["sec17_code"] == code17]
        tks = sub["ticker"].tolist()
        n = len(tks)

        period_stats = {}
        cumulative_vals = []
        for label, s_date, e_date in PERIODS:
            vals = [period_returns[label][tk] for tk in tks if tk in period_returns[label]]
            if vals:
                med = np.median(vals)
                avg = np.mean(vals)
                q75 = np.percentile(vals, 75)
            else:
                med = avg = q75 = np.nan
            period_stats[label] = {"median": med, "mean": avg, "q75": q75, "n": len(vals)}
            if not np.isnan(med):
                cumulative_vals.append(med)

        # 累積: 各年度中央値の複利計算
        if len(cumulative_vals) == 3:
            cum = (1+cumulative_vals[0])*(1+cumulative_vals[1])*(1+cumulative_vals[2]) - 1
        else:
            cum = np.nan

        rows.append({
            "17業種コード": code17,
            "17業種名": name17,
            "銘柄数": n,
            **{f"{lbl}_中央値": period_stats[lbl]["median"] for lbl, _, _ in PERIODS},
            **{f"{lbl}_平均値": period_stats[lbl]["mean"] for lbl, _, _ in PERIODS},
            **{f"{lbl}_上位25%": period_stats[lbl]["q75"] for lbl, _, _ in PERIODS},
            "3年累積(中央値複利)": cum,
        })

    result_df = pd.DataFrame(rows)

    # 5. 利益率：17業種ごとに上位銘柄からサンプリング
    print("\n[STEP 4] 利益率をサンプリング取得中...")
    margin_by_sector = {}
    for _, row in sec17_map.iterrows():
        code17 = row["sec17_code"]
        sub = prime[prime["sec17_code"] == code17]
        tks = sub["ticker"].tolist()
        margins = fetch_profit_margins(tks, sample_n=min(20, len(tks)))
        if margins:
            margin_by_sector[code17] = np.median(list(margins.values()))
            print(f"  {row['sec17_name']}: {len(margins)}銘柄サンプル, 利益率中央値={margin_by_sector[code17]*100:.1f}%")
        else:
            margin_by_sector[code17] = np.nan

    result_df["利益率中央値"] = result_df["17業種コード"].map(margin_by_sector)

    # 6. 33業種→17業種マッピング
    mapping_df = listing.drop_duplicates(["sec33_code"])[["sec33_code","sec33_name","sec17_code","sec17_name"]].sort_values("sec33_code")

    # 7. 出力
    print_results(result_df, mapping_df)
    save_markdown(result_df, mapping_df)


def fmt_pct(v):
    if pd.isna(v):
        return "  N/A "
    return f"{v*100:+6.1f}%"


def print_results(df, mapping_df):
    print("\n" + "=" * 90)
    print("【17業種別 株価上昇率・利益率ランキング】（3年累積順）")
    print("=" * 90)
    sorted_df = df.sort_values("3年累積(中央値複利)", ascending=False, na_position='last')

    labels = [lbl for lbl, _, _ in PERIODS]
    header = f"{'順':>2} {'17業種名':<16} {'銘柄数':>4}  {labels[0]:>10}  {labels[1]:>10}  {labels[2]:>10}  {'3年累積':>8}  {'利益率':>6}"
    print(header)
    print("-" * 90)
    for rank, (_, row) in enumerate(sorted_df.iterrows(), 1):
        y1 = fmt_pct(row[f"{labels[0]}_中央値"])
        y2 = fmt_pct(row[f"{labels[1]}_中央値"])
        y3 = fmt_pct(row[f"{labels[2]}_中央値"])
        cum = fmt_pct(row["3年累積(中央値複利)"])
        margin = fmt_pct(row["利益率中央値"])
        print(f"{rank:>2} {row['17業種名']:<16} {int(row['銘柄数']):>4}  {y1:>10}  {y2:>10}  {y3:>10}  {cum:>8}  {margin:>6}")


def save_markdown(result_df, mapping_df):
    today = date.today().strftime("%Y%m%d")
    path = Path(__file__).resolve().parent / f"sector_analysis_{today}.md"
    labels = [lbl for lbl, _, _ in PERIODS]
    sorted_df = result_df.sort_values("3年累積(中央値複利)", ascending=False, na_position='last')

    lines = [
        f"# 業種別パフォーマンス分析 ({date.today()})",
        "",
        "## 17業種別 株価上昇率・利益率（プライム市場、中央値）",
        "",
        "| 順位 | 17業種 | 銘柄数 | Y1 23→24 | Y2 24→25 | Y3 25→26 | 3年累積 | 利益率中央値 |",
        "|------|--------|--------|----------|----------|----------|---------|------------|",
    ]
    for rank, (_, row) in enumerate(sorted_df.iterrows(), 1):
        y1 = fmt_pct(row[f"{labels[0]}_中央値"]).strip()
        y2 = fmt_pct(row[f"{labels[1]}_中央値"]).strip()
        y3 = fmt_pct(row[f"{labels[2]}_中央値"]).strip()
        cum = fmt_pct(row["3年累積(中央値複利)"]).strip()
        margin = fmt_pct(row["利益率中央値"]).strip()
        lines.append(f"| {rank} | {row['17業種名']} | {int(row['銘柄数'])} | {y1} | {y2} | {y3} | {cum} | {margin} |")

    lines += [
        "",
        "---",
        "",
        "## 各年度 上位25%リターン（17業種別）",
        "",
        "| 17業種 | Y1上位25% | Y2上位25% | Y3上位25% |",
        "|--------|-----------|-----------|-----------|",
    ]
    for _, row in sorted_df.iterrows():
        q1 = fmt_pct(row[f"{labels[0]}_上位25%"]).strip()
        q2 = fmt_pct(row[f"{labels[1]}_上位25%"]).strip()
        q3 = fmt_pct(row[f"{labels[2]}_上位25%"]).strip()
        lines.append(f"| {row['17業種名']} | {q1} | {q2} | {q3} |")

    lines += [
        "",
        "---",
        "",
        "## 33業種 → 17業種 マッピング",
        "",
        "| 33業種コード | 33業種名 | 17業種コード | 17業種名 |",
        "|------------|---------|------------|---------|",
    ]
    for _, row in mapping_df.iterrows():
        lines.append(f"| {int(row['sec33_code'])} | {row['sec33_name']} | {int(row['sec17_code'])} | {row['sec17_name']} |")

    lines += [
        "",
        "---",
        "",
        "## 注記",
        "- 株価上昇率: プライム市場銘柄の中央値（外れ値の影響を除外）",
        "- Y1=2023/04→2024/03, Y2=2024/04→2025/03, Y3=2025/04→2026/04",
        "- 3年累積 = 各年度中央値の複利計算",
        "- 利益率: 各17業種から最大20銘柄をサンプリングした純利益率（profitMargins）の中央値",
        "- データソース: JPX公式銘柄一覧 + yfinance",
    ]

    path.write_text("\n".join(lines), encoding="utf-8")
    print(f"\nMarkdownレポート保存: {path}")


if __name__ == "__main__":
    main()
