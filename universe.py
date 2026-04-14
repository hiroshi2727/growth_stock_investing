"""
ユニバース自動構築モジュール

JPX公式の東証上場銘柄一覧(data_j.xls)を自動ダウンロードし、
yfinanceで流動性（20日平均売買代金）を取得してユニバースを構築する。

構成方針（新高値ブレイク投資法向け）:
  - プライム市場: 売買代金上位500銘柄（大型株偏重を避けつつ流動性を確保）
  - グロース市場: 売買代金上位150銘柄（主力成長株）
  - スタンダード市場: 売買代金上位100銘柄（上位の流動性のみ）
  - コア銘柄（CORE_TICKERS）を常時追加
  - 結果は data/universe_cache.json に日次キャッシュ
"""

from __future__ import annotations

import json
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
import yfinance as yf

JPX_URL = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"

DATA_DIR = Path(__file__).resolve().parent / "data"
JPX_FILE = DATA_DIR / "data_j.xls"
UNIVERSE_CACHE = DATA_DIR / "universe_cache.json"

PRIME = "プライム（内国株式）"
STANDARD = "スタンダード（内国株式）"
GROWTH = "グロース（内国株式）"

TOP_PRIME = 500
TOP_GROWTH = 150
TOP_STANDARD = 100

JPX_CACHE_DAYS = 7
UNIVERSE_CACHE_HOURS = 20

# コア銘柄: 新高値ブレイクの定番成長株。流動性フィルタを通らなくても常時含める
CORE_TICKERS: dict[str, str] = {
    "6920.T": "レーザーテック",
    "6857.T": "アドバンテスト",
    "8035.T": "東京エレクトロン",
    "6861.T": "キーエンス",
    "7741.T": "HOYA",
    "6526.T": "ソシオネクスト",
    "6532.T": "ベイカレント",
    "3659.T": "ネクソン",
    "7974.T": "任天堂",
    "7832.T": "バンダイナムコHD",
    "4063.T": "信越化学工業",
    "6098.T": "リクルートHD",
    "9984.T": "ソフトバンクG",
    "4385.T": "メルカリ",
    "4478.T": "フリー",
    "4443.T": "Sansan",
    "3923.T": "ラクス",
    "4071.T": "プラスアルファ・コンサルティング",
    "4011.T": "ヘッドウォータース",
    "186A.T": "アストロスケールHD",
    "7342.T": "ウェルスナビ",
    "4480.T": "メドレー",
    "4485.T": "JTOWER",
    "3697.T": "SHIFT",
    "2158.T": "FRONTEO",
    "4434.T": "サーバーワークス",
    "3962.T": "チェンジHD",
    "6035.T": "IRジャパンHD",
    "3769.T": "GMOPG",
    "4689.T": "LINEヤフー",
    "4684.T": "オービック",
    "4307.T": "野村総合研究所",
    "4626.T": "太陽ホールディングス",
    "6594.T": "ニデック",
    "6645.T": "オムロン",
    "6954.T": "ファナック",
    "7735.T": "SCREENホールディングス",
    "9735.T": "セコム",
}


def download_jpx_list(force: bool = False) -> Path:
    """JPX公式の東証上場銘柄一覧.xlsをダウンロード（7日キャッシュ）"""
    DATA_DIR.mkdir(exist_ok=True)
    if JPX_FILE.exists() and not force:
        age = datetime.now() - datetime.fromtimestamp(JPX_FILE.stat().st_mtime)
        if age < timedelta(days=JPX_CACHE_DAYS):
            return JPX_FILE
    print(f"  JPX銘柄一覧DL: {JPX_URL}")
    req = urllib.request.Request(JPX_URL, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=60) as r:
        JPX_FILE.write_bytes(r.read())
    print(f"  保存先: {JPX_FILE}")
    return JPX_FILE


def parse_jpx_list(path: Path) -> pd.DataFrame:
    """JPX銘柄一覧をパース。プライム/スタンダード/グロースの内国株式のみ返す"""
    # data_j.xlsはBIFF形式。pandas 2.x + xlrd 2.xでは読めないのでxlrdを直接使う
    import xlrd
    book = xlrd.open_workbook(str(path))
    sheet = book.sheet_by_index(0)
    rows = [sheet.row_values(i) for i in range(sheet.nrows)]
    df = pd.DataFrame(rows[1:], columns=rows[0])
    df = df.rename(columns={
        "コード": "code",
        "銘柄名": "name",
        "市場・商品区分": "segment",
        "33業種区分": "sector33",
        "規模区分": "size",
    })
    df = df[df["segment"].isin([PRIME, STANDARD, GROWTH])].copy()
    df["code"] = df["code"].apply(
        lambda x: str(int(x)) if isinstance(x, (int, float)) else str(x).strip()
    )
    df["ticker"] = df["code"] + ".T"
    return df[["ticker", "code", "name", "segment", "sector33", "size"]].reset_index(drop=True)


def fetch_liquidity(tickers: list[str], period_days: int = 30, batch_size: int = 200) -> pd.DataFrame:
    """yfinance一括DLで20日平均売買代金（円）を算出"""
    print(f"  流動性取得: {len(tickers)}銘柄（バッチサイズ{batch_size}）")
    rows: list[dict] = []
    for i in range(0, len(tickers), batch_size):
        batch = tickers[i:i + batch_size]
        print(f"    バッチ {i // batch_size + 1}/{(len(tickers) - 1) // batch_size + 1}: {len(batch)}銘柄", flush=True)
        data = yf.download(
            batch,
            period=f"{period_days}d",
            interval="1d",
            group_by="ticker",
            threads=True,
            progress=False,
            auto_adjust=False,
        )
        for tk in batch:
            try:
                d = data[tk] if len(batch) > 1 else data
                closes = d["Close"].dropna()
                vols = d["Volume"].dropna()
                if len(closes) < 10 or len(vols) < 10:
                    continue
                tv = (closes * vols).tail(20).mean()
                if pd.isna(tv) or tv <= 0:
                    continue
                rows.append({"ticker": tk, "avg_trading_value": float(tv)})
            except (KeyError, AttributeError, TypeError):
                continue
    return pd.DataFrame(rows)


def build_universe() -> dict[str, str]:
    """ユニバースを構築して dict[ticker->name] を返す"""
    print("=" * 70)
    print("ユニバース構築開始")
    print("=" * 70)
    path = download_jpx_list()
    listing = parse_jpx_list(path)
    print(f"  上場銘柄数: プライム={int((listing['segment'] == PRIME).sum())}, "
          f"スタンダード={int((listing['segment'] == STANDARD).sum())}, "
          f"グロース={int((listing['segment'] == GROWTH).sum())}")

    liq = fetch_liquidity(listing["ticker"].tolist())
    merged = listing.merge(liq, on="ticker", how="inner")
    print(f"  流動性データ取得成功: {len(merged)}銘柄")

    selected_frames = []
    for segment, top_n in [(PRIME, TOP_PRIME), (GROWTH, TOP_GROWTH), (STANDARD, TOP_STANDARD)]:
        sub = merged[merged["segment"] == segment].nlargest(top_n, "avg_trading_value")
        print(f"  {segment}: 上位{len(sub)}銘柄を採用 "
              f"(最小売買代金 {sub['avg_trading_value'].min() / 1e8:.2f}億円/日)")
        selected_frames.append(sub)
    result = pd.concat(selected_frames, ignore_index=True)

    universe = dict(zip(result["ticker"], result["name"]))

    # コア銘柄を常時含める
    added = 0
    for tk, nm in CORE_TICKERS.items():
        if tk not in universe:
            row = listing[listing["ticker"] == tk]
            universe[tk] = row["name"].iloc[0] if not row.empty else nm
            added += 1
    print(f"  コア銘柄追加: {added}銘柄")
    print(f"  最終ユニバース: {len(universe)}銘柄")
    return universe


def load_universe(force_rebuild: bool = False) -> dict[str, str]:
    """キャッシュがあれば利用、なければビルド（日次キャッシュ）"""
    if UNIVERSE_CACHE.exists() and not force_rebuild:
        age = datetime.now() - datetime.fromtimestamp(UNIVERSE_CACHE.stat().st_mtime)
        if age < timedelta(hours=UNIVERSE_CACHE_HOURS):
            with open(UNIVERSE_CACHE, encoding="utf-8") as f:
                cache = json.load(f)
            print(f"  ユニバースキャッシュ使用: {len(cache)}銘柄 (age: {age})")
            return cache

    universe = build_universe()
    DATA_DIR.mkdir(exist_ok=True)
    with open(UNIVERSE_CACHE, "w", encoding="utf-8") as f:
        json.dump(universe, f, ensure_ascii=False, indent=2)
    print(f"  キャッシュ保存: {UNIVERSE_CACHE}")
    return universe


if __name__ == "__main__":
    import sys
    force = "--rebuild" in sys.argv
    u = load_universe(force_rebuild=force)
    print(f"\n合計: {len(u)}銘柄")
