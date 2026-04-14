# growth_stock_investing

新高値ブレイク投資法に基づく日本株スクリーニングツール。DUKE。メソッドを必須条件（MUST）、ミネルヴィニ／オニール流のトレンドテンプレート・VCP・CAN-SLIM をボーナス加点（NICE）とする優先度スコアリング方式で、東証上場の主要銘柄ユニバースから買い候補を抽出し Markdown レポートを出力します。

## 特徴

- **市場環境チェック**: 日経平均の移動平均・ディストリビューション・フォロースルーデイ等（M-01〜M-06）
- **52週新高値スキャン**: ユニバース全銘柄から新高値近接銘柄を抽出
- **MUST 条件**: トレンドテンプレート（TT-01〜08）、ボックスブレイク（BP-01〜05）、ファンダメンタルズ（F-01〜F-05）
- **NICE 加点**: 相対強度（RS）、VCP 検出、CAN-SLIM 補完評価、業績加速度
- **総合スコアリング**: 推奨・ウォッチリスト・その他を自動振り分け
- **Markdown レポート**: 実行日付きで `screening_report_YYYYMMDD.md` を自動生成

## 必要環境

- Python 3.9+
- 依存パッケージは [requirements.txt](requirements.txt) を参照

```bash
pip install -r requirements.txt
```

## 使い方

```bash
python screening.py
```

実行すると以下が出力されます。

- 標準出力: 市場環境判定、新高値銘柄、推奨/ウォッチリスト銘柄
- ファイル: `screening_report_YYYYMMDD.md`（当日日付）

## ファイル構成

- [screening.py](screening.py) — メインスクリプト
- [new_high_breakout_investment_rules.md](new_high_breakout_investment_rules.md) — 投資ルール定義
- [requirements.txt](requirements.txt) — 依存パッケージ
- `screening_report_*.md` — 過去の実行レポート

## データソース

yfinance 経由で Yahoo Finance から株価・ファンダメンタルズデータを取得します。
