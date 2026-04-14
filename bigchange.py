"""STEP 5a: ビッグチェンジ分析 (F-05) — yfinanceニュース分類 + TDnet + claude CLI 調査"""
import json
import shutil
import subprocess
import urllib.error
import urllib.request
from datetime import datetime, timedelta

import yfinance as yf


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
    """F-05 [MUST/SEMI]: yfinanceニュースAPIからビッグチェンジ候補を検出。"""
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

        categories_hit = set()
        matched_news = []

        for item in news_items:
            title = item.get("title", "")
            link = item.get("link", "") or item.get("url", "")
            pub_date = ""
            if "providerPublishTime" in item:
                try:
                    pub_date = datetime.fromtimestamp(item["providerPublishTime"]).strftime("%Y-%m-%d")
                except Exception:
                    pass
            elif "publishedDate" in item:
                pub_date = str(item["publishedDate"])[:10]

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
                        break

        seen_titles = set()
        unique_matches = []
        for m in matched_news:
            if m["title"] not in seen_titles:
                seen_titles.add(m["title"])
                unique_matches.append(m)

        result["matches"] = unique_matches
        result["categories_hit"] = sorted(categories_hit)
        result["has_candidate"] = len(unique_matches) > 0

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
                for m in items[:3]:
                    date_str = f"（{m['date']}）" if m['date'] else ""
                    summary_parts.append(f"- {m['title']}{date_str}\n")

            summary_parts.append(f"\n上記ニュースの内容を精査し、"
                                 f"株価上昇の背景にある構造的な変革（ビッグチェンジ）かどうかを最終判断してください。"
                                 f"一時的なニュースや既に織り込み済みの材料は除外する必要があります。\n")

        result["summary"] = "".join(summary_parts)

    except Exception as e:
        result["summary"] = f"{name}のニュース取得中にエラーが発生しました: {e}\nIR資料・ニュースサイトで直接確認してください。"

    return result


def fetch_tdnet_disclosures(ticker, months=6, limit=100):
    """yanoshin TDnet WebAPI から適時開示を取得。"""
    code = ticker.replace(".T", "").strip()
    if not code.isdigit():
        return []

    url = f"https://webapi.yanoshin.jp/webapi/tdnet/list/{code}.json?limit={limit}"
    cutoff = datetime.now() - timedelta(days=months * 31)

    try:
        req = urllib.request.Request(url, headers={"User-Agent": "screening.py/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, json.JSONDecodeError, TimeoutError) as e:
        print(f"    TDnet取得エラー({code}): {e}")
        return []

    items = data.get("items", []) or []
    disclosures = []
    for raw in items:
        d = raw.get("Tdnet") or raw
        pubdate = d.get("pubdate") or d.get("pub_date") or ""
        title = d.get("title") or ""
        doc_url = d.get("document_url") or d.get("url") or ""
        if not (pubdate and title):
            continue
        try:
            dt = datetime.strptime(pubdate[:10], "%Y-%m-%d")
        except ValueError:
            continue
        if dt < cutoff:
            continue
        disclosures.append({
            "date": dt.strftime("%Y-%m-%d"),
            "title": title,
            "url": doc_url,
        })
    return disclosures


BIGCHANGE_LLM_PROMPT_TEMPLATE = """\
あなたは日本株の成長株投資アナリストです。以下の銘柄について、与えた素材と必要に応じたWeb検索結果をもとに、指定フォーマットで調査レポートを作成してください。

# 対象銘柄
- 銘柄名: {name}
- 証券コード: {code}
- 業種: {sector} / {industry}
- 時価総額: {market_cap}
- 直近業績: 売上成長率 {rev_growth} / EPS成長率 {eps_growth} / ROE {roe}

# 素材1: TDnet 適時開示（直近6ヶ月、一次情報・最重要）
{tdnet_block}

# 素材2: yfinance ニュース（参考）
{yfnews_block}

# 指示
- 必要に応じてWeb検索(WebSearch/WebFetch)を使い、業界動向・新製品・経営陣発言などの二次情報を補完してよい
- 素材にない情報を創作しないこと。不明点は「不明」と明記
- 日本語で簡潔に。出典URLは可能な限り併記

# 出力フォーマット（このまま出力）
## 1. 事業概要（3行以内）
- 何で稼いでいる会社か（売上構成の主力セグメント）
- 顧客は誰か（BtoB/BtoC、業界）
- 競合優位性があれば一言で

## 2. ビッグチェンジ（該当するもののみ、なければ「特になし」）
直近6ヶ月以内に起きた変化・材料を箇条書きで。
- **業績変化**: 上方修正、コンセンサス上振れ、営業利益率の急改善
- **新製品/新市場**: 新規事業、海外展開、大型受注
- **業界構造変化**: 規制変更、市場拡大、競合撤退
- **資本政策**: 自社株買い、増配、株式分割、MBO/TOB
- **需給変化**: 大量保有報告、機関投資家の新規組入れ
"""


def _format_tdnet_block(disclosures, limit=30):
    if not disclosures:
        return "（直近6ヶ月の適時開示取得なし）"
    lines = []
    for d in disclosures[:limit]:
        url = f" {d['url']}" if d.get("url") else ""
        lines.append(f"- [{d['date']}] {d['title']}{url}")
    return "\n".join(lines)


def _format_yfnews_block(yf_matches, limit=10):
    if not yf_matches:
        return "（ビッグチェンジキーワードにヒットしたニュースなし）"
    lines = []
    for m in yf_matches[:limit]:
        date = f"[{m['date']}] " if m.get("date") else ""
        lines.append(f"- {date}({m.get('category_label','')}) {m.get('title','')}")
    return "\n".join(lines)


def _fmt_pct(x):
    return f"{x*100:+.1f}%" if isinstance(x, (int, float)) else "N/A"


def _fmt_market_cap(x):
    if not isinstance(x, (int, float)):
        return "N/A"
    return f"{x/1e8:,.0f}億円"


def generate_bigchange_report_via_claude(ticker, name, tdnet, yf_news, fund, timeout=240):
    """Claude Code CLI (`claude -p`) 経由でビッグチェンジ調査レポートを生成する。"""
    claude_bin = shutil.which("claude")
    if not claude_bin:
        return None

    info = fund or {}
    prompt = BIGCHANGE_LLM_PROMPT_TEMPLATE.format(
        name=name or "",
        code=ticker.replace(".T", ""),
        sector=info.get("sector", "N/A"),
        industry=info.get("industry", "N/A"),
        market_cap=_fmt_market_cap(info.get("market_cap")),
        rev_growth=_fmt_pct(info.get("revenue_growth")),
        eps_growth=_fmt_pct(info.get("earnings_growth")),
        roe=_fmt_pct(info.get("roe")),
        tdnet_block=_format_tdnet_block(tdnet),
        yfnews_block=_format_yfnews_block((yf_news or {}).get("matches", [])),
    )

    try:
        proc = subprocess.run(
            [claude_bin, "-p", "--output-format", "text", prompt],
            capture_output=True,
            text=True,
            timeout=timeout,
            encoding="utf-8",
        )
    except subprocess.TimeoutExpired:
        print(f"    claude -p タイムアウト({ticker})")
        return None
    except Exception as e:
        print(f"    claude -p 実行エラー({ticker}): {e}")
        return None

    if proc.returncode != 0:
        print(f"    claude -p 失敗({ticker}) rc={proc.returncode}: {proc.stderr[:200]}")
        return None

    out = (proc.stdout or "").strip()
    return out if out else None


def enrich_with_llm_bigchange(stocks):
    """推奨/監視銘柄に対してTDnet取得＋claude -pでLLM調査レポートを付与する。"""
    if not stocks:
        return
    if not shutil.which("claude"):
        print("\n  claude CLIが見つからないため、LLMビッグチェンジ調査はスキップします。")
        return

    print(f"\n{'=' * 70}")
    print(f"LLMビッグチェンジ調査（{len(stocks)}銘柄, claude -p 経由）")
    print(f"{'=' * 70}")

    for s in stocks:
        ticker = s["ticker"]
        name = s.get("name", "")
        code = ticker.replace(".T", "")
        print(f"  {code} {name}: TDnet取得中...", end="", flush=True)
        tdnet = fetch_tdnet_disclosures(ticker, months=6)
        print(f" {len(tdnet)}件 → claude -p 実行中...", end="", flush=True)

        fund = s.get("fund", {}) or {}
        yf_news = fund.get("bigchange", {}) or {}
        llm_summary = generate_bigchange_report_via_claude(ticker, name, tdnet, yf_news, fund)

        bc = fund.setdefault("bigchange", {})
        bc["tdnet_count"] = len(tdnet)
        bc["tdnet"] = tdnet
        if llm_summary:
            bc["llm_summary"] = llm_summary
            print(" OK")
        else:
            print(" 失敗/スキップ")
