"""Markdownレポート生成"""


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

    L("---")
    L("")
    L("## 3. 総合スコアリング結果")
    L("")

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

            L("| 項目 | 値 |")
            L("|------|-----|")
            L(f"| 終値 | {s['close']:,.0f}円 |")
            L(f"| 52週高値比 | {s['diff_pct']:+.1f}% |")
            L(f"| 52週安値比 | +{s['above_low_pct']:.0f}% |")
            if tt:
                L(f"| MA | 50日={tt['ma50']:,.0f} / 150日={tt['ma150']:,.0f} / 200日={tt['ma200']:,.0f} |")
            L("")

            L("**MUST条件（DUKE。メソッド）:**")
            L("")
            L("| 条件 | 結果 | 詳細 |")
            L("|------|------|------|")

            tt_str = f"{tt['pass_count']}/{tt['total']}" if tt else "N/A"
            tt_pass = "PASS" if tt and tt['all_pass'] else "FAIL"
            tt_fails = ""
            if tt and not tt['all_pass']:
                fails = [k for k, v in tt['results'].items() if not v]
                tt_fails = f" 未達: {', '.join(fails)}"
            L(f"| TT（ステージ2） | {tt_pass} | {tt_str}{tt_fails} |")

            if bp and bp.get('detected'):
                bp_str = f"期間{bp['box_period']}日, 値幅{bp['box_range_pct']:.1f}%, 上限{bp['box_high']:,.0f}円"
                bp04_str = "出来高OK" if bp.get('bp04_volume_breakout') else "出来高不足"
                bp05_str = f"乖離{bp['chase_pct']:.1f}%" if bp.get('bp05_early_entry') else f"乖離{bp['chase_pct']:.1f}%(追いかけ注意)"
                L(f"| BP（ボックスブレイク） | PASS | {bp_str} |")
                L(f"| BP-04（出来高） | {'PASS' if bp.get('bp04_volume_breakout') else 'FAIL'} | {bp04_str}（倍率{bp.get('vol_ratio', 0):.2f}x） |")
                L(f"| BP-05（初期エントリー） | {'PASS' if bp.get('bp05_early_entry') else 'WARN'} | {bp05_str} |")
            else:
                L(f"| BP（ボックスブレイク） | FAIL | ボックス圏未検出 |")

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

            L("**NICE条件（ミネルヴィニ/オニール）:**")
            L("")
            L("| 条件 | 結果 | 詳細 |")
            L("|------|------|------|")

            if rs:
                rs_raw = rs.get('rs_raw', 0)
                L(f"| RS（レラティブストレングス） | RS={rs_raw:+.1f} | 12M={rs['stock_ret_12m']:+.1f}%, 6M={rs['stock_ret_6m']:+.1f}%, 3M={rs['stock_ret_3m']:+.1f}% |")
                rs_trend = "改善中" if rs.get('rs_improving') else ("悪化" if rs.get('rs_improving') is False else "N/A")
                L(f"| RS推移 | {rs_trend} | - |")
            else:
                L(f"| RS | N/A | データ不足 |")

            if vcp and vcp.get('detected'):
                contractions_str = " → ".join([f"{c['drawdown_pct']:.1f}%" for c in vcp['contractions']])
                L(f"| VCP | 検出 (+{vcp['score']}pt) | 収縮{vcp['contraction_count']}回: {contractions_str} |")
            else:
                L(f"| VCP | 未検出 | - |")

            canslim = s.get('canslim_nice', {})
            for key, val in canslim.get('details', {}).items():
                L(f"| {key} | - | {val} |")

            roe = fund.get('roe')
            pe = fund.get('pe_trailing')
            if roe is not None:
                L(f"| ROE | {roe*100:.1f}% | {'17%以上' if roe >= 0.17 else '17%未満'} |")
            if pe is not None:
                L(f"| PER | {pe:.1f}x | - |")

            L("")

            bigchange = fund.get('bigchange', {})
            llm_summary = bigchange.get('llm_summary', '')
            bc_summary = bigchange.get('summary', '')
            if llm_summary:
                tdnet_n = bigchange.get('tdnet_count', 0)
                L(f"**F-05 ビッグチェンジ調査報告（TDnet {tdnet_n}件 + Web検索 / claude -p 生成）:**")
                L("")
                for line in llm_summary.strip().split('\n'):
                    L(line)
                L("")
            elif bc_summary:
                L("**F-05 ビッグチェンジ調査報告:**")
                L("")
                for line in bc_summary.strip().split('\n'):
                    L(line)
                L("")

            L("---")
            L("")

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
