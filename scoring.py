"""STEP 7: 総合スコアリング（MUST達成率 + NICEボーナス）"""


def calculate_total_score(stock):
    """MUST達成率とNICEボーナスを統合した総合スコアを計算する。"""
    must_checks = []
    must_score = 0
    nice_score = 0

    tt = stock.get("tt")
    if tt and tt["all_pass"]:
        must_checks.append(("TT(ステージ2)", True))
        must_score += 10
    else:
        must_checks.append(("TT(ステージ2)", False))

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

    must_score += fund.get("f03_score", 0)
    must_score += fund.get("f04_score", 0)

    must_checks.append(("F-05(ビッグチェンジ)", "SEMI"))

    must_pass = sum(1 for _, v in must_checks if v is True)
    must_fail = sum(1 for _, v in must_checks if v is False)
    must_semi = sum(1 for _, v in must_checks if v == "SEMI")
    must_total = must_pass + must_fail
    must_rate = (must_pass / must_total * 100) if must_total > 0 else 0

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

    vcp = stock.get("vcp")
    if vcp and vcp.get("detected"):
        nice_score += vcp.get("score", 0)

    canslim = stock.get("canslim_nice", {})
    nice_score += canslim.get("score", 0)

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
