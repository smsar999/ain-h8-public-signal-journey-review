"""Y3e0/Y3e2 Decision Contract — طبقة قرار ظلّية فقط.

This module intentionally does not alter ranker scores, pred_rank, signal_tier,
official flags, or any execution gate.  It only appends explanatory shadow
columns to the final console dataframe after the existing engine has finished
making its official/watch decision.
"""
from __future__ import annotations
from exception_observability import report_suppressed_exception as _report_suppressed_exception

from typing import Any, Iterable, Optional, Set

import numpy as np
import pandas as pd


DECISION_CONTRACT_COLUMNS = [
    "decision_action_shadow",
    "decision_grade_shadow",
    "decision_blockers_shadow",
    "decision_warnings_shadow",
    "decision_reason_shadow_ar",
    "rr_ratio_shadow",
    "breakeven_win_rate_shadow",
]


_TRUE_VALUES = {"1", "true", "yes", "y", "on", "نعم", "صح", "صحيح"}
_BAD_TIME_VALUES = {"ERROR", "STALE", "UNSAFE", "TIME_ERROR", "TIME_STALE", "TIME_UNSAFE"}
_BAD_DATA_VALUES = {"ERROR", "STALE", "UNSAFE", "BAD", "INVALID", "MIXED_MARKET", "LEAK"}

_BLOCKER_AR = {
    "MARKET_PANIC": "حظر سوقي واضح",
    "TIME_UNSAFE": "جودة الوقت غير آمنة أو قديمة",
    "INVALID_PRICE": "السعر غير صالح",
    "INVALID_STOP": "وقف الخسارة غير صالح",
    "SYMBOL_OUT_OF_MARKET": "الرمز لا ينتمي إلى سوق هذه الدورة",
    "DATA_SAFETY_RISK": "يوجد خطر سلامة بيانات",
}

_WARNING_AR = {
    "RANK_MARGIN_LOW": "هامش الرانكر منخفض أو غير مرتفع",
    "OFFICIAL_TOP1_WEAK_TAPE": "التوصية الرسمية جاءت في شريط سوق ضعيف",
    "RR_WEAK": "العائد إلى المخاطرة ضعيف",
    "RR_MEDIUM": "العائد إلى المخاطرة متوسط",
    "NEAR_TARGET": "السعر قريب من الهدف",
    "FAR_FROM_EVALUATION_PRICE": "السعر بعيد عن سعر التقييم",
    "BREADTH_PARTIAL_WEAKNESS": "الاتساع ضعيف جزئيًا بدون حظر سوقي صريح",
    "TIME_SLOT_REVIEW": "وقت الشمعة يحتاج مراجعة ولا يمنع آليًا",
    "RISK_CONTEXT_MISSING": "سياق الوقف/الهدف غير مكتمل في مخرجات الرانكر الخام",
}


def _as_str_series(df: pd.DataFrame, column: str, default: str = "") -> pd.Series:
    if column in df.columns:
        return df[column].fillna(default).astype(str)
    return pd.Series(default, index=df.index, dtype="object")


def _as_num_series(df: pd.DataFrame, column: str, default: float = np.nan) -> pd.Series:
    if column in df.columns:
        return pd.to_numeric(df[column], errors="coerce")
    return pd.Series(default, index=df.index, dtype="float64")


def _as_bool_series(df: pd.DataFrame, column: str, default: bool = False) -> pd.Series:
    if column not in df.columns:
        return pd.Series(default, index=df.index, dtype="bool")
    raw = df[column]
    if pd.api.types.is_bool_dtype(raw):
        return raw.fillna(default).astype(bool)
    if pd.api.types.is_numeric_dtype(raw):
        return pd.to_numeric(raw, errors="coerce").fillna(1 if default else 0).ne(0)
    return raw.fillna(default).astype(str).str.strip().str.lower().isin(_TRUE_VALUES)


def _finite_positive(s: pd.Series) -> pd.Series:
    return np.isfinite(s.astype(float)) & (s.astype(float) > 0)


def _normalise_symbols(values: Optional[Iterable[object]]) -> Set[str]:
    out: Set[str] = set()
    for value in values or []:
        text = str(value or "").strip().upper()
        if text:
            out.add(text)
    return out


def _join_codes(codes: list[str]) -> str:
    return ";".join(codes) if codes else ""


def _reason_ar(action: str, blockers: list[str], warnings: list[str], rr: float) -> str:
    if blockers:
        details = "، ".join(_BLOCKER_AR.get(code, code) for code in blockers)
        return f"{action}: منع ظلي فقط بسبب: {details}. لا يغيّر القرار الرسمي."
    if warnings:
        details = "، ".join(_WARNING_AR.get(code, code) for code in warnings)
        rr_note = f" | R/R={rr:.2f}" if np.isfinite(rr) else ""
        return f"{action}: صالح كقرار ظلي مع تحذيرات: {details}.{rr_note} لا يغيّر القرار الرسمي."
    rr_note = f" | R/R={rr:.2f}" if np.isfinite(rr) else ""
    return f"{action}: لا توجد موانع أو تحذيرات مهمة في طبقة Y3e0 الظلية.{rr_note} لا يغيّر القرار الرسمي."



def _json_safe_scalar(value: Any) -> Any:
    """Return a small JSON-safe scalar for audit reports."""
    if value is None:
        return None
    try:
        if pd.isna(value):
            return None
    except Exception as _suppressed_exc:
        _report_suppressed_exception(
            _suppressed_exc, module=__name__, file=__file__,
            function='_json_safe_scalar', line=113,
            stage='', critical=False,
        )
    try:
        if isinstance(value, (np.integer,)):
            return int(value)
        if isinstance(value, (np.floating,)):
            return float(value) if np.isfinite(value) else None
        if isinstance(value, (np.bool_,)):
            return bool(value)
    except Exception as _suppressed_exc:
        _report_suppressed_exception(
            _suppressed_exc, module=__name__, file=__file__,
            function='_json_safe_scalar', line=122,
            stage='', critical=False,
        )
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except Exception as _suppressed_exc:
            _report_suppressed_exception(
                _suppressed_exc, module=__name__, file=__file__,
                function='_json_safe_scalar', line=127,
                stage='', critical=False,
            )
    return value if isinstance(value, (str, int, float, bool)) else str(value)


def build_decision_contract_audit(
    console_df: pd.DataFrame,
    *,
    version: str,
    market_key: str = "",
    latest_bar: Any = "",
) -> dict[str, Any]:
    """Build a lightweight schema/action audit for the Y3e0 shadow layer.

    This is diagnostic-only. It does not alter the dataframe or any official
    decision fields. It proves that shadow columns exist even when rows=0.
    """
    df = console_df if isinstance(console_df, pd.DataFrame) else pd.DataFrame()
    present = [c for c in DECISION_CONTRACT_COLUMNS if c in df.columns]
    missing = [c for c in DECISION_CONTRACT_COLUMNS if c not in df.columns]
    action_counts: dict[str, int] = {}
    grade_counts: dict[str, int] = {}
    blocker_rows = 0
    warning_rows = 0
    if len(df) and "decision_action_shadow" in df.columns:
        action_counts = {
            str(k): int(v)
            for k, v in df["decision_action_shadow"].fillna("").astype(str).value_counts(dropna=False).items()
            if str(k).strip()
        }
    if len(df) and "decision_grade_shadow" in df.columns:
        grade_counts = {
            str(k): int(v)
            for k, v in df["decision_grade_shadow"].fillna("").astype(str).value_counts(dropna=False).items()
            if str(k).strip()
        }
    if len(df) and "decision_blockers_shadow" in df.columns:
        blocker_rows = int(df["decision_blockers_shadow"].fillna("").astype(str).str.strip().ne("").sum())
    if len(df) and "decision_warnings_shadow" in df.columns:
        warning_rows = int(df["decision_warnings_shadow"].fillna("").astype(str).str.strip().ne("").sum())

    official_count = 0
    watch_count = 0
    if len(df) and "official" in df.columns:
        official_mask = df["official"].fillna(False).astype(str).str.lower().isin(["true", "1", "yes", "y", "نعم"])
        official_count = int(official_mask.sum())
        watch_count = int((~official_mask).sum())

    return {
        "version": str(version),
        "market": str(market_key or ""),
        "bar": _json_safe_scalar(latest_bar),
        "rows": int(len(df)),
        "shadow_columns_ok": not missing,
        "shadow_columns_present": int(len(present)),
        "shadow_columns_expected": int(len(DECISION_CONTRACT_COLUMNS)),
        "missing_columns": missing,
        "action_counts": action_counts,
        "action_counts_ar": action_counts,
        "grade_counts": grade_counts,
        "blocker_rows": blocker_rows,
        "warning_rows": warning_rows,
        "official_count": official_count,
        "watch_count": watch_count,
        "ملخص_عربي": {
            "الإصدار": str(version),
            "السوق": str(market_key or ""),
            "الشمعة": _json_safe_scalar(latest_bar),
            "عدد_صفوف_القرار": int(len(df)),
            "أعمدة_الظل_سليمة": not missing,
            "أعمدة_الظل_الموجودة": int(len(present)),
            "أعمدة_الظل_المتوقعة": int(len(DECISION_CONTRACT_COLUMNS)),
            "الأعمدة_المفقودة": missing,
            "إحصاء_الإجراءات": action_counts,
            "إحصاء_الدرجات": grade_counts,
            "صفوف_بمانع": blocker_rows,
            "صفوف_بتحذير": warning_rows,
            "عدد_الرسمي": official_count,
            "عدد_المراقبة": watch_count,
        },
    }


def apply_decision_contract(
    console_df: pd.DataFrame,
    *,
    valid_symbols: Optional[Iterable[object]] = None,
    rank_margin_high: Optional[float] = None,
) -> pd.DataFrame:
    """Append Y3e0 shadow decision columns to the final console dataframe.

    The function is deliberately side-effect free: it returns a copy and never
    modifies official, signal_tier, pred_rank, or ranker_score.
    """
    out = console_df.copy() if isinstance(console_df, pd.DataFrame) else pd.DataFrame()
    for col in DECISION_CONTRACT_COLUMNS:
        if col not in out.columns:
            out[col] = np.nan if col.endswith("_shadow") and col.startswith(("rr_", "breakeven_")) else ""

    if out.empty:
        return out

    idx = out.index
    n = len(out)
    blockers: list[list[str]] = [[] for _ in range(n)]
    warnings: list[list[str]] = [[] for _ in range(n)]

    def add_block(mask: pd.Series, code: str) -> None:
        vals = mask.fillna(False).to_numpy(dtype=bool)
        for i, flag in enumerate(vals):
            if flag:
                blockers[i].append(code)

    def add_warning(mask: pd.Series, code: str) -> None:
        vals = mask.fillna(False).to_numpy(dtype=bool)
        for i, flag in enumerate(vals):
            if flag:
                warnings[i].append(code)

    official = _as_bool_series(out, "official", False)
    source_upper = _as_str_series(out, "source").str.upper()
    tier_upper = _as_str_series(out, "signal_tier").str.upper()
    market_regime_upper = _as_str_series(out, "market_regime").str.upper()
    time_quality_upper = _as_str_series(out, "time_quality").str.upper()

    close = _as_num_series(out, "close")
    next_open = _as_num_series(out, "next_open")
    entry = next_open.where(_finite_positive(next_open), close)
    stop = _as_num_series(out, "stop")
    target = _as_num_series(out, "target_1")
    if "take_profit" in out.columns:
        take_profit = _as_num_series(out, "take_profit")
        target = target.where(_finite_positive(target), take_profit)

    risk = entry - stop
    reward = target - entry
    rr = reward / risk.replace(0, np.nan)
    rr = rr.where((_finite_positive(entry) & _finite_positive(stop) & _finite_positive(target) & (risk > 0) & (reward > 0)), np.nan)
    breakeven = 1.0 / (1.0 + rr)
    breakeven = breakeven.where(np.isfinite(breakeven), np.nan)

    out["rr_ratio_shadow"] = rr.round(4)
    out["breakeven_win_rate_shadow"] = breakeven.round(4)

    clear_panic_regime = market_regime_upper.eq("PANIC_NO_TRADE") | market_regime_upper.str.startswith("PANIC", na=False)
    add_block(clear_panic_regime | _as_bool_series(out, "market_panic", False), "MARKET_PANIC")
    add_block(time_quality_upper.isin(_BAD_TIME_VALUES), "TIME_UNSAFE")
    add_block(~_finite_positive(close) | ~_finite_positive(entry), "INVALID_PRICE")

    explicit_risk_source = source_upper.str.contains("SNIPER|SWEEP", regex=True, na=False)
    explicit_stop_present = stop.notna()
    invalid_stop = official & ((explicit_risk_source & ~explicit_stop_present) | (explicit_stop_present & ((stop <= 0) | (stop >= entry))))
    add_block(invalid_stop, "INVALID_STOP")

    valid_symbol_set = _normalise_symbols(valid_symbols)
    if valid_symbol_set:
        symbols = _as_str_series(out, "symbol").str.strip().str.upper()
        add_block(~symbols.isin(valid_symbol_set), "SYMBOL_OUT_OF_MARKET")

    data_safety_bad = pd.Series(False, index=idx)
    for col in ("data_safety", "data_quality", "safety_status", "time_safety"):
        if col in out.columns:
            data_safety_bad = data_safety_bad | _as_str_series(out, col).str.upper().isin(_BAD_DATA_VALUES)
    add_block(data_safety_bad, "DATA_SAFETY_RISK")

    rank_margin = _as_num_series(out, "rank_margin_1_2")
    rank_margin_quality = _as_str_series(out, "rank_margin_quality").str.upper()
    if rank_margin_high is not None and np.isfinite(float(rank_margin_high)):
        add_warning(rank_margin.notna() & (rank_margin < float(rank_margin_high)), "RANK_MARGIN_LOW")
    else:
        add_warning(rank_margin_quality.notna() & rank_margin_quality.ne("") & ~rank_margin_quality.str.contains("HIGH", na=False) & rank_margin.notna(), "RANK_MARGIN_LOW")

    add_warning(tier_upper.str.contains("OFFICIAL_TOP1_WEAK_TAPE", na=False), "OFFICIAL_TOP1_WEAK_TAPE")
    add_warning(rr.notna() & (rr < 1.0), "RR_WEAK")
    add_warning(rr.notna() & (rr >= 1.0) & (rr < 1.5), "RR_MEDIUM")
    add_warning(target.notna() & _finite_positive(entry) & (target >= entry) & ((target - entry) / entry.abs().replace(0, np.nan) < 0.003), "NEAR_TARGET")

    eval_col = None
    for candidate in ("gann_evaluation_price", "evaluation_price", "سعر التقييم"):
        if candidate in out.columns:
            eval_col = candidate
            break
    if eval_col:
        evaluation = _as_num_series(out, eval_col)
        add_warning(_finite_positive(evaluation) & _finite_positive(entry) & ((entry - evaluation).abs() / evaluation.abs().replace(0, np.nan) > 0.02), "FAR_FROM_EVALUATION_PRICE")

    breadth = _as_num_series(out, "breadth_score")
    add_warning(breadth.notna() & (breadth < 0.45) & ~clear_panic_regime, "BREADTH_PARTIAL_WEAKNESS")
    add_warning(time_quality_upper.str.contains("WEAK", na=False), "TIME_SLOT_REVIEW")

    raw_ranker_without_risk = official & source_upper.str.contains("RANKER", na=False) & stop.isna() & target.isna()
    add_warning(raw_ranker_without_risk, "RISK_CONTEXT_MISSING")

    pred_rank = _as_num_series(out, "pred_rank")
    watch_like = tier_upper.str.contains("WATCH", na=False) | (pred_rank.notna() & (pred_rank <= 5))

    actions: list[str] = []
    grades: list[str] = []
    reasons: list[str] = []
    block_codes: list[str] = []
    warn_codes: list[str] = []

    rr_values = rr.to_numpy(dtype="float64", copy=False)
    official_values = official.to_numpy(dtype=bool, copy=False)
    watch_values = watch_like.fillna(False).to_numpy(dtype=bool, copy=False)
    rank_quality_values = rank_margin_quality.reindex(idx).fillna("").to_numpy(dtype=object, copy=False)

    for i in range(n):
        row_blockers = blockers[i]
        row_warnings = warnings[i]
        if row_blockers:
            action = "ممنوع"
            grade = "BLOCKED"
        elif official_values[i]:
            if row_warnings:
                action = "دخول بحذر"
                grade = "B-" if (len(row_warnings) >= 3 or "RR_WEAK" in row_warnings) else "B"
            else:
                action = "دخول قوي"
                row_rr = rr_values[i]
                grade = "A+" if ("HIGH" in str(rank_quality_values[i]) and (not np.isfinite(row_rr) or row_rr >= 2.0)) else "A"
        elif watch_values[i]:
            action = "مراقبة قريبة"
            grade = "C"
        else:
            action = "انتظار"
            grade = "WAIT"
        actions.append(action)
        grades.append(grade)
        block_codes.append(_join_codes(row_blockers))
        warn_codes.append(_join_codes(row_warnings))
        reasons.append(_reason_ar(action, row_blockers, row_warnings, rr_values[i]))

    out["decision_action_shadow"] = actions
    out["decision_grade_shadow"] = grades
    out["decision_blockers_shadow"] = block_codes
    out["decision_warnings_shadow"] = warn_codes
    out["decision_reason_shadow_ar"] = reasons

    return out
