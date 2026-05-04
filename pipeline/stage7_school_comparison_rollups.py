"""Stage 7 — Recompute `school_comparison_rollups` from active `programs` + satellite tables.

Maps product comparison axes to DB coverage:
  1) Academic & reputation — already on `schools` (qs_* + tier), not duplicated here.
  2) Admissions difficulty — aggregates `program_evaluations.application_difficulty_score`.
  3) Fees — medians over `program_fees.international_tuition_fee` (per dominant currency).
  4) Career linkage — heuristic from `programs.career_paths` lengths + alumni list size.
  5) Intl / language barrier — mins over `program_admissions` plus `schools.international_students_page`.

Resource axis uses `school_resource_metrics` populated by Stage 6.

Scans **all active programs** once, then aggregates per school — `--batch` is ignored for this stage
(deliberate full recompute for consistent dashboards).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from statistics import median
from typing import Any

from config.settings import Settings
from db.supabase_client import TABLE as SCHOOLS_TABLE, get_client
from utils.logger import get_logger

log = get_logger("stage7_rollups")

PROGRAMS_TABLE = "programs"
FEES_TABLE = "program_fees"
ADM_TABLE = "program_admissions"
EVAL_TABLE = "program_evaluations"
ROLLUPS_TABLE = "school_comparison_rollups"

PAGE_PROGRAMS = 400


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _median_int(vals: list[int]) -> int | None:
    if not vals:
        return None
    return int(round(float(median(vals))))


def _median_float(vals: list[float]) -> float | None:
    if not vals:
        return None
    return float(median(vals))


def _chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[i : i + size] for i in range(0, len(items), size)]


def _normalize_career_list(val: Any) -> list:
    if val is None:
        return []
    if isinstance(val, list):
        return [x for x in val if x is not None and str(x).strip()]
    return []


def _normalize_alumni(val: Any) -> list:
    return _normalize_career_list(val)


def _career_signal(entries: int, alumni_n: int) -> int | None:
    score_raw = entries + alumni_n * 3
    if score_raw <= 0:
        return None
    if score_raw < 6:
        return 1
    if score_raw < 14:
        return 2
    if score_raw < 28:
        return 3
    if score_raw < 48:
        return 4
    return 5


def _fetch_programs_page(client: Any, start: int, end: int) -> list[dict]:
    resp = (
        client.table(PROGRAMS_TABLE)
        .select("id,school_id,career_paths,status")
        .eq("status", "active")
        .range(start, end)
        .execute()
    )
    return resp.data or []


def _fetch_rows_by_program_ids(client: Any, table: str, program_ids: list[str]) -> list[dict]:
    out: list[dict] = []
    for group in _chunks(program_ids, 80):
        resp = client.table(table).select("*").in_("program_id", group).execute()
        out.extend(resp.data or [])
    return out


def _fetch_schools_slice(client: Any, school_ids: list[str]) -> dict[str, dict]:
    out: dict[str, dict] = {}
    for group in _chunks(school_ids, 40):
        resp = (
            client.table(SCHOOLS_TABLE)
            .select("id,international_students_page,notable_alumni")
            .in_("id", group)
            .execute()
        )
        for r in resp.data or []:
            rid = r.get("id")
            if rid:
                out[str(rid)] = r
    return out


def _fee_medians_by_currency(rows: list[dict]) -> tuple[dict[str, float], str | None, bool]:
    by_ccy: dict[str, list[float]] = defaultdict(list)
    for r in rows:
        fee = r.get("international_tuition_fee")
        ccy = (str(r.get("currency_code") or "")).strip().upper()
        if fee is None or len(ccy) != 3:
            continue
        try:
            by_ccy[ccy].append(float(fee))
        except (TypeError, ValueError):
            continue
    medians: dict[str, float] = {}
    for ccy, vals in by_ccy.items():
        m = _median_float(vals)
        if m is not None:
            medians[ccy] = m
    if not medians:
        return {}, None, False
    dominant = max(medians.keys(), key=lambda c: len(by_ccy[c]))
    mixed = len(medians) > 1
    return medians, dominant, mixed


def _rollup_payload_for_school(
    *,
    school_id: str,
    program_ids: list[str],
    career_entries: int,
    school_meta: dict,
    eval_rows: list[dict],
    fee_rows: list[dict],
    adm_rows: list[dict],
) -> dict:
    difficulties = []
    for er in eval_rows:
        ds = er.get("application_difficulty_score")
        if ds is None:
            continue
        try:
            v = int(ds)
        except (TypeError, ValueError):
            continue
        if 1 <= v <= 5:
            difficulties.append(v)

    ielts_vals: list[float] = []
    toefl_vals: list[int] = []
    for ar in adm_rows:
        io = ar.get("ielts_overall")
        if io is not None:
            try:
                fv = float(io)
                if 0 < fv <= 9:
                    ielts_vals.append(fv)
            except (TypeError, ValueError):
                pass
        tb = ar.get("toefl_ibt")
        if tb is not None:
            try:
                iv = int(tb)
                if iv > 0:
                    toefl_vals.append(iv)
            except (TypeError, ValueError):
                pass

    medians_map, dom_ccy, mixed = _fee_medians_by_currency(fee_rows)
    dom_median = medians_map.get(dom_ccy) if dom_ccy else None

    alumni = _normalize_alumni(school_meta.get("notable_alumni"))
    career_score = _career_signal(career_entries, len(alumni))

    has_intl = bool((school_meta.get("international_students_page") or "").strip())

    now = _utc_now_iso()

    adm_prog_ids = {str(ar.get("program_id")) for ar in adm_rows if ar.get("program_id")}

    return {
        "school_id": school_id,
        "median_application_difficulty_score": _median_int(difficulties),
        "programs_with_evaluation_count": len(
            {str(er.get("program_id")) for er in eval_rows if er.get("program_id")}
        ),
        "median_international_tuition_fee": dom_median,
        "tuition_dominant_currency_code": dom_ccy,
        "international_fee_medians_json": medians_map if medians_map else None,
        "programs_with_international_fee_count": len(
            {str(fr.get("program_id")) for fr in fee_rows if fr.get("program_id")}
        ),
        "intl_fee_mixed_currency": mixed,
        "career_paths_total_entries": career_entries,
        "notable_alumni_count": len(alumni),
        "career_signal_score": career_score,
        "min_ielts_overall": min(ielts_vals) if ielts_vals else None,
        "min_toefl_ibt": min(toefl_vals) if toefl_vals else None,
        "programs_with_admissions_count": len(adm_prog_ids),
        "has_international_students_page": has_intl,
        "programs_active_for_rollup": len(program_ids),
        "rollup_computed_at": now,
        "updated_at": now,
    }


def run(settings: Settings, batch_size: int) -> None:  # noqa: ARG002 — retained for CLI parity
    _ = batch_size
    client = get_client(settings)

    per_school: dict[str, dict[str, Any]] = defaultdict(lambda: {"pids": [], "career": 0})
    pid_to_sid: dict[str, str] = {}

    idx = 0
    prog_total = 0
    while True:
        end = idx + PAGE_PROGRAMS - 1
        chunk = _fetch_programs_page(client, idx, end)
        if not chunk:
            break
        for prog in chunk:
            sid = prog.get("school_id")
            pid = prog.get("id")
            if sid is None or pid is None:
                continue
            sid_s = str(sid)
            pid_s = str(pid)
            per_school[sid_s]["pids"].append(pid_s)
            pid_to_sid[pid_s] = sid_s
            per_school[sid_s]["career"] += len(_normalize_career_list(prog.get("career_paths")))
            prog_total += 1
        idx += PAGE_PROGRAMS

    log.info(
        "Stage 7: aggregating rollups across %s active program rows, %s school(s)",
        prog_total,
        len(per_school),
    )

    school_ids_ordered = sorted(per_school.keys())
    touched = 0

    def partition_satellite_rows(rows: list[dict]) -> defaultdict[str, list]:
        buck: defaultdict[str, list] = defaultdict(list)
        for row in rows:
            pid_s = str(row.get("program_id") or "")
            sid_s = pid_to_sid.get(pid_s)
            if sid_s:
                buck[sid_s].append(row)
        return buck

    for group in _chunks(school_ids_ordered, 36):
        schools_meta = _fetch_schools_slice(client, group)

        merged_pids: list[str] = []
        for sid_g in group:
            merged_pids.extend(per_school.get(sid_g, {}).get("pids") or [])
        merged_pids = list(dict.fromkeys(merged_pids))

        eval_all = (
            _fetch_rows_by_program_ids(client, EVAL_TABLE, merged_pids) if merged_pids else []
        )
        fee_all = (
            _fetch_rows_by_program_ids(client, FEES_TABLE, merged_pids) if merged_pids else []
        )
        adm_all = (
            _fetch_rows_by_program_ids(client, ADM_TABLE, merged_pids) if merged_pids else []
        )

        eval_by_s = partition_satellite_rows(eval_all)
        fee_by_s = partition_satellite_rows(fee_all)
        adm_by_s = partition_satellite_rows(adm_all)

        for sid in group:
            bucket = per_school.get(sid) or {}
            program_ids = list(dict.fromkeys(bucket.get("pids") or []))
            career_ent = int(bucket.get("career") or 0)
            meta = schools_meta.get(sid) or {"id": sid}
            meta.setdefault("id", sid)

            eval_rows = eval_by_s.get(sid, [])
            fee_rows = fee_by_s.get(sid, [])
            adm_rows = adm_by_s.get(sid, [])

            payload = _rollup_payload_for_school(
                school_id=sid,
                program_ids=program_ids,
                career_entries=career_ent,
                school_meta=meta,
                eval_rows=eval_rows,
                fee_rows=fee_rows,
                adm_rows=adm_rows,
            )
            try:
                client.table(ROLLUPS_TABLE).upsert(payload).execute()
            except Exception as exc:
                log.error("rollup upsert failed school_id=%s: %s", sid, exc)
                continue
            touched += 1

    log.info("Stage 7 complete: upserted %s school_comparison_rollups row(s).", touched)
