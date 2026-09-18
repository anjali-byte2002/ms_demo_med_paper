"""
ms_dmt_additional_analyses.py -- the 4 additional analyses requested on top of
ms_dmt_analysis_run.py (which already produced ms_summary_tables.xlsx):

  1. dmt_utilization_by_sex       -- drug_class x sex matrix (never built before)
  2. dmt_route_utilization /      -- route (Oral/Injectable/Infusion) utilization
     dmt_by_year_route               overall and by calendar year. Route was DEFINED
                                      per-drug in DMT_REFERENCE but silently discarded
                                      in ms_dmt_analysis_run.py's SQL-generation loop
                                      (bound to `_route`, never turned into a CASE
                                      expression or a route_where clause) -- rebuilt
                                      here purely in pandas via a generic_name->route
                                      map, since ms_dmt already carries
                                      drug_generic_name 1:1 with route. No new SQL
                                      needed for this piece.
  3. sex_ratio_by_year            -- F:M ratio per first_diagnosis_year, to check the
                                      Introduction's cited literature claim (ratio
                                      2.3 -> 3.5:1 "since last decade") against this
                                      actual cohort.
  4. dmt_switch_vs_subtype_transition -- for patients with BOTH a recorded subtype
                                      change (subtype_transitions) and a DMT switch
                                      (dmt_transitions_long), the gap in days between
                                      each DMT switch and that patient's subtype
                                      transition date, bucketed to test whether
                                      prescribing changes cluster around disease-stage
                                      transitions -- the Introduction's central claim,
                                      which nothing in the original notebook tested.

Reuses ms_dmt_analysis_run.py's on-disk checkpoints (./_checkpoints/ms_cohort_demo.pkl,
./_checkpoints/ms_dmt_final.pkl) when present, instead of re-querying the DB for the
same cohort/demographics/DMT data -- both checkpoints are supersets of what this
script needs, so this skips ~40-70 min of redundant work once the primary run has
completed. Falls back to a live (retrying) re-query only if a checkpoint is missing.
The 4 new sheets are appended to that SAME workbook (not a separate file), each with
the same heading-banner style already added to every other tab.

Everything runs on rgd_udm_silver only, per the standing instruction for this task.
Read-only SELECTs only -- no ALTER TABLE / write access.
"""
from __future__ import annotations

import os
import time

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ── Database connection (MySQL, rgd_udm_silver) ──────────────────────────────────────
load_dotenv("/Users/anjali28/Desktop/Eli lily AD model/.env")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT", "3306")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

url = URL.create(
    drivername="mysql+pymysql", username=db_user, password=db_password,
    host=db_host, port=int(db_port), database="rgd_udm_silver",
)
engine = create_engine(
    url, pool_pre_ping=True,
    # bumped from 900s -- the primary run's COHORT_SQL blew past that and killed the
    # socket mid-query; this script reruns the same COHORT_SQL, so it needs the same fix.
    connect_args={"connect_timeout": 30, "read_timeout": 7200, "write_timeout": 7200},
)

OUT_PATH = "/Users/anjali28/Desktop/MS demo paper/ms_summary_tables.xlsx"

# ms_dmt_analysis_run.py checkpoints its two most expensive intermediate tables here
# (added after repeated mid-query connection drops on this DB). Both are supersets of
# what this script needs (same COHORT_SQL/DEMOGRAPHICS_SQL; ms_dmt here only adds a
# `route` column on top via a pure-pandas map) -- reusing them skips ~40-70 min of
# redundant re-querying entirely when the primary run has already completed.
CHECKPOINT_DIR = "./_checkpoints"
COHORT_DEMO_CHECKPOINT = os.path.join(CHECKPOINT_DIR, "ms_cohort_demo.pkl")
MS_DMT_CHECKPOINT = os.path.join(CHECKPOINT_DIR, "ms_dmt_final.pkl")


def log(msg):
    print(f"[additional] {msg}", flush=True)


# ---------------------------------------------------------------------------
# Upstream re-derivation (verbatim from ms_dmt_analysis_run.py cells 2-9, 16)
# ---------------------------------------------------------------------------

MS_ICD_REFERENCE_SQL = """
ms_reference AS (
    SELECT 'G35'   AS ref_code, 'ICD-10' AS coding_system, 'Multiple sclerosis' AS description, 'MS_unspecified' AS ms_subtype
    UNION ALL SELECT '340',   'ICD-9',  'Multiple sclerosis', 'MS_unspecified'
    UNION ALL SELECT 'G35A',  'ICD-10', 'Relapsing-remitting multiple sclerosis', 'RRMS'
    UNION ALL SELECT 'G35B',  'ICD-10', 'Primary progressive multiple sclerosis', 'PPMS'
    UNION ALL SELECT 'G35B0', 'ICD-10', 'Primary progressive multiple sclerosis, unspecified', 'PPMS'
    UNION ALL SELECT 'G35B1', 'ICD-10', 'Active primary progressive multiple sclerosis', 'PPMS'
    UNION ALL SELECT 'G35B2', 'ICD-10', 'Non-active primary progressive multiple sclerosis', 'PPMS'
    UNION ALL SELECT 'G35C',  'ICD-10', 'Secondary progressive multiple sclerosis', 'SPMS'
    UNION ALL SELECT 'G35C0', 'ICD-10', 'Secondary progressive multiple sclerosis, unspecified', 'SPMS'
    UNION ALL SELECT 'G35C1', 'ICD-10', 'Active secondary progressive multiple sclerosis', 'SPMS'
    UNION ALL SELECT 'G35C2', 'ICD-10', 'Non-active secondary progressive multiple sclerosis', 'SPMS'
    UNION ALL SELECT 'G35D',  'ICD-10', 'Multiple sclerosis, unspecified', 'MS_unspecified'
)
"""

COHORT_SQL = f"""
WITH {MS_ICD_REFERENCE_SQL},
diagnosis_norm AS (
    SELECT
        ndid,
        UPPER(TRIM(REPLACE(diag_code_stripped, '.', '')))          AS diagnosis_code_normalized,
        diag_date                                                  AS diagnosis_date
    FROM rgd_udm_silver.diagnosis
    WHERE diag_code_stripped IS NOT NULL
      AND TRIM(diag_code_stripped) <> ''
      AND udm_active_flag = 'Y'
),
ms_rows AS (
    SELECT d.ndid, d.diagnosis_date, r.ms_subtype
    FROM diagnosis_norm d
    INNER JOIN ms_reference r ON d.diagnosis_code_normalized = r.ref_code
),
subtype_first_seen AS (
    SELECT ndid, ms_subtype, MIN(diagnosis_date) AS subtype_first_date
    FROM ms_rows
    GROUP BY ndid, ms_subtype
)
SELECT
    ndid,
    MIN(subtype_first_date)                                                     AS first_diagnosis_date,
    YEAR(MIN(subtype_first_date))                                               AS first_diagnosis_year,
    GROUP_CONCAT(ms_subtype ORDER BY subtype_first_date ASC)                    AS ms_subtypes_ever,
    GROUP_CONCAT(subtype_first_date ORDER BY subtype_first_date ASC)            AS ms_subtype_dates,
    MAX(CASE WHEN ms_subtype = 'RRMS'           THEN 1 ELSE 0 END)              AS rrms_flag,
    MIN(CASE WHEN ms_subtype = 'RRMS'           THEN subtype_first_date END)    AS rrms_date,
    MAX(CASE WHEN ms_subtype = 'PPMS'           THEN 1 ELSE 0 END)              AS ppms_flag,
    MIN(CASE WHEN ms_subtype = 'PPMS'           THEN subtype_first_date END)    AS ppms_date,
    MAX(CASE WHEN ms_subtype = 'SPMS'           THEN 1 ELSE 0 END)              AS spms_flag,
    MIN(CASE WHEN ms_subtype = 'SPMS'           THEN subtype_first_date END)    AS spms_date,
    MAX(CASE WHEN ms_subtype = 'MS_unspecified' THEN 1 ELSE 0 END)              AS ms_unspecified_flag,
    MIN(CASE WHEN ms_subtype = 'MS_unspecified' THEN subtype_first_date END)    AS ms_unspecified_date
FROM subtype_first_seen
GROUP BY ndid
"""

DEMOGRAPHICS_SQL = f"""
SELECT
    base.*,
    CASE
        WHEN age_at_diagnosis BETWEEN 0 AND 19   THEN '<20'
        WHEN age_at_diagnosis BETWEEN 20 AND 29  THEN '20-29'
        WHEN age_at_diagnosis BETWEEN 30 AND 39  THEN '30-39'
        WHEN age_at_diagnosis BETWEEN 40 AND 49  THEN '40-49'
        WHEN age_at_diagnosis BETWEEN 50 AND 59  THEN '50-59'
        WHEN age_at_diagnosis BETWEEN 60 AND 69  THEN '60-69'
        WHEN age_at_diagnosis >= 70              THEN '70+'
        ELSE 'Unknown'
    END                                                      AS age_group,
    CASE
        WHEN age_at_diagnosis BETWEEN 0 AND 17   THEN 'Pediatric-onset'
        WHEN age_at_diagnosis BETWEEN 18 AND 49  THEN 'Adult-onset'
        WHEN age_at_diagnosis BETWEEN 50 AND 59  THEN 'Late-onset'
        WHEN age_at_diagnosis >= 60               THEN 'Very-late-onset'
        ELSE 'Unknown'
    END                                                      AS onset_category
FROM (
    SELECT
        cohort.*,
        p.dob                                                        AS birth_year,
        CASE
            WHEN (CAST(cohort.first_diagnosis_year AS SIGNED) - CAST(p.dob AS SIGNED)) >= 0
            THEN (CAST(cohort.first_diagnosis_year AS SIGNED) - CAST(p.dob AS SIGNED))
            ELSE NULL
        END                                                          AS age_at_diagnosis,
        p.gender_hl7_std                                             AS gender,
        p.pat_state                                                  AS state
    FROM ({COHORT_SQL}) AS cohort
    LEFT JOIN rgd_udm_silver.patients p
        ON cohort.ndid = p.ndid
        AND p.udm_active_flag = 'Y'
) AS base
"""

if os.path.exists(COHORT_DEMO_CHECKPOINT):
    ms_cohort_demo = pd.read_pickle(COHORT_DEMO_CHECKPOINT)
    log(f"Step 1-2/4: loaded ms_cohort_demo from checkpoint ({len(ms_cohort_demo):,} rows) -- skipping the re-query")
else:
    log("Step 1-2/4: no checkpoint found -- running DEMOGRAPHICS_SQL live ...")
    t0 = time.time()
    last_exc = None
    for attempt in range(1, 5):
        try:
            with engine.connect() as conn:
                ms_cohort_demo = pd.read_sql(text(DEMOGRAPHICS_SQL), conn)
            break
        except Exception as exc:
            last_exc = exc
            log(f"  attempt {attempt} failed -- {exc}")
            if attempt < 4:
                time.sleep(30 * attempt)
    else:
        raise last_exc
    log(f"  demographics rows: {len(ms_cohort_demo):,} ({time.time()-t0:.0f}s)")


def _gender_bucket(g):
    if pd.isna(g):
        return "Unknown"
    g_low = str(g).strip().lower()
    if g_low.startswith("f"):
        return "Female"
    if g_low.startswith("m"):
        return "Male"
    return "Unknown"


ms_cohort_demo["gender_bucket"] = ms_cohort_demo["gender"].apply(_gender_bucket)
total_cohort_n = len(ms_cohort_demo)

# ── DMT reference (verbatim from ms_dmt_analysis_run.py cell 7) ─────────────────────
DMT_REFERENCE = [
    ("Interferon beta-1a",    "Interferon beta",               "Standard efficacy", "Injectable", ["interferon beta-1a", "interferon beta 1a", "avonex", "rebif"], ["peginterferon"]),
    ("Interferon beta-1b",    "Interferon beta",               "Standard efficacy", "Injectable", ["interferon beta-1b", "interferon beta 1b", "betaseron", "extavia"], []),
    ("Peginterferon beta-1a", "Interferon beta",               "Standard efficacy", "Injectable", ["peginterferon beta-1a", "peginterferon beta 1a", "plegridy"], []),
    ("Glatiramer acetate",    "Glatiramoid",                    "Standard efficacy", "Injectable", ["glatiramer", "copaxone", "glatopa"], []),
    ("Dimethyl fumarate",     "Fumarate",                       "Standard efficacy", "Oral",       ["dimethyl fumarate", "tecfidera"], []),
    ("Diroximel fumarate",    "Fumarate",                       "Standard efficacy", "Oral",       ["diroximel fumarate", "vumerity"], []),
    ("Monomethyl fumarate",   "Fumarate",                       "Standard efficacy", "Oral",       ["monomethyl fumarate", "bafiertam"], []),
    ("Teriflunomide",         "Pyrimidine synthesis inhibitor", "Standard efficacy", "Oral",       ["teriflunomide", "aubagio"], []),
    ("Fingolimod",            "S1P modulator",                  "High efficacy",     "Oral",       ["fingolimod", "gilenya"], []),
    ("Siponimod",             "S1P modulator",                  "High efficacy",     "Oral",       ["siponimod", "mayzent"], []),
    ("Ozanimod",              "S1P modulator",                  "High efficacy",     "Oral",       ["ozanimod", "zeposia"], []),
    ("Ponesimod",             "S1P modulator",                  "High efficacy",     "Oral",       ["ponesimod", "ponvory"], []),
    ("Cladribine",            "Purine analog",                  "High efficacy",     "Oral",       ["cladribine", "mavenclad"], []),
    ("Natalizumab",           "Anti-alpha4-integrin",           "High efficacy",     "Infusion",   ["natalizumab", "tysabri"], []),
    ("Alemtuzumab",           "Anti-CD52",                      "High efficacy",     "Infusion",   ["alemtuzumab", "lemtrada"], []),
    ("Ocrelizumab",           "Anti-CD20",                      "High efficacy",     "Infusion",   ["ocrelizumab", "ocrevus"], []),
    ("Ofatumumab",            "Anti-CD20",                      "High efficacy",     "Injectable", ["ofatumumab", "kesimpta"], []),
    ("Ublituximab",           "Anti-CD20",                      "High efficacy",     "Infusion",   ["ublituximab", "briumvi"], []),
    ("Mitoxantrone",          "Immunosuppressant",              "High efficacy",     "Infusion",   ["mitoxantrone", "novantrone"], []),
]
route_map = {generic: route for generic, _cls, _tier, route, _terms, _exc in DMT_REFERENCE}
all_classes = sorted({row[1] for row in DMT_REFERENCE})
all_routes = sorted({row[3] for row in DMT_REFERENCE})

generic_clauses, where_clauses = [], []
for generic, drug_class, tier, _route, terms, exclude_terms in DMT_REFERENCE:
    conds = []
    for t in terms:
        esc = t.replace("'", "''")
        conds.append(
            f"(LOWER(med_name) LIKE '%{esc}%' OR LOWER(med_name_std_BN) LIKE '%{esc}%' "
            f"OR LOWER(med_name_std_LN) LIKE '%{esc}%')"
        )
    combined = " OR ".join(conds)
    if exclude_terms:
        exclude_conds = []
        for et in exclude_terms:
            esc_et = et.replace("'", "''")
            exclude_conds.append(
                f"(LOWER(med_name) LIKE '%{esc_et}%' OR LOWER(med_name_std_BN) LIKE '%{esc_et}%' "
                f"OR LOWER(med_name_std_LN) LIKE '%{esc_et}%')"
            )
        combined = f"({combined}) AND NOT ({' OR '.join(exclude_conds)})"
    generic_clauses.append(f"WHEN {combined} THEN '{generic}'")
    where_clauses.append(f"({combined})")

generic_case_sql = "CASE " + " ".join(generic_clauses) + " END"
dmt_where_sql = " OR ".join(where_clauses)

if os.path.exists(MS_DMT_CHECKPOINT):
    ms_dmt = pd.read_pickle(MS_DMT_CHECKPOINT)
    log(f"Step 3/4: loaded ms_dmt from checkpoint ({len(ms_dmt):,} rows) -- skipping the batch re-query")
else:
    log("Step 3/4: no checkpoint found -- running the DMT medication batch extraction live (~20-25 min) ...")
    BATCH_SIZE = 1000
    cohort_ndids = ms_cohort_demo["ndid"].astype(str).tolist()
    results = []
    t_start = time.time()
    for i in range(0, len(cohort_ndids), BATCH_SIZE):
        batch = cohort_ndids[i : i + BATCH_SIZE]
        id_list = ",".join(batch)
        sql = text(f"""
            SELECT
                ndid,
                {generic_case_sql}   AS drug_generic_name,
                med_start_date                                              AS med_start_date_raw,
                med_fill_date,
                enc_date,
                COALESCE(med_start_date, med_fill_date, enc_date)           AS med_start_date,
                med_end_date
            FROM rgd_udm_silver.medication FORCE INDEX (idx_ndid)
            WHERE ndid IN ({id_list})
              AND udm_active_flag = 'Y'
              AND ({dmt_where_sql})
        """)
        last_exc = None
        for attempt in range(1, 5):
            try:
                with engine.connect() as conn:
                    batch_df = pd.read_sql(sql, conn)
                break
            except Exception as exc:
                last_exc = exc
                log(f"  batch at {i} attempt {attempt} failed -- {exc}")
                if attempt < 4:
                    time.sleep(30 * attempt)
        else:
            raise last_exc
        results.append(batch_df)
        done = i + len(batch)
        if (done // BATCH_SIZE) % 10 == 0 or done >= len(cohort_ndids):
            log(f"  [{done}/{len(cohort_ndids)}] batch matches: {len(batch_df)} (elapsed {time.time()-t_start:.0f}s)")

    ms_dmt = pd.concat(results, ignore_index=True)
    for _col in ["med_start_date_raw", "med_fill_date", "enc_date", "med_start_date", "med_end_date"]:
        ms_dmt[_col] = pd.to_datetime(ms_dmt[_col], errors="coerce")
    _date_floor, _date_ceiling = pd.Timestamp("1985-01-01"), pd.Timestamp.now() + pd.Timedelta(days=7)
    for _col in ["med_start_date", "med_end_date"]:
        _bad = (ms_dmt[_col] < _date_floor) | (ms_dmt[_col] > _date_ceiling)
        ms_dmt.loc[_bad, _col] = pd.NaT

ms_dmt["route"] = ms_dmt["drug_generic_name"].map(route_map)
log(f"  Total DMT rows: {len(ms_dmt):,}; distinct patients: {ms_dmt['ndid'].nunique():,}")

n_dmt_treated = ms_dmt["ndid"].nunique()

# ---------------------------------------------------------------------------
# Shared year-expansion (verbatim pattern from ms_dmt_analysis_run.py cell 16)
# ---------------------------------------------------------------------------
STUDY_END_YEAR = int(max(
    ms_cohort_demo["first_diagnosis_year"].max(),
    ms_dmt["med_start_date"].dt.year.max(),
    ms_dmt["med_end_date"].dt.year.max() if ms_dmt["med_end_date"].notna().any() else 0,
))
_dmt_years = ms_dmt.copy()
_dmt_years["start_year"] = _dmt_years["med_start_date"].dt.year
_dmt_years["end_year"] = _dmt_years["med_end_date"].dt.year
_dmt_years["end_year"] = _dmt_years["end_year"].fillna(STUDY_END_YEAR).clip(upper=STUDY_END_YEAR).astype(int)
_dmt_years = _dmt_years[_dmt_years["start_year"].notna() & (_dmt_years["start_year"] <= _dmt_years["end_year"])]
_dmt_years["start_year"] = _dmt_years["start_year"].astype(int)
_dmt_years["active_years"] = [list(range(s, e + 1)) for s, e in zip(_dmt_years["start_year"], _dmt_years["end_year"])]

dmt_year_expanded = (
    _dmt_years[["ndid", "drug_generic_name", "route", "active_years"]]
    .explode("active_years")
    .rename(columns={"active_years": "year"})
    .drop_duplicates()
)

_first_dx_year = ms_cohort_demo["first_diagnosis_year"]
year_range = range(int(_first_dx_year.min()), STUDY_END_YEAR + 1)
cumulative_diagnosed_by_year = pd.Series(
    {y: int((_first_dx_year <= y).sum()) for y in year_range}
)
cumulative_diagnosed_by_year.index.name = "year"
cumulative_diagnosed_by_year.name = "cumulative_diagnosed"

log("Step 4/4: building the 4 requested analyses ...")


# ===========================================================================
# 1. dmt_utilization_by_sex
# ===========================================================================
sex_order = ["Female", "Male", "Unknown"]
dmt_utilization_by_sex = pd.DataFrame(index=all_classes)
dmt_utilization_by_sex.index.name = "drug_class"
# ms_dmt may come from the checkpoint (primary script's SQL already computed
# drug_class natively) or from a fresh re-query in this script (which only pulls
# drug_generic_name) -- map drug_class from DMT_REFERENCE directly rather than
# merging, so this works either way without a duplicate-column collision.
class_map = {generic: cls for generic, cls, _tier, _route, _terms, _exc in DMT_REFERENCE}
_ms_dmt_with_class = ms_dmt.copy()
_ms_dmt_with_class["drug_class"] = _ms_dmt_with_class["drug_generic_name"].map(class_map)
for sex in sex_order:
    sex_ndids = set(ms_cohort_demo.loc[ms_cohort_demo["gender_bucket"] == sex, "ndid"])
    sex_n = len(sex_ndids)
    sex_dmt = _ms_dmt_with_class[_ms_dmt_with_class["ndid"].isin(sex_ndids)]
    class_counts = sex_dmt.groupby("drug_class")["ndid"].nunique()
    dmt_utilization_by_sex[f"{sex} (N)"] = [int(class_counts.get(c, 0)) for c in all_classes]
    dmt_utilization_by_sex[f"{sex} (%)"] = [
        round(100 * class_counts.get(c, 0) / sex_n, 1) if sex_n else None for c in all_classes
    ]
log("  built dmt_utilization_by_sex")

# ===========================================================================
# 2. dmt_route_utilization + dmt_by_year_route + dmt_by_year_route_coverage
# ===========================================================================
route_patients = ms_dmt.groupby("route")["ndid"].nunique()
dmt_route_utilization = pd.DataFrame({
    "N_patients": route_patients,
    "% of total cohort": (100 * route_patients / total_cohort_n).round(1),
    "% of DMT-treated patients": (100 * route_patients / n_dmt_treated).round(1),
}).reindex(all_routes).sort_values("N_patients", ascending=False)

dmt_by_year_absolute_route = (
    dmt_year_expanded.groupby(["year", "route"])["ndid"].nunique().unstack(fill_value=0)
    .reindex(year_range, fill_value=0)
)
dmt_by_year_coverage_route = (
    dmt_by_year_absolute_route.div(cumulative_diagnosed_by_year, axis=0).mul(100).round(1)
)
log("  built dmt_route_utilization / dmt_by_year_route (absolute + coverage)")

# ===========================================================================
# 3/4. sex_ratio_by_year
# ===========================================================================
_by_year = ms_cohort_demo.groupby("first_diagnosis_year")
_year_n = _by_year.size()
_female_n = ms_cohort_demo[ms_cohort_demo["gender_bucket"] == "Female"].groupby("first_diagnosis_year").size()
_male_n = ms_cohort_demo[ms_cohort_demo["gender_bucket"] == "Male"].groupby("first_diagnosis_year").size()
sex_ratio_by_year = pd.DataFrame({"N_patients": _year_n}).sort_index()
sex_ratio_by_year["Female_N"] = _female_n.reindex(sex_ratio_by_year.index, fill_value=0).astype(int)
sex_ratio_by_year["Male_N"] = _male_n.reindex(sex_ratio_by_year.index, fill_value=0).astype(int)
sex_ratio_by_year["Female_pct"] = (100 * sex_ratio_by_year["Female_N"] / sex_ratio_by_year["N_patients"]).round(1)
sex_ratio_by_year["Male_pct"] = (100 * sex_ratio_by_year["Male_N"] / sex_ratio_by_year["N_patients"]).round(1)
sex_ratio_by_year["F_to_M_ratio"] = (
    sex_ratio_by_year["Female_N"] / sex_ratio_by_year["Male_N"].replace(0, np.nan)
).round(2)
log("  built sex_ratio_by_year")

# ===========================================================================
# 5. dmt_switch_vs_subtype_transition
# ===========================================================================
# rebuild subtype_transitions (verbatim from ms_dmt_analysis_run.py cell 41)
subtype_seq = ms_cohort_demo["ms_subtypes_ever"].str.split(",")
subtype_dates_seq = ms_cohort_demo["ms_subtype_dates"].str.split(",")
multi_mask = subtype_seq.str.len() > 1
subtype_transitions = ms_cohort_demo.loc[multi_mask, ["ndid"]].copy()
subtype_transitions["last_subtype_date"] = subtype_dates_seq[multi_mask].str[-1].values
subtype_transitions["transition_pattern"] = subtype_seq[multi_mask].str.join(" -> ").values
subtype_transitions["last_subtype_date"] = pd.to_datetime(subtype_transitions["last_subtype_date"], errors="coerce")

# rebuild dmt_transitions_long (verbatim from ms_dmt_analysis_run.py cells 27-28)
patient_drug_first_start = (
    ms_dmt.groupby(["ndid", "drug_generic_name"])["med_start_date"].min()
    .reset_index().sort_values(["ndid", "med_start_date"])
)
_seq = patient_drug_first_start.groupby("ndid").agg(
    drug_list=("drug_generic_name", list), date_list=("med_start_date", list),
)
_seq["n_drugs"] = _seq["drug_list"].str.len()
multi_dmt_seq = _seq[_seq["n_drugs"] > 1].copy()

transition_rows = []
for ndid, row in multi_dmt_seq.iterrows():
    drugs, dates = row["drug_list"], row["date_list"]
    for step, (from_drug, to_drug, from_date, to_date) in enumerate(
        zip(drugs[:-1], drugs[1:], dates[:-1], dates[1:]), start=1
    ):
        transition_rows.append((ndid, step, from_drug, to_drug, to_date))
dmt_transitions_long = pd.DataFrame(
    transition_rows, columns=["ndid", "step", "from_drug", "to_drug", "switch_date"]
)

# join: patients with BOTH a recorded subtype transition AND >=1 DMT switch
_joined = dmt_transitions_long.merge(
    subtype_transitions[["ndid", "last_subtype_date", "transition_pattern"]], on="ndid", how="inner"
)
_joined["gap_days"] = (_joined["switch_date"] - _joined["last_subtype_date"]).dt.days
# a NaT switch_date or last_subtype_date (missing dates) leaves gap_days as NaN --
# drop those rows before idxmin, since an all-NaN group has no valid argmin and
# idxmin() returns NaN, which .loc[] can't index with.
_joined = _joined.dropna(subset=["gap_days"])

# per-patient: keep only the switch closest in time to their subtype transition
_joined["abs_gap"] = _joined["gap_days"].abs()
_closest = _joined.loc[_joined.groupby("ndid")["abs_gap"].idxmin()].copy()


def _window_bucket(g):
    if pd.isna(g):
        return "Unknown"
    if abs(g) <= 90:
        return "Within +/-90 days"
    if abs(g) <= 180:
        return "Within +/-180 days (excl. above)"
    if abs(g) <= 365:
        return "Within +/-365 days (excl. above)"
    return "More than 365 days apart"


_closest["window_bucket"] = _closest["gap_days"].apply(_window_bucket)
_closest["direction"] = np.where(
    _closest["gap_days"].isna(), "Unknown",
    np.where(_closest["gap_days"] >= 0, "DMT switch on/after subtype transition",
             "DMT switch before subtype transition"),
)

n_multi_subtype = int(multi_mask.sum())
n_multi_dmt = len(multi_dmt_seq)
n_both = len(_closest)

window_order = ["Within +/-90 days", "Within +/-180 days (excl. above)",
                 "Within +/-365 days (excl. above)", "More than 365 days apart", "Unknown"]
_wc = _closest["window_bucket"].value_counts()
dmt_switch_vs_subtype_summary = pd.DataFrame([
    ("Patients with >1 recorded subtype (subtype_transitions)", n_multi_subtype, None),
    ("Patients with >1 distinct DMT (multi_dmt_seq)", n_multi_dmt, None),
    ("Patients with BOTH (this analysis's population)", n_both,
     round(100 * n_both / n_multi_subtype, 1) if n_multi_subtype else None),
] + [
    (f"  -- {w}", int(_wc.get(w, 0)), round(100 * _wc.get(w, 0) / n_both, 1) if n_both else None)
    for w in window_order
], columns=["Metric", "N", "%"])

dmt_switch_vs_subtype_detail = _closest[
    ["ndid", "transition_pattern", "last_subtype_date", "from_drug", "to_drug",
     "switch_date", "gap_days", "window_bucket", "direction"]
].sort_values("gap_days")

dmt_switch_vs_subtype_by_pattern = (
    _closest.groupby("transition_pattern")
    .agg(N_patients=("ndid", "nunique"), Median_gap_days=("gap_days", "median"),
         Pct_within_180d=("gap_days", lambda s: round(100 * (s.abs() <= 180).mean(), 1)))
    .sort_values("N_patients", ascending=False)
)
log("  built dmt_switch_vs_subtype_transition (summary + detail + by-pattern)")

# ---------------------------------------------------------------------------
# Append to the existing workbook
# ---------------------------------------------------------------------------
from openpyxl import load_workbook
from openpyxl.utils.dataframe import dataframe_to_rows
from openpyxl.styles import Font, PatternFill, Alignment

HEADER_FILL = PatternFill(start_color="FF305496", end_color="FF305496", fill_type="solid")
HEADER_FONT = Font(color="FFFFFFFF", bold=True)
WRAP_TOP = Alignment(wrap_text=True, vertical="top", horizontal="left")

NEW_SHEETS = [
    ("dmt_utilization_by_sex", dmt_utilization_by_sex, True,
     "drug_class x sex (Female/Male/Unknown) matrix -- N/% within each sex's own patient count. "
     "Not built in the original notebook despite sex being one of the paper's 4 stated axes."),
    ("dmt_route_utilization", dmt_route_utilization, True,
     "Unique patients ever on each route of administration (Oral/Injectable/Infusion), as % of "
     "full cohort and of DMT-treated patients. Route was defined per-drug in DMT_REFERENCE but "
     "silently unused in the original SQL-generation loop -- rebuilt here in pandas via the "
     "existing generic_name->route mapping (no new SQL needed)."),
    ("dmt_by_year_absolute_route", dmt_by_year_absolute_route, True,
     "Unique patients per route ACTIVE in each calendar year (period prevalence) -- shows the "
     "injectable -> oral -> infusion shift over time."),
    ("dmt_by_year_coverage_route", dmt_by_year_coverage_route, True,
     "dmt_by_year_absolute_route / cumulative patients diagnosed by that year, as a %."),
    ("sex_ratio_by_year", sex_ratio_by_year, True,
     "Female:Male ratio per first_diagnosis_year -- tests the Introduction's cited literature "
     "claim (ratio 2.3 -> 3.5:1 'since last decade') against this specific cohort."),
    ("dmt_switch_vs_subtype", dmt_switch_vs_subtype_summary, False,
     "Among patients with a recorded subtype change AND a DMT switch: how close in time (days) "
     "the nearest DMT switch falls to that patient's subtype-transition date -- the most direct "
     "test of the Introduction's claim that prescribing evolves 'across disease-stage transitions', "
     "which nothing in the original notebook tested. (Full name dmt_switch_vs_subtype_transition "
     "shortened -- Excel sheet names cap at 31 characters.)"),
    ("dmt_switch_vs_subtype_detail", dmt_switch_vs_subtype_detail, False,
     "Patient-level detail behind dmt_switch_vs_subtype: each patient's closest DMT switch to "
     "their subtype-transition date, the gap in days, and which side of the transition it fell on."),
    ("dmt_switch_vs_subtype_pattern", dmt_switch_vs_subtype_by_pattern, True,
     "Same analysis, broken out by transition_pattern (e.g. RRMS -> SPMS): N patients, median gap "
     "in days, and % whose DMT switch fell within +/-180 days of the subtype transition."),
]

wb = load_workbook(OUT_PATH)
# clean up any stale sheets from a prior run where a long name got silently
# truncated to 31 chars by openpyxl (e.g. dmt_switch_vs_subtype_transitio) --
# match by prefix so those orphans don't linger alongside the correctly-named ones.
_new_names = {name[:31] for name, *_ in NEW_SHEETS}
for existing in list(wb.sheetnames):
    if existing not in _new_names and any(existing.startswith(n[:28]) for n in _new_names if n.startswith("dmt_switch")):
        del wb[existing]

for name, df, keep_index, desc in NEW_SHEETS:
    if name in wb.sheetnames:
        del wb[name]
    assert len(name) <= 31, f"sheet name too long for Excel: {name!r}"
    ws = wb.create_sheet(name)
    for r in dataframe_to_rows(df.reset_index() if keep_index else df, index=False, header=True):
        ws.append(r)
    max_col = max(ws.max_column, 1)
    ws.insert_rows(1)
    ws.cell(row=1, column=1, value=f"{name} -- {desc}")
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=max_col)
    ws["A1"].fill = HEADER_FILL
    ws["A1"].font = HEADER_FONT
    ws["A1"].alignment = WRAP_TOP
    ws.row_dimensions[1].height = 30
    for cell in ws[2]:
        cell.fill = HEADER_FILL
        cell.font = HEADER_FONT
    ws.freeze_panes = ws.cell(row=3, column=1)

wb.save(OUT_PATH)
log(f"Appended {len(NEW_SHEETS)} new sheets to {OUT_PATH}")
log("=== Done ===")
