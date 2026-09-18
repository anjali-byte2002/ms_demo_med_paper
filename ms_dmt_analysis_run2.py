
# ===== cell 0 =====
# ms_dmt_analysis_run2.py -- revision of ms_dmt_analysis_run.py (NOT modified; kept as-is
# for reference / reproducibility of the original ms_summary_tables.xlsx / paper draft).
#
# Three changes requested on top of the original pipeline:
#   1. Date range filter: restrict ALL pulled data (diagnoses, medications, procedures)
#      strictly to 2015-01-01 through 2026-06-30 (inclusive). This also changes cohort
#      membership itself, since a patient now qualifies only via a diagnosis event that
#      falls inside this window -- see the DATE_START/DATE_END note in cell 1b.
#   2. Cross-table retrieval: for the (now date-restricted) MS cohort, pull DMT-relevant
#      records from BOTH rgd_udm_silver.medication (as before) AND rgd_udm_silver.procedures
#      (new), and combine them into a single unified ms_dmt table.
#   3. Infusion/injectable capture via Procedures: MS DMTs administered by a clinician
#      (infusions, in-office injectables) are billed via HCPCS/CPT drug-administration
#      codes on the Procedures table, which the original medications-only pipeline could
#      not see at all. A small HCPCS crosswalk (verified against what's actually present
#      in this DB -- see cell 7b) captures these: e.g. J2350 = Ocrelizumab/Ocrevus 1mg
#      (the example given in the request), J2323 = Natalizumab, J0202 = Alemtuzumab,
#      J1826/Q3027 = Interferon beta-1a, J1595 = Glatiramer acetate.
#
# Everything else (DMT_REFERENCE drug list, cohort/diagnosis logic, all downstream
# utilization / time-to-first-DMT / switching / discontinuation analyses) is reused
# verbatim from ms_dmt_analysis_run.py -- only the pieces above are new.
import os
import time

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL

# ===== cell 1 =====
# ── Database connection (MySQL, rgd_udm_silver) ──────────────────────────────────────
load_dotenv("/Users/anjali28/Desktop/Eli lily AD model/.env")

db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT", "3306")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")

if not all([db_host, db_port, db_user, db_password]):
    raise ValueError("Missing required database connection parameters in .env file")

url = URL.create(
    drivername="mysql+pymysql",
    username=db_user,
    password=db_password,
    host=db_host,
    port=int(db_port),
    database="rgd_udm_silver",
)
engine = create_engine(
    url,
    pool_pre_ping=True,
    connect_args={"connect_timeout": 30, "read_timeout": 7200, "write_timeout": 7200},
)

# ── Checkpoint + retry helper (same pattern as ms_dmt_analysis_run.py) ───────────────
import pickle

# separate checkpoint dir from the original run -- the SQL here differs (date filter +
# a new procedures pull), so the original's cached .pkl files must NOT be reused.
CHECKPOINT_DIR = "./_checkpoints2"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)


def checkpointed_read_sql(name, sql, max_retries=4, base_delay=30):
    path = os.path.join(CHECKPOINT_DIR, f"{name}.pkl")
    if os.path.exists(path):
        df = pd.read_pickle(path)
        print(f"[checkpoint] {name}: loaded {len(df):,} rows from {path}", flush=True)
        return df
    last_exc = None
    for attempt in range(1, max_retries + 1):
        try:
            print(f"[checkpoint] {name}: running query (attempt {attempt}/{max_retries})...", flush=True)
            t0 = time.time()
            with engine.connect() as conn:
                df = pd.read_sql(sql, conn)
            print(f"[checkpoint] {name}: {len(df):,} rows in {time.time()-t0:.0f}s", flush=True)
            df.to_pickle(path)
            return df
        except Exception as exc:
            last_exc = exc
            print(f"[checkpoint] {name}: attempt {attempt} failed -- {exc}", flush=True)
            if attempt < max_retries:
                delay = base_delay * attempt
                print(f"[checkpoint] {name}: retrying in {delay}s...", flush=True)
                time.sleep(delay)
    raise last_exc

# ===== cell 1b =====
# ── Study date window (NEW) ──────────────────────────────────────────────────────────
# Applied to every date-bearing table pulled below: diagnosis.diag_date (so this also
# redefines cohort MEMBERSHIP -- a patient qualifies only via a diagnosis event that
# falls inside this window), medication's effective date (med_start_date -> med_fill_date
# -> enc_date), and procedures' effective date (proc_start_date -> encounter_date ->
# enc_date_proxy). A consequence: "first_diagnosis_date" downstream means "first
# qualifying diagnosis WITHIN this window", not necessarily the patient's true lifetime-
# first MS diagnosis if they had an earlier one before 2015.
DATE_START = "2015-01-01"
DATE_END = "2026-06-30"

# ===== cell 2 =====
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

# ===== cell 3 =====
# diag_date filter added (NEW) -- everything else identical to ms_dmt_analysis_run.py
DIAGNOSIS_NORMALIZED_SQL = f"""
WITH {MS_ICD_REFERENCE_SQL},
diagnosis_norm AS (
    SELECT
        ndid,
        diag_code_stripped                                        AS diagnosis_code_raw,
        UPPER(TRIM(REPLACE(diag_code_stripped, '.', '')))          AS diagnosis_code_normalized,
        diag_coding_system_std                                     AS coding_system_src,
        icd10_desc_std                                             AS icd10_desc_std,
        icd9_desc_std                                              AS icd9_desc_std,
        diag_date                                                  AS diagnosis_date,
        YEAR(diag_date)                                            AS diagnosis_year
    FROM rgd_udm_silver.diagnosis
    WHERE diag_code_stripped IS NOT NULL
      AND TRIM(diag_code_stripped) <> ''
      AND udm_active_flag = 'Y'
      AND diag_date BETWEEN '{DATE_START}' AND '{DATE_END}'
)
SELECT
    d.ndid,
    d.diagnosis_code_raw,
    d.diagnosis_code_normalized,
    COALESCE(d.coding_system_src, r.coding_system)                 AS coding_system,
    r.ms_subtype,
    r.description                                                  AS diag_description,
    d.icd10_desc_std,
    d.icd9_desc_std,
    d.diagnosis_date,
    d.diagnosis_year
FROM diagnosis_norm d
INNER JOIN ms_reference r
    ON d.diagnosis_code_normalized = r.ref_code
"""

ms_diagnoses = checkpointed_read_sql("ms_diagnoses", text(DIAGNOSIS_NORMALIZED_SQL))

print(f"Qualifying MS diagnosis rows (within {DATE_START}..{DATE_END}): {len(ms_diagnoses):,}")
print(f"Distinct patients (ndid): {ms_diagnoses['ndid'].nunique():,}")
ms_diagnoses.head()

# ===== cell 4 =====
# diag_date filter added (NEW) -- everything else identical to ms_dmt_analysis_run.py
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
      AND diag_date BETWEEN '{DATE_START}' AND '{DATE_END}'
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

ms_cohort = checkpointed_read_sql("ms_cohort", text(COHORT_SQL))

print(f"MS cohort size (distinct patients, diagnosed within {DATE_START}..{DATE_END}): {len(ms_cohort):,}")
ms_cohort.head()

# ===== cell 5 =====
# unchanged from ms_dmt_analysis_run.py -- built on top of the new (date-restricted) COHORT_SQL
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
        p.pat_race_std                                               AS race,
        p.pat_ethnicity_std                                          AS ethnicity,
        p.pat_marital_status_std                                     AS marital_status,
        p.pat_city                                                   AS city,
        p.pat_state                                                  AS state,
        p.pat_zip                                                    AS zip,
        p.pat_country                                                AS country
    FROM ({COHORT_SQL}) AS cohort
    LEFT JOIN rgd_udm_silver.patients p
        ON cohort.ndid = p.ndid
        AND p.udm_active_flag = 'Y'
) AS base
"""

ms_cohort_demo = checkpointed_read_sql("ms_cohort_demo", text(DEMOGRAPHICS_SQL))

print(f"MS cohort with demographics: {len(ms_cohort_demo):,} rows")
print(f"Rows with implausible/missing birth year (age_at_diagnosis is null): "
      f"{ms_cohort_demo['age_at_diagnosis'].isna().sum():,}")
ms_cohort_demo.head()

# ===== cell 6 =====
# unchanged from ms_dmt_analysis_run.py
DOB_QUALITY_SQL = f"""
SELECT
    cohort.ndid,
    p.dob                                                        AS birth_year,
    cohort.first_diagnosis_year,
    CASE WHEN p.dob IS NOT NULL
         THEN (CAST(cohort.first_diagnosis_year AS SIGNED) - CAST(p.dob AS SIGNED))
    END                                                           AS raw_age_at_diagnosis,
    CASE
        WHEN p.dob IS NULL
            THEN 'missing_dob'
        WHEN (CAST(cohort.first_diagnosis_year AS SIGNED) - CAST(p.dob AS SIGNED)) < 0
            THEN 'negative_age'
        WHEN (CAST(cohort.first_diagnosis_year AS SIGNED) - CAST(p.dob AS SIGNED)) > 100
            THEN 'implausible_over_100'
        ELSE 'plausible'
    END                                                           AS dob_quality_flag
FROM ({COHORT_SQL}) AS cohort
LEFT JOIN rgd_udm_silver.patients p
    ON cohort.ndid = p.ndid
    AND p.udm_active_flag = 'Y'
"""

dob_quality = checkpointed_read_sql("dob_quality", text(DOB_QUALITY_SQL))

print(dob_quality["dob_quality_flag"].value_counts())
print()

missing_dob = dob_quality[dob_quality["dob_quality_flag"] == "missing_dob"]
negative_age = dob_quality[dob_quality["dob_quality_flag"] == "negative_age"]
implausible_over_100 = dob_quality[dob_quality["dob_quality_flag"] == "implausible_over_100"]

print(f"missing_dob ({len(missing_dob)}):")
print(missing_dob.head(10).to_string(index=False))
print(f"\nnegative_age ({len(negative_age)}):")
print(negative_age.head(10).to_string(index=False))
print(f"\nimplausible_over_100 ({len(implausible_over_100)}):")
print(implausible_over_100.head(15).to_string(index=False))

# ===== cell 7 =====
# unchanged from ms_dmt_analysis_run.py -- same 19-drug DMT reference (generic, class,
# tier, route, search terms, exclude terms)
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

print(f"{len(DMT_REFERENCE)} DMTs defined")

# ===== cell 7b =====
# ── HCPCS/CPT crosswalk for Procedures-table capture (NEW) ──────────────────────────
# Injectable/infusion MS DMTs administered in a clinical setting are billed on
# rgd_udm_silver.procedures via HCPCS Level II drug-administration (J-code) values,
# which the medications-only pipeline never looked at. This list was NOT guessed --
# it was verified against what actually exists in this DB: every plausible MS-DMT
# J-code (J0202, J1595, J1826, J1830, J2323, J2327, J2350, J9293, Q3027, Q3028, plus
# several unclassified-biologic codes) was checked against procedures.proc_code_std,
# and only the codes below were both present AND resolved (via proc_name_std) to an
# MS DMT rather than an unrelated biologic. Notably J2327 in THIS database is
# "Risankizumab-rzaa" (Skyrizi, for psoriasis/Crohn's), not ublituximab -- it is
# deliberately excluded rather than misattributed. Mitoxantrone (J9293), Betaseron/
# Extavia (J1830), and ublituximab/ofatumumab-specific codes were checked and are
# NOT present in this DB's procedures data, so those drugs continue to be captured
# only via the Medications table, exactly as before.
#
#   J2350 -- Injection, ocrelizumab, 1 mg          (Ocrevus)  -- the example cited in
#                                                                 the change request
#   J2323 -- Natalizumab injection, 1 mg           (Tysabri)
#   J0202 -- Injection, alemtuzumab                (Lemtrada)
#   J1826 -- Interferon beta-1a injection, 30 mcg  (Avonex)
#   Q3027 -- Interferon beta-1a IM injection, 1 mcg (Avonex, non-ESRD supply)
#   J1595 -- Injection, glatiramer acetate, 20 mg  (Copaxone/Glatopa)
HCPCS_DMT_REFERENCE = [
    ("J2350", "Ocrelizumab"),
    ("J2323", "Natalizumab"),
    ("J0202", "Alemtuzumab"),
    ("J1826", "Interferon beta-1a"),
    ("Q3027", "Interferon beta-1a"),
    ("J1595", "Glatiramer acetate"),
]

# lookups from the single DMT_REFERENCE list, reused for both the medication CASE/WHEN
# fragments (cell 8) and the procedures crosswalk (cell 9b) so class/tier/route can
# never disagree between the two source tables for the same generic drug
class_map = {generic: cls for generic, cls, _tier, _route, _terms, _exc in DMT_REFERENCE}
tier_map = {generic: tier for generic, _cls, tier, _route, _terms, _exc in DMT_REFERENCE}
route_map = {generic: route for generic, _cls, _tier, route, _terms, _exc in DMT_REFERENCE}

print(f"{len(HCPCS_DMT_REFERENCE)} HCPCS codes mapped for Procedures-table DMT capture")

# ===== cell 8 =====
# unchanged from ms_dmt_analysis_run.py -- build the three parallel CASE/WHEN fragments
# (generic name, class, efficacy tier) for the medication query
generic_clauses, class_clauses, tier_clauses, where_clauses = [], [], [], []
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
    class_clauses.append(f"WHEN {combined} THEN '{drug_class}'")
    tier_clauses.append(f"WHEN {combined} THEN '{tier}'")
    where_clauses.append(f"({combined})")

generic_case_sql = "CASE " + " ".join(generic_clauses) + " END"
class_case_sql = "CASE " + " ".join(class_clauses) + " END"
tier_case_sql = "CASE " + " ".join(tier_clauses) + " END"
dmt_where_sql = " OR ".join(where_clauses)

# ===== cell 9 =====
# ── Medications batch pull (date filter added; otherwise identical to
# ms_dmt_analysis_run.py's cell 9) ──────────────────────────────────────────────────
BATCH_DIR = os.path.join(CHECKPOINT_DIR, "ms_dmt_batches")
os.makedirs(BATCH_DIR, exist_ok=True)
FINAL_MS_DMT_MEDS_PATH = os.path.join(CHECKPOINT_DIR, "ms_dmt_meds_final.pkl")

BATCH_SIZE = 1000
cohort_ndids = ms_cohort_demo["ndid"].astype(str).tolist()

if os.path.exists(FINAL_MS_DMT_MEDS_PATH):
    ms_dmt_meds = pd.read_pickle(FINAL_MS_DMT_MEDS_PATH)
    print(f"[checkpoint] ms_dmt_meds: loaded {len(ms_dmt_meds):,} rows from {FINAL_MS_DMT_MEDS_PATH}", flush=True)
else:
    t_start = time.time()
    n_batches = (len(cohort_ndids) + BATCH_SIZE - 1) // BATCH_SIZE

    for b in range(n_batches):
        batch_path = os.path.join(BATCH_DIR, f"batch_{b:04d}.pkl")
        if os.path.exists(batch_path):
            continue

        i = b * BATCH_SIZE
        batch = cohort_ndids[i : i + BATCH_SIZE]
        id_list = ",".join(batch)

        sql = text(f"""
            SELECT
                ndid,
                {generic_case_sql}   AS drug_generic_name,
                {class_case_sql}     AS drug_class,
                {tier_case_sql}      AS efficacy_tier,
                med_name,
                med_code,
                med_code_std,
                med_start_date                                              AS med_start_date_raw,
                med_fill_date,
                enc_date,
                COALESCE(med_start_date, med_fill_date, enc_date)           AS med_start_date,
                CASE
                    WHEN med_start_date IS NOT NULL THEN 'med_start_date'
                    WHEN med_fill_date IS NOT NULL  THEN 'med_fill_date'
                    WHEN enc_date IS NOT NULL       THEN 'enc_date'
                    ELSE NULL
                END                                                         AS med_start_date_source,
                med_end_date,
                discont_date,
                discont_reason,
                med_status,
                source,
                ehr_source_name
            FROM rgd_udm_silver.medication FORCE INDEX (idx_ndid)
            WHERE ndid IN ({id_list})
              AND udm_active_flag = 'Y'
              AND ({dmt_where_sql})
              AND COALESCE(med_start_date, med_fill_date, enc_date) BETWEEN '{DATE_START}' AND '{DATE_END}'
        """)

        last_exc = None
        for attempt in range(1, 5):
            try:
                with engine.connect() as conn:
                    batch_df = pd.read_sql(sql, conn)
                batch_df.to_pickle(batch_path)
                done = i + len(batch)
                print(f"[meds {done}/{len(cohort_ndids)}] batch {b} matches: {len(batch_df)}  "
                      f"(elapsed {time.time() - t_start:.0f}s)", flush=True)
                break
            except Exception as exc:
                last_exc = exc
                print(f"[meds batch {b}] attempt {attempt} failed -- {exc}", flush=True)
                if attempt < 4:
                    time.sleep(30 * attempt)
        else:
            raise last_exc

    results = [pd.read_pickle(os.path.join(BATCH_DIR, f"batch_{b:04d}.pkl")) for b in range(n_batches)]
    ms_dmt_meds = pd.concat(results, ignore_index=True)

for _col in ["med_start_date_raw", "med_fill_date", "enc_date", "med_start_date", "med_end_date", "discont_date"]:
    ms_dmt_meds[_col] = pd.to_datetime(ms_dmt_meds[_col], errors="coerce")

_date_floor, _date_ceiling = pd.Timestamp("1985-01-01"), pd.Timestamp.now() + pd.Timedelta(days=7)
for _col in ["med_start_date", "med_end_date", "discont_date"]:
    _bad = (ms_dmt_meds[_col] < _date_floor) | (ms_dmt_meds[_col] > _date_ceiling)
    print(f"{_col}: nulling {_bad.sum()} implausible date(s)")
    ms_dmt_meds.loc[_bad, _col] = pd.NaT

ms_dmt_meds["source_table"] = "Medications"
ms_dmt_meds.to_pickle(FINAL_MS_DMT_MEDS_PATH)

print(f"\nTotal Medications-sourced DMT rows: {len(ms_dmt_meds):,}")
print(f"Distinct patients on a DMT (via Medications): {ms_dmt_meds['ndid'].nunique():,}")
ms_dmt_meds.head()

# ===== cell 9b =====
# ── Procedures batch pull (NEW) -- MS DMT infusion/injectable administration codes ──
# Same batching-by-ndid / checkpoint / retry pattern as cell 9, run against
# rgd_udm_silver.procedures instead of .medication, matched by exact HCPCS code
# (proc_code_std / proc_code) rather than a name LIKE pattern -- J-codes are drug-
# specific, so no substring-collision risk the way "interferon beta-1a" vs.
# "peginterferon beta-1a" was for medication name matching.
PROC_BATCH_DIR = os.path.join(CHECKPOINT_DIR, "ms_dmt_proc_batches")
os.makedirs(PROC_BATCH_DIR, exist_ok=True)
FINAL_MS_DMT_PROCS_PATH = os.path.join(CHECKPOINT_DIR, "ms_dmt_procs_final.pkl")

hcpcs_case_clauses = " ".join(
    f"WHEN proc_code_std = '{code}' OR proc_code = '{code}' THEN '{generic}'"
    for code, generic in HCPCS_DMT_REFERENCE
)
hcpcs_generic_case_sql = "CASE " + hcpcs_case_clauses + " END"
hcpcs_code_list_sql = ",".join(f"'{code}'" for code, _generic in HCPCS_DMT_REFERENCE)

if os.path.exists(FINAL_MS_DMT_PROCS_PATH):
    ms_dmt_procs = pd.read_pickle(FINAL_MS_DMT_PROCS_PATH)
    print(f"[checkpoint] ms_dmt_procs: loaded {len(ms_dmt_procs):,} rows from {FINAL_MS_DMT_PROCS_PATH}", flush=True)
else:
    t_start = time.time()
    n_batches = (len(cohort_ndids) + BATCH_SIZE - 1) // BATCH_SIZE

    for b in range(n_batches):
        batch_path = os.path.join(PROC_BATCH_DIR, f"batch_{b:04d}.pkl")
        if os.path.exists(batch_path):
            continue

        i = b * BATCH_SIZE
        batch = cohort_ndids[i : i + BATCH_SIZE]
        id_list = ",".join(batch)

        sql = text(f"""
            SELECT
                ndid,
                {hcpcs_generic_case_sql}  AS drug_generic_name,
                proc_code,
                proc_code_std,
                proc_name,
                proc_start_date                                             AS proc_start_date_raw,
                encounter_date,
                enc_date_proxy,
                COALESCE(proc_start_date, encounter_date, enc_date_proxy)   AS med_start_date,
                CASE
                    WHEN proc_start_date IS NOT NULL THEN 'proc_start_date'
                    WHEN encounter_date IS NOT NULL  THEN 'encounter_date'
                    WHEN enc_date_proxy IS NOT NULL  THEN 'enc_date_proxy'
                    ELSE NULL
                END                                                         AS med_start_date_source,
                ehr_source_name
            FROM rgd_udm_silver.procedures FORCE INDEX (idx_ndid)
            WHERE ndid IN ({id_list})
              AND udm_active_flag = 'Y'
              AND (proc_code_std IN ({hcpcs_code_list_sql}) OR proc_code IN ({hcpcs_code_list_sql}))
              AND COALESCE(proc_start_date, encounter_date, enc_date_proxy) BETWEEN '{DATE_START}' AND '{DATE_END}'
        """)

        last_exc = None
        for attempt in range(1, 5):
            try:
                with engine.connect() as conn:
                    batch_df = pd.read_sql(sql, conn)
                batch_df.to_pickle(batch_path)
                done = i + len(batch)
                print(f"[procs {done}/{len(cohort_ndids)}] batch {b} matches: {len(batch_df)}  "
                      f"(elapsed {time.time() - t_start:.0f}s)", flush=True)
                break
            except Exception as exc:
                last_exc = exc
                print(f"[procs batch {b}] attempt {attempt} failed -- {exc}", flush=True)
                if attempt < 4:
                    time.sleep(30 * attempt)
        else:
            raise last_exc

    results = [pd.read_pickle(os.path.join(PROC_BATCH_DIR, f"batch_{b:04d}.pkl")) for b in range(n_batches)]
    ms_dmt_procs = pd.concat(results, ignore_index=True)

for _col in ["proc_start_date_raw", "encounter_date", "enc_date_proxy", "med_start_date"]:
    ms_dmt_procs[_col] = pd.to_datetime(ms_dmt_procs[_col], errors="coerce")

_bad = (ms_dmt_procs["med_start_date"] < _date_floor) | (ms_dmt_procs["med_start_date"] > _date_ceiling)
print(f"med_start_date (procedures): nulling {_bad.sum()} implausible date(s)")
ms_dmt_procs.loc[_bad, "med_start_date"] = pd.NaT

# a single infusion/injection encounter has no separate "end date" the way a
# prescription fill does -- treat it as a same-day course, matching how dmt_courses
# (cell 30) aggregates min(start)/max(end) across all of a patient's records for a drug
ms_dmt_procs["med_end_date"] = ms_dmt_procs["med_start_date"]
ms_dmt_procs["drug_class"] = ms_dmt_procs["drug_generic_name"].map(class_map)
ms_dmt_procs["efficacy_tier"] = ms_dmt_procs["drug_generic_name"].map(tier_map)
ms_dmt_procs["discont_date"] = pd.NaT
ms_dmt_procs["discont_reason"] = None
ms_dmt_procs["source_table"] = "Procedures"
ms_dmt_procs.to_pickle(FINAL_MS_DMT_PROCS_PATH)

print(f"\nTotal Procedures-sourced DMT rows: {len(ms_dmt_procs):,}")
print(f"Distinct patients on a DMT (via Procedures): {ms_dmt_procs['ndid'].nunique():,}")
print(ms_dmt_procs.groupby("drug_generic_name")["ndid"].nunique().sort_values(ascending=False))
ms_dmt_procs.head()

# ===== cell 9c =====
# ── Combine Medications + Procedures into one unified ms_dmt table (NEW) ────────────
# pd.concat aligns by column name and fills anything missing on one side with NaN
# (e.g. med_name/med_code only exist on the Medications side, proc_code/proc_name only
# on the Procedures side) -- every downstream cell below (10 onward, verbatim from
# ms_dmt_analysis_run.py) only ever reads ndid / drug_generic_name / drug_class /
# efficacy_tier / med_start_date / med_end_date / discont_date / discont_reason, all of
# which are populated on both sides, so nothing downstream needs to change.
ms_dmt = pd.concat([ms_dmt_meds, ms_dmt_procs], ignore_index=True, sort=False)
ms_dmt["route"] = ms_dmt["drug_generic_name"].map(route_map)

print(f"\nCombined ms_dmt (Medications + Procedures): {len(ms_dmt):,} rows")
print(f"Distinct DMT-treated patients (combined): {ms_dmt['ndid'].nunique():,}")
print(ms_dmt["source_table"].value_counts())

# ── dmt_by_source: per-drug breakdown of where each patient's record(s) came from,
# and -- the single most important number for this change -- how many patients would
# have been MISSED entirely by a medications-only pipeline (found via a Procedures
# record for that drug with NO corresponding Medications record for the same drug) ──
_meds_pairs = set(zip(ms_dmt_meds["ndid"], ms_dmt_meds["drug_generic_name"]))
_procs_pairs = set(zip(ms_dmt_procs["ndid"], ms_dmt_procs["drug_generic_name"]))
_only_procs_pairs = _procs_pairs - _meds_pairs

_only_procs_df = pd.DataFrame(list(_only_procs_pairs), columns=["ndid", "drug_generic_name"])
_only_procs_counts = _only_procs_df.groupby("drug_generic_name")["ndid"].nunique()

_drugs_with_any_proc_capture = sorted({g for _c, g in HCPCS_DMT_REFERENCE})
dmt_by_source = pd.DataFrame(index=_drugs_with_any_proc_capture)
dmt_by_source.index.name = "drug_generic_name"
dmt_by_source["N_patients_via_Medications"] = (
    ms_dmt_meds[ms_dmt_meds["drug_generic_name"].isin(_drugs_with_any_proc_capture)]
    .groupby("drug_generic_name")["ndid"].nunique()
    .reindex(_drugs_with_any_proc_capture, fill_value=0)
)
dmt_by_source["N_patients_via_Procedures"] = (
    ms_dmt_procs.groupby("drug_generic_name")["ndid"].nunique()
    .reindex(_drugs_with_any_proc_capture, fill_value=0)
)
dmt_by_source["N_patients_combined_total"] = (
    ms_dmt[ms_dmt["drug_generic_name"].isin(_drugs_with_any_proc_capture)]
    .groupby("drug_generic_name")["ndid"].nunique()
    .reindex(_drugs_with_any_proc_capture, fill_value=0)
)
dmt_by_source["N_patients_ONLY_found_via_Procedures"] = (
    _only_procs_counts.reindex(_drugs_with_any_proc_capture, fill_value=0)
)
dmt_by_source["Pct_gained_vs_Medications_only"] = (
    100 * dmt_by_source["N_patients_ONLY_found_via_Procedures"]
    / dmt_by_source["N_patients_via_Medications"].replace(0, pd.NA)
).round(1)
dmt_by_source = dmt_by_source.sort_values("N_patients_combined_total", ascending=False)

print("\ndmt_by_source (incremental value of adding the Procedures table):")
print(dmt_by_source)

# ── hcpcs_reference sheet: documents the crosswalk used in cell 9b ─────────────────
hcpcs_reference = pd.DataFrame(
    [
        (code, generic, class_map[generic], tier_map[generic], route_map[generic])
        for code, generic in HCPCS_DMT_REFERENCE
    ],
    columns=["hcpcs_code", "drug_generic_name", "drug_class", "efficacy_tier", "route"],
)
hcpcs_reference

# ===== cell 10 =====
# ── Overall DMT utilization: % of the MS cohort ever on >=1 DMT (Medications OR
# Procedures) -- unchanged logic from ms_dmt_analysis_run.py, now on the combined table
total_cohort_n = len(ms_cohort_demo)
dmt_treated_ndids = set(ms_dmt["ndid"].unique())
n_dmt_treated = len(dmt_treated_ndids)

dmt_overall_utilization = pd.DataFrame([
    ("Total MS patients", total_cohort_n, None),
    ("Patients receiving >=1 DMT", n_dmt_treated, round(100 * n_dmt_treated / total_cohort_n, 1)),
], columns=["Characteristic", "N", "%"])

dmt_overall_utilization

# ===== cell 11 =====
class_patients = ms_dmt.groupby("drug_class")["ndid"].nunique()

dmt_utilization_by_class = pd.DataFrame({
    "N_patients": class_patients,
    "% of total cohort": (100 * class_patients / total_cohort_n).round(1),
}).sort_values("N_patients", ascending=False)

dmt_utilization_by_class

# ===== cell 12 =====
drug_patients = ms_dmt.groupby("drug_generic_name")["ndid"].nunique()

dmt_utilization_by_drug = pd.DataFrame({
    "N_patients": drug_patients,
    "% of DMT-treated patients": (100 * drug_patients / n_dmt_treated).round(1),
}).sort_values("N_patients", ascending=False)
dmt_utilization_by_drug.insert(0, "Rank", range(1, len(dmt_utilization_by_drug) + 1))

dmt_utilization_by_drug

# ===== cell 13 =====
all_classes = sorted({row[1] for row in DMT_REFERENCE})
subtype_defs = [("RRMS", "rrms_flag"), ("PPMS", "ppms_flag"),
                 ("SPMS", "spms_flag"), ("Unspecified", "ms_unspecified_flag")]

dmt_utilization_by_subtype = pd.DataFrame(index=all_classes)
dmt_utilization_by_subtype.index.name = "drug_class"

for label, flag_col in subtype_defs:
    subtype_ndids = set(ms_cohort_demo.loc[ms_cohort_demo[flag_col] == 1, "ndid"])
    subtype_n = len(subtype_ndids)
    subtype_dmt = ms_dmt[ms_dmt["ndid"].isin(subtype_ndids)]
    class_counts = subtype_dmt.groupby("drug_class")["ndid"].nunique()

    dmt_utilization_by_subtype[f"{label} (N)"] = [int(class_counts.get(c, 0)) for c in all_classes]
    dmt_utilization_by_subtype[f"{label} (%)"] = [
        round(100 * class_counts.get(c, 0) / subtype_n, 1) if subtype_n else None
        for c in all_classes
    ]

dmt_utilization_by_subtype

# ===== cell 14 =====
def dmt_by_category(cat_series, cat_order):
    cat_clean = cat_series.fillna("Unknown")
    result = pd.DataFrame(index=all_classes)
    result.index.name = "drug_class"
    for cat in cat_order:
        cat_ndids = set(ms_cohort_demo.loc[cat_clean == cat, "ndid"])
        cat_n = len(cat_ndids)
        cat_dmt = ms_dmt[ms_dmt["ndid"].isin(cat_ndids)]
        class_counts = cat_dmt.groupby("drug_class")["ndid"].nunique()
        result[f"{cat} (N)"] = [int(class_counts.get(c, 0)) for c in all_classes]
        result[f"{cat} (%)"] = [
            round(100 * class_counts.get(c, 0) / cat_n, 1) if cat_n else None
            for c in all_classes
        ]
    return result

age_group_order = ["<20", "20-29", "30-39", "40-49", "50-59", "60-69", "70+", "Unknown"]
dmt_utilization_by_age_group = dmt_by_category(ms_cohort_demo["age_group"], age_group_order)
dmt_utilization_by_age_group

# ===== cell 15 =====
onset_order = ["Pediatric-onset", "Adult-onset", "Late-onset", "Very-late-onset", "Unknown"]
dmt_utilization_by_onset = dmt_by_category(ms_cohort_demo["onset_category"], onset_order)
dmt_utilization_by_onset

# ===== cell 16 =====
STUDY_END_YEAR = int(max(
    ms_cohort_demo["first_diagnosis_year"].max(),
    ms_dmt["med_start_date"].dt.year.max(),
    ms_dmt["med_end_date"].dt.year.max() if ms_dmt["med_end_date"].notna().any() else 0,
))

_dmt_years = ms_dmt.copy()
_dmt_years["start_year"] = _dmt_years["med_start_date"].dt.year
_dmt_years["end_year"] = _dmt_years["med_end_date"].dt.year
_dmt_years["end_year"] = _dmt_years["end_year"].fillna(STUDY_END_YEAR).clip(upper=STUDY_END_YEAR).astype(int)
_dmt_years = _dmt_years[
    _dmt_years["start_year"].notna() & (_dmt_years["start_year"] <= _dmt_years["end_year"])
]
_dmt_years["start_year"] = _dmt_years["start_year"].astype(int)

_dmt_years["active_years"] = [
    list(range(s, e + 1)) for s, e in zip(_dmt_years["start_year"], _dmt_years["end_year"])
]
dmt_year_expanded = (
    _dmt_years[["ndid", "drug_generic_name", "drug_class", "efficacy_tier", "active_years"]]
    .explode("active_years")
    .rename(columns={"active_years": "year"})
    .drop_duplicates()
)

print(f"Expanded to {len(dmt_year_expanded):,} (patient, drug, year) rows, "
      f"covering years {dmt_year_expanded['year'].min()}-{dmt_year_expanded['year'].max()}")

_first_dx_year = ms_cohort_demo["first_diagnosis_year"]
year_range = range(int(_first_dx_year.min()), STUDY_END_YEAR + 1)
cumulative_diagnosed_by_year = pd.Series(
    {y: int((_first_dx_year <= y).sum()) for y in year_range}
)
cumulative_diagnosed_by_year.index.name = "year"
cumulative_diagnosed_by_year.name = "cumulative_diagnosed"
cumulative_diagnosed_by_year

# ===== cell 17 =====
dmt_by_year_absolute_class = (
    dmt_year_expanded.groupby(["year", "drug_class"])["ndid"].nunique().unstack(fill_value=0)
)
dmt_by_year_absolute_class["Any DMT"] = (
    dmt_year_expanded.groupby("year")["ndid"].nunique()
)
dmt_by_year_absolute_class = dmt_by_year_absolute_class.reindex(year_range, fill_value=0)

dmt_by_year_absolute_drug = (
    dmt_year_expanded.groupby(["year", "drug_generic_name"])["ndid"].nunique().unstack(fill_value=0)
)
dmt_by_year_absolute_drug = dmt_by_year_absolute_drug.reindex(year_range, fill_value=0)

dmt_by_year_absolute_class

# ===== cell 18 =====
dmt_by_year_absolute_drug

# ===== cell 19 =====
dmt_by_year_coverage_class = dmt_by_year_absolute_class.div(cumulative_diagnosed_by_year, axis=0).mul(100).round(1)
dmt_by_year_coverage_drug = dmt_by_year_absolute_drug.div(cumulative_diagnosed_by_year, axis=0).mul(100).round(1)

dmt_by_year_coverage_class

# ===== cell 20 =====
dmt_by_year_coverage_drug

# ===== cell 21 =====
dmt_by_year_absolute_efficacy = (
    dmt_year_expanded.groupby(["year", "efficacy_tier"])["ndid"].nunique().unstack(fill_value=0)
)
dmt_by_year_absolute_efficacy = dmt_by_year_absolute_efficacy.reindex(year_range, fill_value=0)
dmt_by_year_absolute_efficacy

# ===== cell 22 =====
dmt_by_year_coverage_efficacy = (
    dmt_by_year_absolute_efficacy.div(cumulative_diagnosed_by_year, axis=0).mul(100).round(1)
)
dmt_by_year_coverage_efficacy

# ===== cell 23 =====
first_dmt_per_patient = ms_dmt.groupby("ndid")["med_start_date"].min().rename("first_dmt_date")

time_to_first_dmt = (
    ms_cohort_demo[["ndid", "first_diagnosis_date"]]
    .merge(first_dmt_per_patient, on="ndid", how="inner")
)
time_to_first_dmt["first_diagnosis_date"] = pd.to_datetime(time_to_first_dmt["first_diagnosis_date"], errors="coerce")
time_to_first_dmt["days_to_first_dmt"] = (
    time_to_first_dmt["first_dmt_date"] - time_to_first_dmt["first_diagnosis_date"]
).dt.days

n_treated = len(time_to_first_dmt)
days = time_to_first_dmt["days_to_first_dmt"]
q1, q3 = days.quantile(0.25), days.quantile(0.75)

def _within(threshold):
    cnt = int((days <= threshold).sum())
    return cnt, round(100 * cnt / n_treated, 1)

n30, pct30 = _within(30)
n90, pct90 = _within(90)
n365, pct365 = _within(365)

time_to_dmt_summary = pd.DataFrame([
    ("N (DMT-treated patients)", n_treated, None, None),
    ("Median (days)", None, None, round(days.median(), 1)),
    ("Q1 (days)", None, None, round(q1, 1)),
    ("Q3 (days)", None, None, round(q3, 1)),
    ("IQR (days)", None, None, round(q3 - q1, 1)),
    ("Mean (days)", None, None, round(days.mean(), 1)),
    ("Treated within 30 days", n30, pct30, None),
    ("Treated within 90 days", n90, pct90, None),
    ("Treated within 1 year (365 days)", n365, pct365, None),
], columns=["Metric", "N", "%", "Value"])

time_to_dmt_summary

# ===== cell 24 =====
_ttd = time_to_first_dmt.merge(
    ms_cohort_demo[["ndid", "rrms_flag", "ppms_flag", "spms_flag", "ms_unspecified_flag",
                     "age_group", "onset_category"]],
    on="ndid", how="left",
)

def _ttd_stats(days):
    n = len(days)
    if n == 0:
        return {"N": 0, "Median": None, "Q1": None, "Q3": None, "IQR": None, "Mean": None,
                "% within 30d": None, "% within 90d": None, "% within 365d": None}
    q1, q3 = days.quantile(0.25), days.quantile(0.75)
    return {
        "N": n,
        "Median": round(days.median(), 1),
        "Q1": round(q1, 1),
        "Q3": round(q3, 1),
        "IQR": round(q3 - q1, 1),
        "Mean": round(days.mean(), 1),
        "% within 30d": round(100 * (days <= 30).mean(), 1),
        "% within 90d": round(100 * (days <= 90).mean(), 1),
        "% within 365d": round(100 * (days <= 365).mean(), 1),
    }

_subtype_defs = [("RRMS", "rrms_flag"), ("PPMS", "ppms_flag"),
                  ("SPMS", "spms_flag"), ("Unspecified", "ms_unspecified_flag")]
time_to_dmt_by_subtype = pd.DataFrame(
    [{"Group": label, **_ttd_stats(_ttd.loc[_ttd[flag_col] == 1, "days_to_first_dmt"])}
     for label, flag_col in _subtype_defs]
).set_index("Group")

_age_group_order = ["<20", "20-29", "30-39", "40-49", "50-59", "60-69", "70+", "Unknown"]
_age_clean = _ttd["age_group"].fillna("Unknown")
time_to_dmt_by_age_group = pd.DataFrame(
    [{"Group": g, **_ttd_stats(_ttd.loc[_age_clean == g, "days_to_first_dmt"])}
     for g in _age_group_order if (_age_clean == g).any()]
).set_index("Group")

_onset_order = ["Pediatric-onset", "Adult-onset", "Late-onset", "Very-late-onset", "Unknown"]
_onset_clean = _ttd["onset_category"].fillna("Unknown")
time_to_dmt_by_onset = pd.DataFrame(
    [{"Group": g, **_ttd_stats(_ttd.loc[_onset_clean == g, "days_to_first_dmt"])}
     for g in _onset_order if (_onset_clean == g).any()]
).set_index("Group")

time_to_dmt_by_subtype

# ===== cell 25 =====
n_distinct_dmts = (
    ms_dmt.groupby("ndid")["drug_generic_name"].nunique()
    .reindex(ms_cohort_demo["ndid"], fill_value=0)
)
total_cohort_n = len(ms_cohort_demo)

def _bucket(n):
    if n == 0: return "0"
    if n == 1: return "1"
    if n == 2: return "2"
    if n == 3: return "3"
    return "4+"

dmt_count_bucket = n_distinct_dmts.apply(_bucket)
bucket_counts = dmt_count_bucket.value_counts()

dmt_count_categories = pd.DataFrame(
    [(b, int(bucket_counts.get(b, 0)), round(100 * bucket_counts.get(b, 0) / total_cohort_n, 1))
     for b in ["0", "1", "2", "3", "4+"]],
    columns=["N_distinct_DMTs", "N", "%"],
)
dmt_count_categories

# ===== cell 26 =====
treated_counts = n_distinct_dmts[n_distinct_dmts >= 1]
n_treated = len(treated_counts)
n_single = int((treated_counts == 1).sum())
n_multiple = int((treated_counts > 1).sum())

dmt_single_vs_multiple = pd.DataFrame([
    ("N (DMT-treated patients)", n_treated, None),
    ("Single DMT (exactly 1 distinct drug ever)", n_single, round(100 * n_single / n_treated, 1)),
    ("Multiple DMTs (>1 distinct drug ever)", n_multiple, round(100 * n_multiple / n_treated, 1)),
], columns=["Characteristic", "N", "%"])

dmt_single_vs_multiple

# ===== cell 27 =====
patient_drug_first_start = (
    ms_dmt.groupby(["ndid", "drug_generic_name"])["med_start_date"].min()
    .reset_index()
    .sort_values(["ndid", "med_start_date"])
)

_seq = patient_drug_first_start.groupby("ndid").agg(
    drug_list=("drug_generic_name", list),
    date_list=("med_start_date", list),
)
_seq["n_drugs"] = _seq["drug_list"].str.len()

multi_dmt_seq = _seq[_seq["n_drugs"] > 1].copy()
multi_dmt_seq["drug_sequence"] = multi_dmt_seq["drug_list"].str.join(" → ")

dmt_sequences = multi_dmt_seq[["drug_sequence", "n_drugs"]].reset_index()

print(f"Multi-DMT patients (sequences): {len(dmt_sequences):,}")
dmt_sequences.head(10)

# ===== cell 28 =====
transition_rows = []
for ndid, row in multi_dmt_seq.iterrows():
    drugs, dates = row["drug_list"], row["date_list"]
    for step, (from_drug, to_drug, from_date, to_date) in enumerate(
        zip(drugs[:-1], drugs[1:], dates[:-1], dates[1:]), start=1
    ):
        transition_rows.append((ndid, step, from_drug, to_drug, from_date, to_date))

dmt_transitions_long = pd.DataFrame(
    transition_rows, columns=["ndid", "step", "from_drug", "to_drug", "from_date", "to_date"]
)

print(f"Total individual transitions: {len(dmt_transitions_long):,}")
dmt_transitions_long.head(10)

# ===== cell 29 =====
n_multi_dmt_patients = len(dmt_sequences)
total_transitions = len(dmt_transitions_long)

dmt_transitions_long["transition_pair"] = (
    dmt_transitions_long["from_drug"] + " → " + dmt_transitions_long["to_drug"]
)
pair_counts = dmt_transitions_long["transition_pair"].value_counts()

dmt_transition_ranking = pd.DataFrame({
    "N_transitions": pair_counts,
    "% of total transitions": (100 * pair_counts / total_transitions).round(1),
    "% of multi-DMT patients": (100 * pair_counts / n_multi_dmt_patients).round(1),
}).reset_index(names="transition_pair")
dmt_transition_ranking.insert(0, "Rank", range(1, len(dmt_transition_ranking) + 1))

dmt_transition_ranking

# ===== cell 30 =====
dmt_courses = ms_dmt.groupby(["ndid", "drug_generic_name", "drug_class", "efficacy_tier"]).agg(
    course_start=("med_start_date", "min"),
    course_end=("med_end_date", "max"),
    discont_date=("discont_date", "max"),
    discont_reason=("discont_reason", lambda s: s.dropna().iloc[0] if s.notna().any() else None),
).reset_index()

dmt_courses["discontinued"] = dmt_courses["discont_date"].notna()
dmt_courses["persistence_days"] = (
    dmt_courses["discont_date"] - dmt_courses["course_start"]
).dt.days

n_courses = len(dmt_courses)
n_discontinued = int(dmt_courses["discontinued"].sum())
persistence = dmt_courses.loc[dmt_courses["discontinued"], "persistence_days"]
p_q1, p_q3 = persistence.quantile(0.25), persistence.quantile(0.75)

discontinuation_summary = pd.DataFrame([
    ("N (patient, drug) courses", n_courses, None, None),
    ("Discontinued (discont_date present)", n_discontinued,
     round(100 * n_discontinued / n_courses, 1), None),
    ("Median persistence (days)", None, None, round(persistence.median(), 1)),
    ("Q1 persistence (days)", None, None, round(p_q1, 1)),
    ("Q3 persistence (days)", None, None, round(p_q3, 1)),
    ("IQR persistence (days)", None, None, round(p_q3 - p_q1, 1)),
    ("Mean persistence (days)", None, None, round(persistence.mean(), 1)),
], columns=["Metric", "N", "%", "Value"])

discontinuation_summary

# ===== cell 31 =====
_discont_courses = dmt_courses[dmt_courses["discontinued"]]
reason_counts = _discont_courses["discont_reason"].fillna("Unknown / not recorded").value_counts()

discontinuation_reasons = pd.DataFrame({
    "N": reason_counts,
    "% of discontinued courses": (100 * reason_counts / len(_discont_courses)).round(1),
}).reset_index(names="discont_reason")
discontinuation_reasons.insert(0, "Rank", range(1, len(discontinuation_reasons) + 1))

discontinuation_reasons

# ===== cell 32 =====
first_dmt_record = ms_dmt.sort_values("med_start_date").groupby("ndid").first()
n_treated_patients = len(first_dmt_record)

first_class_counts = first_dmt_record["drug_class"].value_counts()

first_dmt_class_distribution = pd.DataFrame({
    "First DMT class": first_class_counts.index,
    "N": first_class_counts.values,
    "% of treated patients": (100 * first_class_counts / n_treated_patients).round(1).values,
}).sort_values("N", ascending=False).reset_index(drop=True)

first_dmt_class_distribution

# ===== cell 33 =====
_first_dmt_with_subtype = first_dmt_record.reset_index().merge(
    ms_cohort_demo[["ndid", "rrms_flag", "ppms_flag", "spms_flag", "ms_unspecified_flag"]],
    on="ndid", how="left",
)

_subtype_defs_fdc = [("RRMS", "rrms_flag"), ("PPMS", "ppms_flag"),
                      ("SPMS", "spms_flag"), ("Unspecified", "ms_unspecified_flag")]

first_dmt_class_by_subtype = pd.DataFrame(index=all_classes)
first_dmt_class_by_subtype.index.name = "drug_class"
for label, flag_col in _subtype_defs_fdc:
    sub = _first_dmt_with_subtype[_first_dmt_with_subtype[flag_col] == 1]
    n_sub = len(sub)
    counts = sub["drug_class"].value_counts()
    first_dmt_class_by_subtype[f"{label} (N)"] = [int(counts.get(c, 0)) for c in all_classes]
    first_dmt_class_by_subtype[f"{label} (%)"] = [
        round(100 * counts.get(c, 0) / n_sub, 1) if n_sub else None for c in all_classes
    ]

first_dmt_class_by_subtype

# ===== cell 33b =====
# ── dmt_route_utilization (NEW, small addition): now that Procedures-sourced infusion
# records are in the mix, this checks whether the route split actually moves ──────────
all_routes = sorted({row[3] for row in DMT_REFERENCE})
route_patients = ms_dmt.groupby("route")["ndid"].nunique()
route_patients_meds_only = ms_dmt_meds.assign(
    route=ms_dmt_meds["drug_generic_name"].map(route_map)
).groupby("route")["ndid"].nunique()

dmt_route_utilization = pd.DataFrame({
    "N_patients_combined": route_patients,
    "% of total cohort (combined)": (100 * route_patients / total_cohort_n).round(1),
    "N_patients_Medications_only": route_patients_meds_only.reindex(route_patients.index, fill_value=0),
    "% of total cohort (Medications only)": (
        100 * route_patients_meds_only.reindex(route_patients.index, fill_value=0) / total_cohort_n
    ).round(1),
}).reindex(all_routes).sort_values("N_patients_combined", ascending=False)

dmt_route_utilization

# ===== cell 34 =====
import numpy as np

icd_reference = pd.DataFrame(
    [
        ("G35",   "ICD-10", "Multiple sclerosis", "MS_unspecified"),
        ("340",   "ICD-9",  "Multiple sclerosis", "MS_unspecified"),
        ("G35A",  "ICD-10", "Relapsing-remitting multiple sclerosis", "RRMS"),
        ("G35B",  "ICD-10", "Primary progressive multiple sclerosis", "PPMS"),
        ("G35B0", "ICD-10", "Primary progressive multiple sclerosis, unspecified", "PPMS"),
        ("G35B1", "ICD-10", "Active primary progressive multiple sclerosis", "PPMS"),
        ("G35B2", "ICD-10", "Non-active primary progressive multiple sclerosis", "PPMS"),
        ("G35C",  "ICD-10", "Secondary progressive multiple sclerosis", "SPMS"),
        ("G35C0", "ICD-10", "Secondary progressive multiple sclerosis, unspecified", "SPMS"),
        ("G35C1", "ICD-10", "Active secondary progressive multiple sclerosis", "SPMS"),
        ("G35C2", "ICD-10", "Non-active secondary progressive multiple sclerosis", "SPMS"),
        ("G35D",  "ICD-10", "Multiple sclerosis, unspecified", "MS_unspecified"),
    ],
    columns=["icd_code", "coding_system", "description", "ms_subtype"],
)

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

icd_reference

# ===== cell 35 =====
def _pct_rows(series, decimals=1):
    s = series.fillna("Unknown")
    counts = s.value_counts()
    total = len(series)
    return [(str(label), int(counts[label]), round(100 * counts[label] / total, decimals))
            for label in counts.index]

n = len(ms_cohort_demo)
age = ms_cohort_demo["age_at_diagnosis"]
q1, q3 = age.quantile(0.25), age.quantile(0.75)
iqr = q3 - q1
outlier_lo, outlier_hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
n_outliers = int(((age < outlier_lo) | (age > outlier_hi)).sum())

rows = [
    ("Study date window", None, None, f"{DATE_START} to {DATE_END} (applied to diagnosis, medication, and procedure dates)"),
    ("Total MS patients", n, None, None),
    ("— Age at diagnosis —", None, None, None),
    ("N (non-missing)", int(age.notna().sum()), None, None),
    ("Mean", None, None, round(age.mean(), 1)),
    ("SD", None, None, round(age.std(), 1)),
    ("Min", None, None, round(age.min(), 0)),
    ("Q1 (25th pct)", None, None, round(q1, 0)),
    ("Median (Q2)", None, None, round(age.median(), 0)),
    ("Q3 (75th pct)", None, None, round(q3, 0)),
    ("Max", None, None, round(age.max(), 0)),
    ("IQR (Q3-Q1)", None, None, round(iqr, 0)),
    (f"Outliers (Tukey rule: <{outlier_lo:.0f} or >{outlier_hi:.0f})",
     n_outliers, round(100 * n_outliers / age.notna().sum(), 1), None),
]

rows.append(("— Sex —", None, None, None))
rows += [(label, cnt, pct, None) for label, cnt, pct in _pct_rows(ms_cohort_demo["gender_bucket"])
         if label in ("Female", "Male", "Unknown")]

rows.append(("— MS subtype (ever recorded; not mutually exclusive) —", None, None, None))
for label, flag_col in [("RRMS", "rrms_flag"), ("PPMS", "ppms_flag"),
                         ("SPMS", "spms_flag"), ("Unspecified", "ms_unspecified_flag")]:
    cnt = int(ms_cohort_demo[flag_col].sum())
    rows.append((label, cnt, round(100 * cnt / n, 1), None))

n_subtypes_ever = ms_cohort_demo["ms_subtypes_ever"].str.split(",").str.len()
n_multi = int((n_subtypes_ever > 1).sum())
rows.append((
    "Patients with >1 subtype ever recorded (progression indicator)",
    n_multi, round(100 * n_multi / n, 1), None,
))

rows.append(("— Age group —", None, None, None))
age_group_order = ["<20", "20-29", "30-39", "40-49", "50-59", "60-69", "70+", "Unknown"]
age_group_val = {label: (cnt, pct) for label, cnt, pct in _pct_rows(ms_cohort_demo["age_group"])}
rows += [(g, *age_group_val[g], None) for g in age_group_order if g in age_group_val]

rows.append(("— Onset category —", None, None, None))
onset_order = ["Pediatric-onset", "Adult-onset", "Late-onset", "Very-late-onset", "Unknown"]
onset_val = {label: (cnt, pct) for label, cnt, pct in _pct_rows(ms_cohort_demo["onset_category"])}
rows += [(o, *onset_val[o], None) for o in onset_order if o in onset_val]

rows.append(("— Race —", None, None, None))
rows += [(label, cnt, pct, None) for label, cnt, pct in _pct_rows(ms_cohort_demo["race"])]

rows.append(("— Ethnicity —", None, None, None))
rows += [(label, cnt, pct, None) for label, cnt, pct in _pct_rows(ms_cohort_demo["ethnicity"])]

rows.append(("— Marital status —", None, None, None))
rows += [(label, cnt, pct, None) for label, cnt, pct in _pct_rows(ms_cohort_demo["marital_status"])]

state_for_chars = (
    ms_cohort_demo["state"]
    .replace("<<STATE>>", "Masked (small-cell suppression)")
    .fillna("Unknown")
)
state_counts = state_for_chars.value_counts()
rows.append(("— Geography: state (top 10 shown) —", None, None, None))
top_states = state_counts.head(10)
other_states_n = int(state_counts.iloc[10:].sum())
rows += [(state, int(cnt), round(100 * cnt / n, 1), None) for state, cnt in top_states.items()]
if other_states_n > 0:
    rows.append(("Other states", other_states_n, round(100 * other_states_n / n, 1), None))

zip_counts = ms_cohort_demo["zip"].fillna("Unknown").value_counts()
rows.append(("— Geography: zip (top 10 shown) —", None, None, None))
top_zips = zip_counts.head(10)
other_zips_n = int(zip_counts.iloc[10:].sum())
rows += [(str(z), int(cnt), round(100 * cnt / n, 1), None) for z, cnt in top_zips.items()]
if other_zips_n > 0:
    rows.append(("Other zips", other_zips_n, round(100 * other_zips_n / n, 1), None))

rows.append(("— Geography: country —", None, None, None))
rows += [(label, cnt, pct, None) for label, cnt, pct in _pct_rows(ms_cohort_demo["country"])]

rows.append(("— Diagnosis coding system (event-level) —", None, None, None))
rows += [(label, cnt, pct, None) for label, cnt, pct in _pct_rows(ms_diagnoses["coding_system"])]

rows.append((
    "Diagnosis year range", None, None,
    f"{int(ms_cohort_demo['first_diagnosis_year'].min())}-{int(ms_cohort_demo['first_diagnosis_year'].max())}",
))

cohort_characteristics = pd.DataFrame(rows, columns=["Characteristic", "N", "%", "Value"])
cohort_characteristics

# ===== cell 36 =====
subtype_flag_cols = {"RRMS": "rrms_flag", "PPMS": "ppms_flag", "SPMS": "spms_flag"}

subtype_characteristics = pd.DataFrame(
    index=["N", "Mean age at diagnosis", "Median age", "Female (N)", "Female (%)", "Male (N)", "Male (%)"],
    columns=list(subtype_flag_cols),
)

for subtype, flag_col in subtype_flag_cols.items():
    subset = ms_cohort_demo[ms_cohort_demo[flag_col] == 1]
    n_sub = len(subset)
    n_female = int((subset["gender_bucket"] == "Female").sum())
    n_male = int((subset["gender_bucket"] == "Male").sum())
    subtype_characteristics.loc["N", subtype] = n_sub
    subtype_characteristics.loc["Mean age at diagnosis", subtype] = round(subset["age_at_diagnosis"].mean(), 1)
    subtype_characteristics.loc["Median age", subtype] = round(subset["age_at_diagnosis"].median(), 0)
    subtype_characteristics.loc["Female (N)", subtype] = n_female
    subtype_characteristics.loc["Female (%)", subtype] = round(100 * n_female / n_sub, 1)
    subtype_characteristics.loc["Male (N)", subtype] = n_male
    subtype_characteristics.loc["Male (%)", subtype] = round(100 * n_male / n_sub, 1)

subtype_characteristics

# ===== cell 37 =====
state_clean = (
    ms_cohort_demo["state"]
    .replace("<<STATE>>", "Masked (small-cell suppression)")
    .fillna("Unknown")
)
by_state = ms_cohort_demo.groupby(state_clean)
state_n = by_state.size()

sex_counts = by_state["gender_bucket"].value_counts().unstack(fill_value=0)
sex_by_region = pd.DataFrame({"N": state_n})
for col in sex_counts.columns:
    sex_by_region[f"{col} (N)"] = sex_counts[col].astype(int)
    sex_by_region[f"{col} (%)"] = (100 * sex_counts[col] / state_n).round(1)
sex_by_region = sex_by_region.sort_values("N", ascending=False)

subtype_by_region = pd.DataFrame({"N": state_n})
for label, flag_col in [("RRMS", "rrms_flag"), ("PPMS", "ppms_flag"),
                         ("SPMS", "spms_flag"), ("Unspecified", "ms_unspecified_flag")]:
    flag_sum = by_state[flag_col].sum()
    subtype_by_region[f"{label} (N)"] = flag_sum.astype(int)
    subtype_by_region[f"{label} (%)"] = (100 * flag_sum / state_n).round(1)
subtype_by_region = subtype_by_region.sort_values("N", ascending=False)

age_by_region = pd.DataFrame({
    "N": state_n,
    "Mean age at diagnosis": by_state["age_at_diagnosis"].mean().round(1),
    "Median age at diagnosis": by_state["age_at_diagnosis"].median(),
}).sort_values("N", ascending=False)

sex_by_region.head(), subtype_by_region.head(), age_by_region.head()

# ===== cell 38 =====
dq_counts = dob_quality["dob_quality_flag"].value_counts()
dq_order = ["plausible", "missing_dob", "negative_age", "implausible_over_100"]
dq_labels = {
    "plausible": "Age 0-100 at diagnosis",
    "missing_dob": "Missing birth year -> age_at_diagnosis is Unknown (not excluded from cohort)",
    "negative_age": "Negative age, dob after first_diagnosis_year -> age_at_diagnosis is Unknown (not excluded)",
    "implausible_over_100": "Age >100 at diagnosis (retained as a real age, not capped or excluded)",
}

data_quality = pd.DataFrame(
    [
        (dq_labels[k], int(dq_counts.get(k, 0)), f"{100 * dq_counts.get(k, 0) / len(dob_quality):.2f}%")
        for k in dq_order
    ],
    columns=["Issue", "N", "% of cohort"],
)
data_quality

# ===== cell 39 =====
_by_year = ms_cohort_demo.groupby("first_diagnosis_year")
_year_n = _by_year.size()

diagnoses_by_year = pd.DataFrame({
    "N_new_patients": _year_n,
    "Mean_age_at_diagnosis": _by_year["age_at_diagnosis"].mean().round(1),
    "Median_age_at_diagnosis": _by_year["age_at_diagnosis"].median(),
})
for label, flag_col in [("RRMS", "rrms_flag"), ("PPMS", "ppms_flag"),
                         ("SPMS", "spms_flag"), ("Unspecified", "ms_unspecified_flag")]:
    flag_sum = _by_year[flag_col].sum()
    diagnoses_by_year[f"{label} (N)"] = flag_sum.astype(int)
    diagnoses_by_year[f"{label} (%)"] = (100 * flag_sum / _year_n).round(1)

diagnoses_by_year = diagnoses_by_year.sort_index()
diagnoses_by_year

# ===== cell 41 =====
subtype_seq = ms_cohort_demo["ms_subtypes_ever"].str.split(",")
subtype_dates_seq = ms_cohort_demo["ms_subtype_dates"].str.split(",")

multi_mask = subtype_seq.str.len() > 1

subtype_transitions = ms_cohort_demo.loc[multi_mask, ["ndid"]].copy()
subtype_transitions["n_subtypes"] = subtype_seq[multi_mask].str.len().values
subtype_transitions["subtype_sequence"] = ms_cohort_demo.loc[multi_mask, "ms_subtypes_ever"].values
subtype_transitions["subtype_dates"] = ms_cohort_demo.loc[multi_mask, "ms_subtype_dates"].values
subtype_transitions["first_subtype"] = subtype_seq[multi_mask].str[0].values
subtype_transitions["last_subtype"] = subtype_seq[multi_mask].str[-1].values
subtype_transitions["first_subtype_date"] = subtype_dates_seq[multi_mask].str[0].values
subtype_transitions["last_subtype_date"] = subtype_dates_seq[multi_mask].str[-1].values
subtype_transitions["transition_pattern"] = (
    subtype_seq[multi_mask].str.join(" → ").values
)

_first_dt = pd.to_datetime(subtype_transitions["first_subtype_date"], errors="coerce")
_last_dt = pd.to_datetime(subtype_transitions["last_subtype_date"], errors="coerce")
subtype_transitions["years_to_progression"] = (
    ((_last_dt - _first_dt).dt.days / 365.25).round(1)
)

print(f"Patients with >1 subtype ever recorded: {len(subtype_transitions):,} "
      f"({100 * len(subtype_transitions) / len(ms_cohort_demo):.1f}% of cohort)")
print(f"Median years to progression: {subtype_transitions['years_to_progression'].median():.1f}")
subtype_transitions.head(10)

# ===== cell 42 =====
transition_counts = subtype_transitions["transition_pattern"].value_counts()
median_years = subtype_transitions.groupby("transition_pattern")["years_to_progression"].median()

subtype_transition_patterns = pd.DataFrame({
    "N_patients": transition_counts,
    "% of multi-subtype patients": (100 * transition_counts / len(subtype_transitions)).round(1),
    "% of full cohort": (100 * transition_counts / len(ms_cohort_demo)).round(2),
    "Median years to progression": median_years,
}).reset_index(names="transition_pattern")

subtype_transition_patterns

# ===== cell 44 =====
SHEET_MARK = "— sheet overview —"

readme_rows = [
    ("icd_reference", SHEET_MARK, "The ICD-9/ICD-10 reference codes used to define the MS cohort, verbatim"),
    ("icd_reference", "icd_code", "ICD code"),
    ("icd_reference", "description", "Text description of that code"),
    ("icd_reference", "coding_system", "ICD-9 or ICD-10"),

    ("hcpcs_reference", SHEET_MARK,
     "NEW in run2: the HCPCS crosswalk used to capture MS DMT infusion/injectable administrations "
     "from rgd_udm_silver.procedures (proc_code_std / proc_code exact match), in addition to the "
     "existing name-based match against rgd_udm_silver.medication. Verified against what is actually "
     "present in this DB -- not every plausible J-code resolved to an MS drug (e.g. J2327 in this "
     "database is Risankizumab/Skyrizi, not ublituximab, and is deliberately excluded)."),
    ("hcpcs_reference", "hcpcs_code", "HCPCS Level II code matched on procedures.proc_code_std / proc_code"),
    ("hcpcs_reference", "drug_generic_name", "MS DMT generic name this code was mapped to"),
    ("hcpcs_reference", "drug_class / efficacy_tier / route", "Same values as that drug's row in DMT_REFERENCE (cell 7) -- guaranteed consistent with the Medications-table classification for the same drug"),

    ("dmt_by_source", SHEET_MARK,
     "NEW in run2: per-drug breakdown of how many DMT-treated patients were found via Medications vs. "
     "Procedures vs. both, and -- the key number -- how many patients would have been MISSED entirely "
     "by a medications-only pipeline (found via a Procedures record for that drug with no matching "
     "Medications record for the same drug). Only covers the 6 drugs with any Procedures-table capture "
     "(see hcpcs_reference); every other DMT is unaffected by this change."),
    ("dmt_by_source", "N_patients_via_Medications / via_Procedures / combined_total", "Distinct patients on that drug, by source and combined (not double-counted)"),
    ("dmt_by_source", "N_patients_ONLY_found_via_Procedures", "Patients with a Procedures record for that drug but zero Medications record for it -- the pure incremental yield of this change"),
    ("dmt_by_source", "Pct_gained_vs_Medications_only", "N_patients_ONLY_found_via_Procedures / N_patients_via_Medications, as a %"),

    ("cohort_characteristics", SHEET_MARK,
     "One Characteristic/Value(N(%)) table: age distribution+outliers, sex, subtype %, "
     "age-group/onset-category, race/ethnicity/marital-status, geography, ICD-9 vs ICD-10 mix. "
     f"n=the full MS cohort, restricted to a qualifying MS diagnosis within {DATE_START}..{DATE_END} "
     "(see 'Study date window' row and the Methodology note below)"),
    ("cohort_characteristics", "Characteristic", "Row label. Section headers (— Age at diagnosis —, — Sex —, etc.) have a blank Value"),
    ("cohort_characteristics", "Value (N (%))", "Either a plain number/stat (age summary rows), or 'N (xx.x%)' for every "
                                                  "categorical breakdown row, computed against the full cohort (n=len(ms_cohort_demo)) "
                                                  "unless the row is itself a sub-breakdown"),

    ("subtype_characteristics", SHEET_MARK,
     "RRMS / PPMS / SPMS columns x (N, mean age, median age, %female, %male) rows, computed on each subtype's "
     "'ever had this flag' patient subset (subsets overlap -- not mutually exclusive). n=the full MS cohort"),
    ("subtype_characteristics", "N", "Number of patients with that subtype's flag = 1 (denominator for Female/Male % in this column)"),
    ("subtype_characteristics", "Mean age at diagnosis / Median age", "age_at_diagnosis within that subtype's subset"),
    ("subtype_characteristics", "Female / Male", "Count (%) within that subtype's subset"),

    ("sex_by_region", SHEET_MARK, "N and Female/Male/Unknown as 'N (%)', one row per state (index). n=the full MS cohort"),
    ("sex_by_region", "(index)", "state (patients.pat_state); '<<STATE>>' masking token relabeled 'Masked (small-cell suppression)', NULL -> 'Unknown'"),
    ("sex_by_region", "N", "Total patients in that state"),
    ("sex_by_region", "Female / Male / Unknown", "Count (%) of that state's N with that gender_bucket value"),

    ("subtype_by_region", SHEET_MARK, "N and RRMS/PPMS/SPMS/Unspecified as 'N (%)', one row per state (not mutually exclusive). n=the full MS cohort"),
    ("subtype_by_region", "(index)", "state, same cleaning as sex_by_region"),
    ("subtype_by_region", "N", "Total patients in that state"),
    ("subtype_by_region", "RRMS / PPMS / SPMS / Unspecified", "Count (%) of that state's N with that subtype flag = 1"),

    ("age_by_region", SHEET_MARK, "N, mean and median age at diagnosis, one row per state. n=the full MS cohort"),
    ("age_by_region", "(index)", "state, same cleaning as sex_by_region"),
    ("age_by_region", "N", "Total patients in that state"),
    ("age_by_region", "Mean age at diagnosis / Median age at diagnosis", "age_at_diagnosis, within that state"),

    ("data_quality", SHEET_MARK,
     "Birth-year data-quality breakdown -- informational QC visibility only, NOT an exclusion "
     "criterion: no patient is ever dropped from the cohort for this"),
    ("data_quality", "Issue", "plausible (age 0-100) / missing_dob (age = Unknown) / "
                              "negative_age (dob after first_diagnosis_year, age = Unknown) / "
                              "implausible_over_100 (age >100, retained as a real age)"),
    ("data_quality", "N", "Count of patients with that issue"),
    ("data_quality", "% of cohort", "N / full cohort size"),

    ("diagnoses_by_year", SHEET_MARK, "New-patient count, mean/median age, and subtype mix, one row per first_diagnosis_year (index). n=the full MS cohort"),
    ("diagnoses_by_year", "(index)", f"first_diagnosis_year: the year of each patient's earliest qualifying MS diagnosis WITHIN {DATE_START}..{DATE_END}"),
    ("diagnoses_by_year", "N_new_patients", "Count of patients whose first_diagnosis_year is that year"),
    ("diagnoses_by_year", "Mean_age_at_diagnosis / Median_age_at_diagnosis", "age_at_diagnosis, among patients first diagnosed that year"),
    ("diagnoses_by_year", "RRMS / PPMS / SPMS / Unspecified", "Count (%), among patients first diagnosed that year, who EVER (within the date window) had that subtype flag = 1"),

    ("subtype_transitions", SHEET_MARK, "Patient-level detail for everyone with >1 distinct subtype ever recorded (a subtype change). n=the full MS cohort"),
    ("subtype_transitions", "ndid", "Patient id"),
    ("subtype_transitions", "n_subtypes", "Count of distinct subtypes in ms_subtypes_ever"),
    ("subtype_transitions", "subtype_sequence", "ms_subtypes_ever verbatim, ordered by when each was first seen"),
    ("subtype_transitions", "subtype_dates", "ms_subtype_dates verbatim -- dates aligned positionally with subtype_sequence"),
    ("subtype_transitions", "first_subtype / last_subtype", "First and last entries of subtype_sequence"),
    ("subtype_transitions", "first_subtype_date / last_subtype_date", "First and last entries of subtype_dates"),
    ("subtype_transitions", "transition_pattern", "subtype_sequence joined with ' → '"),
    ("subtype_transitions", "years_to_progression", "(last_subtype_date - first_subtype_date) in years"),

    ("subtype_transition_patterns", SHEET_MARK, "Aggregate counts + median years-to-progression per distinct transition_pattern. n=the full MS cohort"),
    ("subtype_transition_patterns", "transition_pattern", "As defined in subtype_transitions"),
    ("subtype_transition_patterns", "N_patients", "Count of patients with that exact pattern"),
    ("subtype_transition_patterns", "% of multi-subtype patients", "N_patients / total patients with >1 subtype"),
    ("subtype_transition_patterns", "% of full cohort", "N_patients / the full cohort"),
    ("subtype_transition_patterns", "Median years to progression", "Median of years_to_progression among patients with that pattern"),

    ("dmt_overall_utilization", SHEET_MARK, "% of the full MS cohort ever receiving >=1 DMT, combining Medications AND Procedures records (see dmt_by_source for the source breakdown)"),
    ("dmt_utilization_by_class", SHEET_MARK, "Unique patients ever on each drug class (combined source) / the full cohort (not mutually exclusive)"),
    ("dmt_utilization_by_drug", SHEET_MARK, "Unique patients ever on each individual DMT (combined source) / DMT-treated patients, most to least common"),
    ("dmt_utilization_by_subtype", SHEET_MARK, "drug_class x MS subtype matrix (combined source), N/% within each subtype's own patient count"),
    ("dmt_utilization_by_age_grp", SHEET_MARK, "drug_class x age_group matrix (combined source), N/% within each age group's own patient count"),
    ("dmt_utilization_by_onset", SHEET_MARK, "drug_class x onset_category matrix (combined source), N/% within each onset category's own patient count"),
    ("dmt_route_utilization", SHEET_MARK,
     "NEW in run2: unique patients per route of administration (Oral/Injectable/Infusion), comparing the "
     "combined (Medications+Procedures) total against a Medications-only total, to show the direct effect "
     "of adding Procedures-table infusion/injectable capture."),
    ("dmt_by_year_absolute_class", SHEET_MARK, "Unique patients per drug class ACTIVE in each calendar year (period prevalence, combined source)"),
    ("dmt_by_year_absolute_drug", SHEET_MARK, "Same as above, per individual drug instead of class"),
    ("dmt_by_year_coverage_class", SHEET_MARK, "dmt_by_year_absolute_class / cumulative patients diagnosed by that year, as a %"),
    ("dmt_by_year_coverage_drug", SHEET_MARK, "dmt_by_year_absolute_drug / cumulative patients diagnosed by that year, as a %"),
    ("time_to_first_dmt", SHEET_MARK, "Median/Q1/Q3/IQR/Mean days from first_diagnosis_date to first DMT (combined source), and % treated within 30/90/365 days"),
    ("dmt_count_categories", SHEET_MARK, "0/1/2/3/4+ distinct DMTs ever received per patient (combined source), N/% of the full MS cohort"),
    ("dmt_single_vs_multiple", SHEET_MARK, "Among DMT-treated patients only: % receiving exactly 1 distinct DMT vs. >1 (combined source)"),
    ("dmt_sequences", SHEET_MARK, "One row per multi-DMT patient: full ordered drug_sequence (by first-exposure date, combined source) and n_drugs"),
    ("dmt_transitions_long", SHEET_MARK, "One row per individual switch step within each multi-DMT patient's sequence (combined source)"),
    ("dmt_transition_ranking", SHEET_MARK, "(from_drug -> to_drug) pairs ranked most-to-least common across all steps/patients (combined source)"),
    ("first_dmt_class_distribution", SHEET_MARK, "drug_class of each DMT-treated patient's very FIRST medication/procedure record (combined source), ranked by % of treated patients"),
    ("dmt_by_year_abs_efficacy", SHEET_MARK, "Unique patients per efficacy tier (High vs Standard) ACTIVE in each calendar year, combined source"),
    ("dmt_by_year_cov_efficacy", SHEET_MARK, "dmt_by_year_abs_efficacy / cumulative patients diagnosed by that year, as a %"),
    ("time_to_dmt_by_subtype", SHEET_MARK, "Median/Q1/Q3/IQR/Mean days to first DMT + % within 30/90/365 days, split by MS subtype, combined source"),
    ("time_to_dmt_by_age_grp", SHEET_MARK, "Same time-to-first-DMT metrics, split by age_group"),
    ("time_to_dmt_by_onset", SHEET_MARK, "Same time-to-first-DMT metrics, split by onset_category"),
    ("first_dmt_class_by_subtype", SHEET_MARK, "drug_class of each patient's FIRST DMT record, split by MS subtype, combined source"),
    ("dmt_courses", SHEET_MARK, "One row per (patient, drug) course, combined source: course_start/course_end/discont_date/discont_reason/discontinued flag/persistence_days"),
    ("discontinuation_summary", SHEET_MARK, "N courses, % discontinued, and persistence among discontinued courses only (combined source; Procedures-sourced rows never carry a discont_date, so this is driven by Medications data as before)"),
    ("discontinuation_reasons", SHEET_MARK, "discont_reason ranked most-to-least common, among discontinued courses only"),

    ("Methodology", "Cohort definition", f"≥1 qualifying MS diagnosis (ICD-10 G35 family or legacy ICD-9 340), "
                                          f"udm_active_flag = 'Y', AND diag_date within {DATE_START}..{DATE_END} (NEW in run2 -- "
                                          "the original ms_summary_tables.xlsx used no date cutoff). This also changes "
                                          "first_diagnosis_date: it means 'first qualifying diagnosis within this window', "
                                          "not necessarily a patient's true lifetime-first MS diagnosis"),
    ("Methodology", "DMT / treatment data (NEW in run2)",
     "Combines rgd_udm_silver.medication (name-based match, as in the original run) AND rgd_udm_silver.procedures "
     "(HCPCS-code-based match, new) into one ms_dmt table. Both are restricted to the same date window via each "
     "table's own effective-date fallback chain: medication uses med_start_date -> med_fill_date -> enc_date; "
     "procedures uses proc_start_date -> encounter_date -> enc_date_proxy. See hcpcs_reference and dmt_by_source "
     "for exactly which drugs/patients this added."),
    ("Methodology", "No patients excluded", "Every patient in the (date-restricted) cohort appears in every sheet. Patients with a "
                                              "missing or negative birth year get age_at_diagnosis / age_group / "
                                              "onset_category = NULL, which every breakdown displays as 'Unknown' -- "
                                              "they are NOT dropped from N or the cohort. An age >100 is kept as a real "
                                              "value, not capped or treated as an error."),
    ("Methodology", "\"Region\"", "= patients.pat_state (only geographic field at usable granularity)"),
    ("Methodology", "Subtype flags/percentages", "NOT mutually exclusive -- mean \"ever diagnosed with X\" within the date window; a patient who "
                                                    "progressed RRMS->SPMS counts toward both."),
    ("Methodology", "Age at diagnosis", "patients.dob is birth YEAR only (not full DOB), so age is approximate (+/-1yr). "
                                         "No upper bound -- an age >100 is kept as a real value. NULL ('Unknown') only "
                                         "when dob is missing or the computed age would be negative"),
    ("Methodology", "Masking tokens", "\"<<STATE>>\" / \"<<UPCITY>>\" are literal small-cell-suppression placeholders in the "
                                       "source data, relabeled \"Masked (small-cell suppression)\" where shown"),
]

readme = pd.DataFrame(readme_rows, columns=["Sheet", "Column", "Description"])
readme

# ===== cell 45 =====
OUT_PATH = "/Users/anjali28/Desktop/MS demo paper/ms_summary_tables2.xlsx"

with pd.ExcelWriter(OUT_PATH, engine="openpyxl") as writer:
    readme.to_excel(writer, sheet_name="README", index=False)
    icd_reference.to_excel(writer, sheet_name="icd_reference", index=False)
    hcpcs_reference.to_excel(writer, sheet_name="hcpcs_reference", index=False)
    dmt_by_source.to_excel(writer, sheet_name="dmt_by_source")
    cohort_characteristics.to_excel(writer, sheet_name="cohort_characteristics", index=False)
    subtype_characteristics.to_excel(writer, sheet_name="subtype_characteristics")
    sex_by_region.to_excel(writer, sheet_name="sex_by_region")
    subtype_by_region.to_excel(writer, sheet_name="subtype_by_region")
    age_by_region.to_excel(writer, sheet_name="age_by_region")
    data_quality.to_excel(writer, sheet_name="data_quality", index=False)
    diagnoses_by_year.to_excel(writer, sheet_name="diagnoses_by_year")
    subtype_transitions.to_excel(writer, sheet_name="subtype_transitions", index=False)
    subtype_transition_patterns.to_excel(writer, sheet_name="subtype_transition_patterns", index=False)

    # ── DMT sheets (combined Medications + Procedures source) ─────────────────────
    dmt_overall_utilization.to_excel(writer, sheet_name="dmt_overall_utilization", index=False)
    dmt_utilization_by_class.to_excel(writer, sheet_name="dmt_utilization_by_class")
    dmt_utilization_by_drug.to_excel(writer, sheet_name="dmt_utilization_by_drug")
    dmt_utilization_by_subtype.to_excel(writer, sheet_name="dmt_utilization_by_subtype")
    dmt_utilization_by_age_group.to_excel(writer, sheet_name="dmt_utilization_by_age_grp")
    dmt_utilization_by_onset.to_excel(writer, sheet_name="dmt_utilization_by_onset")
    dmt_route_utilization.to_excel(writer, sheet_name="dmt_route_utilization")
    dmt_by_year_absolute_class.to_excel(writer, sheet_name="dmt_by_year_absolute_class")
    dmt_by_year_absolute_drug.to_excel(writer, sheet_name="dmt_by_year_absolute_drug")
    dmt_by_year_coverage_class.to_excel(writer, sheet_name="dmt_by_year_coverage_class")
    dmt_by_year_coverage_drug.to_excel(writer, sheet_name="dmt_by_year_coverage_drug")
    time_to_dmt_summary.to_excel(writer, sheet_name="time_to_first_dmt", index=False)
    dmt_count_categories.to_excel(writer, sheet_name="dmt_count_categories", index=False)
    dmt_single_vs_multiple.to_excel(writer, sheet_name="dmt_single_vs_multiple", index=False)
    dmt_sequences.to_excel(writer, sheet_name="dmt_sequences", index=False)
    dmt_transitions_long.to_excel(writer, sheet_name="dmt_transitions_long", index=False)
    dmt_transition_ranking.to_excel(writer, sheet_name="dmt_transition_ranking", index=False)
    first_dmt_class_distribution.to_excel(writer, sheet_name="first_dmt_class_distribution", index=False)
    dmt_by_year_absolute_efficacy.to_excel(writer, sheet_name="dmt_by_year_abs_efficacy")
    dmt_by_year_coverage_efficacy.to_excel(writer, sheet_name="dmt_by_year_cov_efficacy")
    time_to_dmt_by_subtype.to_excel(writer, sheet_name="time_to_dmt_by_subtype")
    time_to_dmt_by_age_group.to_excel(writer, sheet_name="time_to_dmt_by_age_grp")
    time_to_dmt_by_onset.to_excel(writer, sheet_name="time_to_dmt_by_onset")
    first_dmt_class_by_subtype.to_excel(writer, sheet_name="first_dmt_class_by_subtype")
    dmt_courses.to_excel(writer, sheet_name="dmt_courses", index=False)
    discontinuation_summary.to_excel(writer, sheet_name="discontinuation_summary", index=False)
    discontinuation_reasons.to_excel(writer, sheet_name="discontinuation_reasons", index=False)

print(f"Wrote {OUT_PATH}")

# ===== post-processing: add a one-row heading banner to every tab =====
from openpyxl import load_workbook
from openpyxl.styles import Font, PatternFill, Alignment

HEADER_FILL = PatternFill(start_color="FF305496", end_color="FF305496", fill_type="solid")
HEADER_FONT = Font(color="FFFFFFFF", bold=True)
WRAP_TOP = Alignment(wrap_text=True, vertical="top", horizontal="left")

sheet_descriptions = {
    sheet: desc for sheet, col, desc in readme_rows
    if col == SHEET_MARK and sheet != "Methodology"
}

wb = load_workbook(OUT_PATH)
for name in wb.sheetnames:
    if name in ("README", "Methodology"):
        continue
    ws = wb[name]
    desc = sheet_descriptions.get(name, "")
    max_col = max(ws.max_column, 1)
    ws.insert_rows(1)
    ws.cell(row=1, column=1, value=f"{name} -- {desc}" if desc else name)
    ws.merge_cells(start_row=1, start_column=1, end_row=1, end_column=max_col)
    ws["A1"].fill = HEADER_FILL
    ws["A1"].font = HEADER_FONT
    ws["A1"].alignment = WRAP_TOP
    ws.row_dimensions[1].height = 30
    ws.freeze_panes = ws.cell(row=3, column=1)

wb.save(OUT_PATH)
print(f"Added heading banners to {len(wb.sheetnames) - 1} tabs; re-saved {OUT_PATH}")
