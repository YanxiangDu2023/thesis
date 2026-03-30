import { useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import FilterableTable from "../components/table/FilterableTable";
import { getA10AdjustmentReport, getCrpD1CombinedReport, getOthDeletionFlagReport, getP10VceNonVceReport } from "../api/uploads";
import type { A10AdjustmentRow, CrpD1CombinedReportRow, OthDeletionFlagRow, P10VceNonVceRow } from "../types/upload";

type LayerDetail = {
  code: string;
  title: string;
  description: string;
  highlights: string[];
};

const LAYER_DETAILS: Record<string, LayerDetail> = {
  P00: {
    code: "P00",
    title: "Preparation Raw Layer",
    description: "",
    highlights: [
      "1. For each CRP record, determine whether it should be deleted and whether it is classified as a reporter.",
      "2. For each OTH record, prepare mapped control report fields.",
      "2.1 Mark Deletion flag: Y if Machine Line Code = 390; otherwise Y when Country + Machine Line Name is not found in Source Matrix.",
      "2.2 Assign Pri/Sec: match Source + Country + Machine Line Name to Source Matrix (country_name + machine_line_name). If Source equals primary_source then P; if Source equals secondary_source then S; otherwise blank.",
      "2.3 Assign Reporter flag: first read CRP Source from Source Matrix by Country + Machine Line Name, then match Reporter List by source_code + machine_line + brand_code; if matched, set Y.",
    ],
  },
  P10: {
    code: "P10",
    title: "Prepared Layer",
    description: "Compute and display TMA, Volvo CE (VCE), and Non-Volvo CE values.",
    highlights: [
      "TMA (Total Market) comes from TMA source records.",
      "Rows with Volvo Deletion Flag = Y are excluded from the P10 report output.",
      "VCE includes Volvo/SAL rows where Source Matrix has a non-empty CRP Source for the matched Country + Machine Line Name, excluding Motor Graders.",
      "For Volvo CEX, Mini is temporarily treated like <6T and Midi like 6<11T, which may create some variance because TMA CEX uses <6T, 6<11T, and 6<10T.",
      "Non-VCE = max(TMA - VCE, 0).",
    ],
  },
  A10: {
    code: "A10",
    title: "Adjustment Layer",
    description: "Intermediate adjusted output after selected business rules.",
    highlights: [
      "Applies adjustment logic and allocation rules.",
      "Produces intermediate adjusted values for review.",
      "Supports validation before final adjustment output.",
    ],
  },
  A20: {
    code: "A20",
    title: "Final Adjustment Layer",
    description: "Finalized adjusted results for downstream review and analysis.",
    highlights: [
      "Finalizes adjustment outputs for reporting readiness.",
      "Provides stable result set for downstream consumers.",
      "Serves as the final stage in current prototype scope.",
    ],
  },
};

const CRP_D1_COMBINED_SQL = `WITH latest_tma AS (
  SELECT id
  FROM upload_runs
  WHERE matrix_type = 'tma_data' AND status = 'success'
  ORDER BY uploaded_at DESC, id DESC
  LIMIT 1
),
latest_volvo AS (
  SELECT id
  FROM upload_runs
  WHERE matrix_type = 'volvo_sale_data' AND status = 'success'
  ORDER BY uploaded_at DESC, id DESC
  LIMIT 1
),
latest_group_country AS (
  SELECT id
  FROM upload_runs
  WHERE matrix_type = 'group_country' AND status = 'success'
  ORDER BY uploaded_at DESC, id DESC
  LIMIT 1
),
latest_source_matrix AS (
  SELECT id
  FROM upload_runs
  WHERE matrix_type = 'source_matrix' AND status = 'success'
  ORDER BY uploaded_at DESC, id DESC
  LIMIT 1
),
gc_by_code AS (
  SELECT
    UPPER(TRIM(country_code)) AS country_code_key,
    UPPER(TRIM(year)) AS year_key,
    MIN(group_code) AS group_code,
    MIN(country_grouping) AS country_grouping,
    MIN(country_name) AS country_name,
    MIN(region) AS region
  FROM group_country_rows
  WHERE upload_run_id = (SELECT id FROM latest_group_country)
  GROUP BY UPPER(TRIM(country_code)), UPPER(TRIM(year))
),
gc_by_name AS (
  SELECT
    UPPER(TRIM(country_name)) AS country_name_key,
    UPPER(TRIM(year)) AS year_key,
    MIN(group_code) AS group_code,
    MIN(country_grouping) AS country_grouping,
    MIN(country_name) AS country_name,
    MIN(region) AS region
  FROM group_country_rows
  WHERE upload_run_id = (SELECT id FROM latest_group_country)
  GROUP BY UPPER(TRIM(country_name)), UPPER(TRIM(year))
),
tma_agg AS (
  SELECT
    TRIM(t.year) AS year,
    TRIM(t.end_country_code) AS end_country_code,
    TRIM(t.end_country) AS country_raw,
    TRIM(t.geographical_region) AS region_raw,
    TRIM(t.machine_line_code) AS machine_line_code,
    TRIM(t.machine_line) AS machine_line_name,
    TRIM(t.size_class_mapping) AS size_class,
    SUM(CAST(REPLACE(NULLIF(TRIM(t.total_market_fid_sales), ''), ',', '') AS REAL)) AS fid,
    'TMA' AS source
  FROM tma_data_rows t
  WHERE t.upload_run_id = (SELECT id FROM latest_tma)
  GROUP BY
    TRIM(t.year),
    TRIM(t.end_country_code),
    TRIM(t.end_country),
    TRIM(t.geographical_region),
    TRIM(t.machine_line_code),
    TRIM(t.machine_line),
    TRIM(t.size_class_mapping)
),
volvo_agg AS (
  SELECT
    TRIM(v.calendar) AS year,
    TRIM(v.country) AS end_country_code,
    TRIM(v.country) AS country_raw,
    TRIM(v.region) AS region_raw,
    TRIM(v.machine) AS machine_line_code,
    TRIM(v.machine_line) AS machine_line_name,
    TRIM(v.size_class) AS size_class,
    SUM(CAST(REPLACE(NULLIF(TRIM(v.fid), ''), ',', '') AS REAL)) AS fid,
    COALESCE(NULLIF(TRIM(v.source), ''), 'SAL') AS source
  FROM volvo_sale_data_rows v
  WHERE v.upload_run_id = (SELECT id FROM latest_volvo)
  GROUP BY
    TRIM(v.calendar),
    TRIM(v.country),
    TRIM(v.region),
    TRIM(v.machine),
    TRIM(v.machine_line),
    TRIM(v.size_class),
    COALESCE(NULLIF(TRIM(v.source), ''), 'SAL')
),
all_agg AS (
  SELECT * FROM tma_agg
  UNION ALL
  SELECT * FROM volvo_agg
),
source_matrix_machine_lines AS (
  SELECT
    UPPER(TRIM(machine_line_name)) AS machine_line_name_key
  FROM source_matrix_rows
  WHERE upload_run_id = (SELECT id FROM latest_source_matrix)
    AND TRIM(COALESCE(machine_line_name, '')) <> ''
  GROUP BY UPPER(TRIM(machine_line_name))
)
SELECT
  a.year AS "Year",
  COALESCE(g_code.group_code, g_name.group_code, '') AS "Country Group Code",
  COALESCE(g_code.country_grouping, g_name.country_grouping, '') AS "Country Grouping",
  COALESCE(g_code.country_name, g_name.country_name, a.country_raw) AS "Country",
  COALESCE(g_code.region, g_name.region, a.region_raw) AS "Region",
  a.machine_line_code AS "Machine Line Code",
  a.machine_line_name AS "Machine Line name",
  a.size_class AS "Size Class",
  CASE
    WHEN UPPER(TRIM(a.source)) = 'SAL' THEN 'VCE'
    ELSE '#'
  END AS "Brand Code",
  CASE
    WHEN UPPER(TRIM(a.source)) = 'TMA' THEN '#'
    ELSE 'Y'
  END AS "Reporter Flag",
  '#' AS "Pri/Sec",
  a.source AS "Source",
  CASE
    WHEN UPPER(TRIM(a.source)) = 'SAL'
         AND TRIM(CAST(a.machine_line_code AS TEXT)) = '390' THEN 'Y'
    WHEN UPPER(TRIM(a.source)) = 'SAL'
         AND TRIM(COALESCE(CAST(a.machine_line_name AS TEXT), '')) <> ''
         AND sm.machine_line_name_key IS NULL THEN 'Y'
    ELSE ''
  END AS "Deletion flag",
  a.fid AS "fid"
FROM all_agg a
LEFT JOIN gc_by_code g_code
  ON UPPER(TRIM(a.end_country_code)) = g_code.country_code_key
 AND UPPER(TRIM(a.year)) = g_code.year_key
LEFT JOIN gc_by_name g_name
  ON UPPER(TRIM(a.country_raw)) = g_name.country_name_key
 AND UPPER(TRIM(a.year)) = g_name.year_key
LEFT JOIN source_matrix_machine_lines sm
  ON UPPER(TRIM(COALESCE(CAST(a.machine_line_name AS TEXT), ''))) = sm.machine_line_name_key
ORDER BY
  "Country Grouping",
  "Country Group Code",
  "Country",
  "Machine Line Code",
  "Machine Line name",
  "Size Class";`;

const CRP_D1_RULE_BULLETS = [
  "Deletion flag is only evaluated for SAL records.",
  "Deletion flag = Y when Source = SAL and Machine Line Code = 390.",
  "Deletion flag = Y when Source = SAL and Machine Line name is not found in latest Source Matrix.",
  "Reporter Flag = # for TMA records, Reporter Flag = Y for SAL records.",
  "Brand Code = VCE for SAL records, Brand Code = # for TMA records.",
  "Country mapping first uses country code + year, then falls back to country name + year.",
];

const OTH_DELETION_FLAG_SQL = `WITH source_matrix_base AS (
  SELECT
    UPPER(TRIM(country_name)) AS country_name_key,
    UPPER(TRIM(machine_line_name)) AS machine_line_name_key,
    UPPER(TRIM(primary_source)) AS primary_source_key,
    UPPER(TRIM(secondary_source)) AS secondary_source_key
  FROM source_matrix_rows
  WHERE upload_run_id = :latest_source_matrix_upload_run_id
    AND TRIM(COALESCE(country_name, '')) <> ''
    AND TRIM(COALESCE(machine_line_name, '')) <> ''
),
source_matrix_keys AS (
  SELECT country_name_key, machine_line_name_key
  FROM source_matrix_base
  GROUP BY country_name_key, machine_line_name_key
),
source_matrix_source_flags AS (
  SELECT country_name_key, machine_line_name_key, primary_source_key AS source_key, 'P' AS pri_sec
  FROM source_matrix_base
  WHERE TRIM(COALESCE(primary_source_key, '')) <> ''
  UNION ALL
  SELECT country_name_key, machine_line_name_key, secondary_source_key AS source_key, 'S' AS pri_sec
  FROM source_matrix_base
  WHERE TRIM(COALESCE(secondary_source_key, '')) <> ''
),
source_matrix_source_flags_dedup AS (
  SELECT
    country_name_key,
    machine_line_name_key,
    source_key,
    CASE
      WHEN SUM(CASE WHEN pri_sec = 'P' THEN 1 ELSE 0 END) > 0 THEN 'P'
      WHEN SUM(CASE WHEN pri_sec = 'S' THEN 1 ELSE 0 END) > 0 THEN 'S'
      ELSE ''
    END AS pri_sec
  FROM source_matrix_source_flags
  GROUP BY country_name_key, machine_line_name_key, source_key
)
SELECT
  o.year AS year,
  o.source AS source,
  o.country AS country_code,
  COALESCE(g.country_name, o.country) AS country,
  COALESCE(m.machine_line_name, o.machine_line) AS machine_line_name,
  m.machine_line_code AS machine_line_code,
  COALESCE(b.brand_code, '') AS brand_code,
  CASE
    WHEN TRIM(COALESCE(m.machine_line_code, '')) = '390' THEN 'Y'
    WHEN TRIM(COALESCE(g.country_name, o.country, '')) <> ''
      AND TRIM(COALESCE(m.machine_line_name, o.machine_line, '')) <> ''
      AND smk.country_name_key IS NULL THEN 'Y'
    ELSE ''
  END AS deletion_flag,
  COALESCE(smsf.pri_sec, '') AS pri_sec,
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM source_matrix_rows sm
      JOIN reporter_list_rows rl
        ON UPPER(TRIM(COALESCE(rl.source_code, ''))) = UPPER(TRIM(COALESCE(sm.crp_source, '')))
        AND UPPER(TRIM(COALESCE(rl.machine_line, ''))) = UPPER(TRIM(COALESCE(m.machine_line_name, o.machine_line, '')))
        AND UPPER(TRIM(COALESCE(rl.brand_code, ''))) = UPPER(TRIM(COALESCE(b.brand_code, '')))
        AND rl.upload_run_id = :latest_reporter_list_upload_run_id
      WHERE sm.upload_run_id = :latest_source_matrix_upload_run_id
        AND UPPER(TRIM(COALESCE(sm.country_name, ''))) = UPPER(TRIM(COALESCE(g.country_name, o.country, '')))
        AND UPPER(TRIM(COALESCE(sm.machine_line_name, ''))) = UPPER(TRIM(COALESCE(m.machine_line_name, o.machine_line, '')))
        AND TRIM(COALESCE(sm.crp_source, '')) <> ''
    ) THEN 'Y'
    ELSE ''
  END AS reporter_flag
FROM oth_data_rows o
LEFT JOIN group_country_rows g
  ON UPPER(TRIM(o.country)) = UPPER(TRIM(g.country_code))
 AND UPPER(TRIM(o.year)) = UPPER(TRIM(g.year))
LEFT JOIN machine_line_mapping_rows m
  ON (
      UPPER(TRIM(o.machine_line)) = UPPER(TRIM(m.machine_line_name))
      OR UPPER(TRIM(o.machine_line)) = UPPER(TRIM(m.machine_line_code))
  )
LEFT JOIN brand_mapping_rows b
  ON UPPER(TRIM(o.brand_name)) = UPPER(TRIM(b.brand_name))
LEFT JOIN source_matrix_keys smk
  ON UPPER(TRIM(COALESCE(g.country_name, o.country))) = smk.country_name_key
 AND UPPER(TRIM(COALESCE(m.machine_line_name, o.machine_line))) = smk.machine_line_name_key
LEFT JOIN source_matrix_source_flags_dedup smsf
  ON UPPER(TRIM(COALESCE(g.country_name, o.country))) = smsf.country_name_key
 AND UPPER(TRIM(COALESCE(m.machine_line_name, o.machine_line))) = smsf.machine_line_name_key
 AND UPPER(TRIM(COALESCE(o.source, ''))) = smsf.source_key
WHERE o.upload_run_id = :latest_oth_upload_run_id
ORDER BY o.row_index ASC;`;

const OTH_RULE_BULLETS = [
  "2.1 Deletion flag: set Y when Machine Line Code = 390, or when Country + Machine Line Name is missing in Source Matrix.",
  "2.2 Pri/Sec: match Source + Country + Machine Line Name to Source Matrix. Match primary_source -> P, match secondary_source -> S, no match -> blank.",
  "2.3 Reporter flag: get CRP Source from Source Matrix by Country + Machine Line Name, then match Reporter List by source_code + machine_line + brand_code. If matched, set Y.",
];

const OTH_SQL_MAP_BULLETS = [
  "`source_matrix_keys`: checks whether Country + Machine Line Name exists in Source Matrix (for 2.1).",
  "`source_matrix_source_flags_dedup`: resolves Pri/Sec by Source + Country + Machine Line Name (for 2.2).",
  "`EXISTS` join with `source_matrix_rows` + `reporter_list_rows`: resolves Reporter flag (for 2.3).",
];

const OTH_KEY_SQL_SNIPPETS: Array<{ title: string; explain: string; sql: string }> = [
  {
    title: "2.2 Pri/Sec Rule",
    explain: "Pri/Sec is assigned by matching Source on top of Country + Machine Line Name.",
    sql: `COALESCE(smsf.pri_sec, '') AS pri_sec
...
LEFT JOIN source_matrix_source_flags_dedup smsf
  ON UPPER(TRIM(COALESCE(g.country_name, o.country))) = smsf.country_name_key
 AND UPPER(TRIM(COALESCE(m.machine_line_name, o.machine_line))) = smsf.machine_line_name_key
 AND UPPER(TRIM(COALESCE(o.source, ''))) = smsf.source_key`,
  },
  {
    title: "2.3 Reporter Flag Rule",
    explain: "Reporter flag becomes Y only when Source Matrix CRP Source links to Reporter List for same machine line + brand.",
    sql: `CASE
  WHEN EXISTS (
    SELECT 1
    FROM source_matrix_rows sm
    JOIN reporter_list_rows rl
      ON UPPER(TRIM(COALESCE(rl.source_code, ''))) = UPPER(TRIM(COALESCE(sm.crp_source, '')))
     AND UPPER(TRIM(COALESCE(rl.machine_line, ''))) = UPPER(TRIM(COALESCE(m.machine_line_name, o.machine_line, '')))
     AND UPPER(TRIM(COALESCE(rl.brand_code, ''))) = UPPER(TRIM(COALESCE(b.brand_code, '')))
    WHERE ...
  ) THEN 'Y'
  ELSE ''
END AS reporter_flag`,
  },
];

const CRP_D1_SQL_MAP_BULLETS = [
  "`gc_by_code` and `gc_by_name`: build country lookup tables from Group Country upload.",
  "`tma_agg` and `volvo_agg`: normalize + aggregate TMA and SAL rows into the same shape.",
  "`source_matrix_machine_lines`: list valid Machine Line names from latest Source Matrix.",
  "`all_agg`: union TMA + SAL records for final rule application.",
];

const CRP_D1_KEY_SQL_SNIPPETS: Array<{ title: string; explain: string; sql: string }> = [
  {
    title: "Deletion Flag Rule",
    explain:
      "Only SAL rows can be marked for deletion: code 390, or machine line name missing from Source Matrix.",
    sql: `CASE
  WHEN UPPER(TRIM(a.source)) = 'SAL'
       AND TRIM(CAST(a.machine_line_code AS TEXT)) = '390' THEN 'Y'
  WHEN UPPER(TRIM(a.source)) = 'SAL'
       AND TRIM(COALESCE(CAST(a.machine_line_name AS TEXT), '')) <> ''
       AND sm.machine_line_name_key IS NULL THEN 'Y'
  ELSE ''
END AS "Deletion flag"`,
  },
  {
    title: "Reporter + Brand Rules",
    explain: "Reporter and brand are assigned by source type (SAL vs TMA).",
    sql: `CASE
  WHEN UPPER(TRIM(a.source)) = 'SAL' THEN 'VCE'
  ELSE '#'
END AS "Brand Code",
CASE
  WHEN UPPER(TRIM(a.source)) = 'TMA' THEN '#'
  ELSE 'Y'
END AS "Reporter Flag"`,
  },
  {
    title: "Country Mapping Priority",
    explain: "Country info prefers country_code + year match, then falls back to country_name + year.",
    sql: `COALESCE(g_code.group_code, g_name.group_code, '') AS country_group_code,
COALESCE(g_code.country_grouping, g_name.country_grouping, '') AS country_grouping,
COALESCE(g_code.country_name, g_name.country_name, a.country_raw) AS country,
COALESCE(g_code.region, g_name.region, a.region_raw) AS region

LEFT JOIN gc_by_code g_code
  ON UPPER(TRIM(a.end_country_code)) = g_code.country_code_key
 AND UPPER(TRIM(a.year)) = g_code.year_key
LEFT JOIN gc_by_name g_name
  ON UPPER(TRIM(a.country_raw)) = g_name.country_name_key
 AND UPPER(TRIM(a.year)) = g_name.year_key`,
  },
];

const P10_RULE_BULLETS = [
  "Total Market (TMA): sum of rows where Source = TMA.",
  "Rows with Volvo Deletion Flag = Y are excluded from the P10 report output.",
  "VCE: sum of Volvo/SAL rows where Source Matrix has a non-empty CRP Source for the matched Country + Machine Line Name, excluding Motor Graders.",
  "For Volvo CEX, Mini is temporarily mapped to <6T and Midi is temporarily mapped to 6<11T.",
  "TMA CEX uses <6T, 6<11T, and 6<10T, so this temporary mapping may cause some variance.",
  "Non-VCE: max(TMA - VCE, 0).",
];

const A10_RULE_BULLETS = [
  "A10 shows SAL rows, TMA rows, and one derived Result row for each matched group.",
  "Result row FID comes from valid Volvo/SAL rows; TM FID comes from TMA rows for the same group.",
  "TM Non VCE is calculated as max(TM FID - FID, 0) on the Result row.",
];

const REPORT_TABLE_MAX_HEIGHT = "72vh";

function LayerDetailPage() {
  const params = useParams();
  const layerCode = (params.layerCode ?? "").toUpperCase();
  const layer = LAYER_DETAILS[layerCode];
  const [runningCombinedReport, setRunningCombinedReport] = useState(false);
  const [combinedReportMessage, setCombinedReportMessage] = useState("");
  const [combinedReportError, setCombinedReportError] = useState("");
  const [combinedReportRows, setCombinedReportRows] = useState<CrpD1CombinedReportRow[]>([]);
  const [combinedReportResetToken, setCombinedReportResetToken] = useState(0);
  const [runningOthDeletionFlagReport, setRunningOthDeletionFlagReport] = useState(false);
  const [othDeletionFlagMessage, setOthDeletionFlagMessage] = useState("");
  const [othDeletionFlagError, setOthDeletionFlagError] = useState("");
  const [othDeletionFlagRows, setOthDeletionFlagRows] = useState<OthDeletionFlagRow[]>([]);
  const [othDeletionFlagResetToken, setOthDeletionFlagResetToken] = useState(0);
  const [runningP10Report, setRunningP10Report] = useState(false);
  const [p10Message, setP10Message] = useState("");
  const [p10Error, setP10Error] = useState("");
  const [p10Rows, setP10Rows] = useState<P10VceNonVceRow[]>([]);
  const [p10FilteredRows, setP10FilteredRows] = useState<P10VceNonVceRow[] | null>(null);
  const [p10Summary, setP10Summary] = useState({
    total_market_sum: 0,
    vce_sum: 0,
    non_vce_sum: 0,
  });
  const [p10ResetToken, setP10ResetToken] = useState(0);
  const [runningA10Report, setRunningA10Report] = useState(false);
  const [a10Message, setA10Message] = useState("");
  const [a10Error, setA10Error] = useState("");
  const [a10Rows, setA10Rows] = useState<A10AdjustmentRow[]>([]);
  const [a10ResetToken, setA10ResetToken] = useState(0);
  const [showSqlGuide, setShowSqlGuide] = useState(false);
  const [showOthSqlGuide, setShowOthSqlGuide] = useState(false);

  const combinedReportColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "country_group_code", label: "Country Group Code" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "country", label: "Country" },
      { key: "region", label: "Region" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "machine_line_name", label: "Machine Line name" },
      { key: "size_class", label: "Size Class" },
      { key: "brand_code", label: "Brand Code" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "source", label: "Source" },
      { key: "deletion_flag", label: "Deletion flag" },
      { key: "fid", label: "fid" },
      { key: "tm", label: "TM" },
      { key: "vce_fid", label: "VCE FID" },
      { key: "tm_non_vce", label: "TM Non VCE" },
    ],
    []
  );

  const p10Columns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "country_group_code", label: "Country Group Code" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "country", label: "Country" },
      { key: "region", label: "Region" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "machine_line_name", label: "Machine Line name" },
      { key: "size_class", label: "Size Class" },
      { key: "total_market", label: "TMA (Total Market)" },
      { key: "vce", label: "Volvo CE (VCE)" },
      { key: "non_vce", label: "Non-Volvo CE" },
      { key: "vce_share_pct", label: "VCE / TMA (%)" },
    ],
    []
  );

  const a10Columns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "country_group_code", label: "Country Group Code" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "country", label: "Country" },
      { key: "region", label: "Region" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "machine_line_name", label: "Machine Line name" },
      { key: "size_class", label: "Size Class" },
      { key: "brand_code", label: "Brand Code" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "vce_flag", label: "VCE Flag" },
      { key: "source", label: "Source" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "calculation_step", label: "Calculation Step" },
      { key: "fid", label: "FID" },
      { key: "tm_fid", label: "TM FID" },
      { key: "tm_non_vce", label: "TM Non VCE" },
    ],
    []
  );

  const othDeletionFlagColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "source", label: "Source" },
      { key: "country_code", label: "Country Code" },
      { key: "country", label: "Country" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "region", label: "Region" },
      { key: "market_area", label: "Market Area" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "fid", label: "FID" },
      { key: "ms_percent", label: "MS (%)" },
      { key: "deletion_flag", label: "Deletion flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "reporter_flag", label: "Reporter flag" },
    ],
    []
  );

  const p10Share = useMemo(() => {
    const rowsForShare = p10FilteredRows ?? p10Rows;
    const totalMarketRaw = rowsForShare.reduce((sum, row) => sum + Number(row.total_market || 0), 0);
    const vceRaw = rowsForShare.reduce((sum, row) => sum + Number(row.vce || 0), 0);
    const nonVceRaw = rowsForShare.reduce((sum, row) => sum + Number(row.non_vce || 0), 0);
    const totalMarket = Math.max(totalMarketRaw, 0);
    const safeVce = Math.max(Math.min(vceRaw, totalMarket), 0);
    const safeNonVce = Math.max(Math.min(nonVceRaw, totalMarket), 0);
    const vcePct = totalMarket > 0 ? (safeVce / totalMarket) * 100 : 0;
    const nonVcePct = totalMarket > 0 ? (safeNonVce / totalMarket) * 100 : 0;
    return {
      totalMarket,
      safeVce,
      safeNonVce,
      vcePct,
      nonVcePct,
    };
  }, [p10FilteredRows, p10Rows]);

  const handleRunCrpD1CombinedReport = async () => {
    try {
      setRunningCombinedReport(true);
      setCombinedReportError("");
      setCombinedReportMessage("");

      const result = await getCrpD1CombinedReport();
      setCombinedReportRows(result.rows);
      setCombinedReportResetToken((prev) => prev + 1);
      setCombinedReportMessage(`Run successful. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setCombinedReportRows([]);
      setCombinedReportResetToken((prev) => prev + 1);
      setCombinedReportError(
        error instanceof Error ? error.message : "Failed to run CRP D1 Combined Report."
      );
    } finally {
      setRunningCombinedReport(false);
    }
  };

  const handleRunOthDeletionFlagReport = async () => {
    try {
      setRunningOthDeletionFlagReport(true);
      setOthDeletionFlagError("");
      setOthDeletionFlagMessage("");

      const result = await getOthDeletionFlagReport();
      setOthDeletionFlagRows(result.rows);
      setOthDeletionFlagResetToken((prev) => prev + 1);
      setOthDeletionFlagMessage(`Run successful. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setOthDeletionFlagRows([]);
      setOthDeletionFlagResetToken((prev) => prev + 1);
      setOthDeletionFlagError(
        error instanceof Error ? error.message : "Failed to run OTH Deletion Flag Report."
      );
    } finally {
      setRunningOthDeletionFlagReport(false);
    }
  };

  const handleRunP10Report = async () => {
    try {
      setRunningP10Report(true);
      setP10Error("");
      setP10Message("");

      const result = await getP10VceNonVceReport();
      setP10Rows(result.rows);
      setP10FilteredRows(result.rows);
      setP10Summary(result.summary);
      setP10ResetToken((prev) => prev + 1);
      setP10Message(`Run successful. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setP10Rows([]);
      setP10FilteredRows(null);
      setP10Summary({
        total_market_sum: 0,
        vce_sum: 0,
        non_vce_sum: 0,
      });
      setP10ResetToken((prev) => prev + 1);
      setP10Error(error instanceof Error ? error.message : "Failed to run P10 VCE / Non-VCE Report.");
    } finally {
      setRunningP10Report(false);
    }
  };

  const handleRunA10Report = async () => {
    try {
      setRunningA10Report(true);
      setA10Error("");
      setA10Message("");

      const result = await getA10AdjustmentReport();
      setA10Rows(result.rows);
      setA10ResetToken((prev) => prev + 1);
      setA10Message(`Run successful. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setA10Rows([]);
      setA10ResetToken((prev) => prev + 1);
      setA10Error(error instanceof Error ? error.message : "Failed to run A10 Adjustment Report.");
    } finally {
      setRunningA10Report(false);
    }
  };

  if (!layer) {
    return (
      <div className="page">
        <section className="section">
          <div className="section-header">
            <p className="section-tag">Layer</p>
            <h2 className="section-title">Layer Not Found</h2>
            <p className="section-description">
              The requested layer does not exist in this prototype.
            </p>
          </div>
          <Link to="/" className="btn btn--primary">
            Back to Home
          </Link>
        </section>
      </div>
    );
  }

  return (
    <div className="page">
      <section className="section">
        <div className="section-header">
          <p className="section-tag">Layer Detail</p>
          <h2 className="section-title">
            {layer.code} - {layer.title}
          </h2>
          {layer.description ? <p className="section-description">{layer.description}</p> : null}
        </div>

        {layer.code === "P00" ? (
          <div className="summary-card">
            <div className="summary-row">
              <span className="summary-value">{layer.highlights[0]}</span>
            </div>
            <div style={{ marginTop: "4px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
              <button
                type="button"
                className="btn btn--overview"
                onClick={handleRunCrpD1CombinedReport}
                disabled={runningCombinedReport}
              >
                Run CRP D1 Combined Report
              </button>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={() => setShowSqlGuide((prev) => !prev)}
              >
                {showSqlGuide ? "Hide SQL Logic" : "View SQL Logic"}
              </button>
            </div>

            {runningCombinedReport ? (
              <p style={{ color: "blue" }}>Running CRP D1 Combined Report...</p>
            ) : null}
            {combinedReportMessage ? (
              <p style={{ color: "green" }}>{combinedReportMessage}</p>
            ) : null}
            {combinedReportError ? (
              <p style={{ color: "red" }}>Error: {combinedReportError}</p>
            ) : null}
            {combinedReportRows.length > 0 ? (
              <div className="section summary-card" style={{ marginTop: "8px" }}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }}>
                  <strong>CRP D1 Combined Report</strong>
                  <button
                    type="button"
                    className="btn btn--tiny"
                    onClick={() => {
                      setCombinedReportRows([]);
                      setCombinedReportMessage("");
                      setCombinedReportError("");
                    }}
                    aria-label="Close CRP D1 Combined Report"
                  >
                    x
                  </button>
                </div>
                <FilterableTable
                  columns={combinedReportColumns}
                  rows={combinedReportRows}
                  maxHeight={REPORT_TABLE_MAX_HEIGHT}
                  resetToken={combinedReportResetToken}
                  compact
                />
              </div>
            ) : null}
            {showSqlGuide ? (
              <div className="sql-guide">
                <h4 className="sql-guide__title">Business Rules</h4>
                <ul className="sql-guide__list">
                  {CRP_D1_RULE_BULLETS.map((rule) => (
                    <li key={rule}>{rule}</li>
                  ))}
                </ul>

                <h4 className="sql-guide__title">SQL Map</h4>
                <ul className="sql-guide__list">
                  {CRP_D1_SQL_MAP_BULLETS.map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>

                <h4 className="sql-guide__title">Key SQL Snippets</h4>
                <div className="sql-guide__snippets">
                  {CRP_D1_KEY_SQL_SNIPPETS.map((snippet) => (
                    <div key={snippet.title} className="sql-guide__snippet">
                      <strong>{snippet.title}</strong>
                      <p>{snippet.explain}</p>
                      <pre>
                        <code>{snippet.sql}</code>
                      </pre>
                    </div>
                  ))}
                </div>

                <details className="sql-guide__details">
                  <summary>View Full SQL</summary>
                  <pre>
                    <code>{CRP_D1_COMBINED_SQL}</code>
                  </pre>
                </details>
              </div>
            ) : null}

            {layer.highlights.slice(1).map((item) => (
              <div key={item} className="summary-row">
                <span className="summary-value">{item}</span>
              </div>
            ))}
            <div style={{ marginTop: "4px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
              <button
                type="button"
                className="btn btn--overview"
                onClick={handleRunOthDeletionFlagReport}
                disabled={runningOthDeletionFlagReport}
              >
                Run OTH Deletion Flag Report
              </button>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={() => setShowOthSqlGuide((prev) => !prev)}
              >
                {showOthSqlGuide ? "Hide SQL Logic" : "View SQL Logic"}
              </button>
            </div>
          </div>
        ) : (
          <div className="summary-card">
            {layer.highlights.map((item) => (
              <div key={item} className="summary-row">
                <span className="summary-value">{item}</span>
              </div>
            ))}
          </div>
        )}

        {layer.code === "P10" ? (
          <div style={{ marginTop: "12px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
            <button
              type="button"
              className="btn btn--overview"
              onClick={handleRunP10Report}
              disabled={runningP10Report}
            >
              Run P10 VCE / Non-VCE
            </button>
          </div>
        ) : null}
        {layer.code === "A10" ? (
          <div style={{ marginTop: "12px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
            <button
              type="button"
              className="btn btn--overview"
              onClick={handleRunA10Report}
              disabled={runningA10Report}
            >
              Run A10 Adjustment Report
            </button>
          </div>
        ) : null}
        {layer.code === "P00" && showOthSqlGuide ? (
          <div className="sql-guide">
            <h4 className="sql-guide__title">OTH Rules (2.1 / 2.2 / 2.3)</h4>
            <ul className="sql-guide__list">
              {OTH_RULE_BULLETS.map((rule) => (
                <li key={rule}>{rule}</li>
              ))}
            </ul>

            <h4 className="sql-guide__title">SQL Map</h4>
            <ul className="sql-guide__list">
              {OTH_SQL_MAP_BULLETS.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>

            <h4 className="sql-guide__title">Key SQL Snippets</h4>
            <div className="sql-guide__snippets">
              {OTH_KEY_SQL_SNIPPETS.map((snippet) => (
                <div key={snippet.title} className="sql-guide__snippet">
                  <strong>{snippet.title}</strong>
                  <p>{snippet.explain}</p>
                  <pre>
                    <code>{snippet.sql}</code>
                  </pre>
                </div>
              ))}
            </div>

            <details className="sql-guide__details">
              <summary>View Full SQL</summary>
              <pre>
                <code>{OTH_DELETION_FLAG_SQL}</code>
              </pre>
            </details>
          </div>
        ) : null}
        {layer.code === "P10" ? (
          <div className="sql-guide">
            <h4 className="sql-guide__title">Calculation Rules</h4>
            <ul className="sql-guide__list">
              {P10_RULE_BULLETS.map((rule) => (
                <li key={rule}>{rule}</li>
              ))}
            </ul>
          </div>
        ) : null}
        {layer.code === "A10" ? (
          <div className="sql-guide">
            <h4 className="sql-guide__title">Calculation Rules</h4>
            <ul className="sql-guide__list">
              {A10_RULE_BULLETS.map((rule) => (
                <li key={rule}>{rule}</li>
              ))}
            </ul>
          </div>
        ) : null}
        {layer.code === "P00" && runningOthDeletionFlagReport ? (
          <p style={{ color: "blue" }}>Running OTH Deletion Flag Report...</p>
        ) : null}
        {layer.code === "P00" && othDeletionFlagMessage ? (
          <p style={{ color: "green" }}>{othDeletionFlagMessage}</p>
        ) : null}
        {layer.code === "P00" && othDeletionFlagError ? (
          <p style={{ color: "red" }}>Error: {othDeletionFlagError}</p>
        ) : null}
        {layer.code === "P00" && othDeletionFlagRows.length > 0 ? (
          <div className="section summary-card" style={{ marginTop: "16px" }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }}>
              <strong>OTH Deletion Flag Report</strong>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={() => {
                  setOthDeletionFlagRows([]);
                  setOthDeletionFlagMessage("");
                  setOthDeletionFlagError("");
                }}
                aria-label="Close OTH Deletion Flag Report"
              >
                x
              </button>
            </div>
            <FilterableTable
              columns={othDeletionFlagColumns}
              rows={othDeletionFlagRows}
              maxHeight={REPORT_TABLE_MAX_HEIGHT}
              resetToken={othDeletionFlagResetToken}
              compact
            />
          </div>
        ) : null}
        {layer.code === "P10" && runningP10Report ? (
          <p style={{ color: "blue" }}>Running P10 VCE / Non-VCE calculation...</p>
        ) : null}
        {layer.code === "P10" && p10Message ? (
          <p style={{ color: "green" }}>{p10Message}</p>
        ) : null}
        {layer.code === "P10" && p10Error ? (
          <p style={{ color: "red" }}>Error: {p10Error}</p>
        ) : null}
        {layer.code === "P10" && p10Rows.length > 0 ? (
          <>
            <div className="card-grid card-grid--three" style={{ marginTop: "16px" }}>
              <article className="card">
                <h4 className="card__title">Total Market (TMA)</h4>
                <p className="summary-value">
                  {p10Summary.total_market_sum.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                </p>
              </article>
              <article className="card">
                <h4 className="card__title">Volvo CE (VCE)</h4>
                <p className="summary-value">
                  {p10Summary.vce_sum.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                </p>
              </article>
              <article className="card">
                <h4 className="card__title">Non-Volvo CE</h4>
                <p className="summary-value">
                  {p10Summary.non_vce_sum.toLocaleString(undefined, { maximumFractionDigits: 2 })}
                </p>
              </article>
            </div>
            <div className="section p10-share-card" style={{ marginTop: "16px" }}>
              <strong>VCE vs Non-VCE Share</strong>
              <div style={{ color: "#64748b", fontSize: "12px", marginTop: "4px" }}>
                Chart follows the filters applied in the table below.
              </div>
              <div className="p10-share-layout">
                <div
                  className="p10-donut"
                  style={{
                    background: `conic-gradient(#2563eb 0% ${p10Share.vcePct}%, #93c5fd ${p10Share.vcePct}% 100%)`,
                  }}
                >
                  <div className="p10-donut__hole">
                    <div className="p10-donut__value">{p10Share.vcePct.toFixed(1)}%</div>
                    <div className="p10-donut__label">VCE Share</div>
                  </div>
                </div>
                <div className="p10-share-legend">
                  <div className="p10-share-legend__item">
                    <span className="p10-share-legend__dot p10-share-legend__dot--vce" />
                    <span>Volvo CE (VCE)</span>
                    <strong>
                      {p10Share.safeVce.toLocaleString(undefined, { maximumFractionDigits: 2 })} ({p10Share.vcePct.toFixed(1)}
                      %)
                    </strong>
                  </div>
                  <div className="p10-share-legend__item">
                    <span className="p10-share-legend__dot p10-share-legend__dot--non-vce" />
                    <span>Non-Volvo CE</span>
                    <strong>
                      {p10Share.safeNonVce.toLocaleString(undefined, { maximumFractionDigits: 2 })} (
                      {p10Share.nonVcePct.toFixed(1)}%)
                    </strong>
                  </div>
                  <div className="p10-share-legend__total">
                    Total Market:{" "}
                    <strong>{p10Share.totalMarket.toLocaleString(undefined, { maximumFractionDigits: 2 })}</strong>
                  </div>
                </div>
              </div>
            </div>
            <div className="section summary-card" style={{ marginTop: "16px" }}>
              <strong>P10 VCE / Non-VCE Report</strong>
              <FilterableTable
                columns={p10Columns}
                rows={p10Rows}
                maxHeight={REPORT_TABLE_MAX_HEIGHT}
                resetToken={p10ResetToken}
                onFilteredRowsChange={(rows) => setP10FilteredRows(rows as P10VceNonVceRow[])}
                compact
              />
            </div>
          </>
        ) : null}
        {layer.code === "A10" && runningA10Report ? (
          <p style={{ color: "blue" }}>Running A10 Adjustment Report...</p>
        ) : null}
        {layer.code === "A10" && a10Message ? (
          <p style={{ color: "green" }}>{a10Message}</p>
        ) : null}
        {layer.code === "A10" && a10Error ? (
          <p style={{ color: "red" }}>Error: {a10Error}</p>
        ) : null}
        {layer.code === "A10" && a10Rows.length > 0 ? (
          <div className="section summary-card" style={{ marginTop: "16px" }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }}>
              <strong>A10 Adjustment Report</strong>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={() => {
                  setA10Rows([]);
                  setA10Message("");
                  setA10Error("");
                }}
                aria-label="Close A10 Adjustment Report"
              >
                x
              </button>
            </div>
            <FilterableTable
              columns={a10Columns}
              rows={a10Rows}
              maxHeight={REPORT_TABLE_MAX_HEIGHT}
              resetToken={a10ResetToken}
              getRowClassName={(row) =>
                String(row.brand_code ?? "").trim().toUpperCase() === "RESULT"
                  ? "data-table__row--result"
                  : undefined
              }
              compact
            />
          </div>
        ) : null}

        <div style={{ marginTop: "16px", display: "flex", gap: "10px", flexWrap: "wrap" }}>
          <Link to="/" className="btn btn--primary">
            Back to Home
          </Link>
          <Link to="/pipeline" className="btn btn--secondary">
            Open Pipeline Viewer
          </Link>
        </div>
      </section>
    </div>
  );
}

export default LayerDetailPage;

