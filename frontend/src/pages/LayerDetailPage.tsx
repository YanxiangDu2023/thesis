import { useMemo, useState } from "react";
import { Link, useParams } from "react-router-dom";
import FilterableTable from "../components/table/FilterableTable";
import { getCrpD1CombinedReport } from "../api/uploads";
import type { CrpD1CombinedReportRow } from "../types/upload";

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
    ],
  },
  P10: {
    code: "P10",
    title: "Prepared Layer",
    description: "Prepared output after preparation and structural processing.",
    highlights: [
      "Applies data preparation logic and structural normalization.",
      "Aligns fields for adjustment stage compatibility.",
      "Generates prepared records ready for adjustment rules.",
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
    WHEN TRIM(CAST(a.machine_line_code AS TEXT)) = '390' THEN 'Y'
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
  "Deletion flag = Y when Machine Line Code = 390.",
  "Deletion flag = Y when Source = SAL and Machine Line name is not found in latest Source Matrix.",
  "Reporter Flag = # for TMA records, Reporter Flag = Y for SAL records.",
  "Brand Code = VCE for SAL records, Brand Code = # for TMA records.",
  "Country mapping first uses country code + year, then falls back to country name + year.",
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
      "Marks deletion for code 390, and also for SAL rows whose machine line name is missing from Source Matrix.",
    sql: `CASE
  WHEN TRIM(CAST(a.machine_line_code AS TEXT)) = '390' THEN 'Y'
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

function LayerDetailPage() {
  const params = useParams();
  const layerCode = (params.layerCode ?? "").toUpperCase();
  const layer = LAYER_DETAILS[layerCode];
  const [runningCombinedReport, setRunningCombinedReport] = useState(false);
  const [combinedReportMessage, setCombinedReportMessage] = useState("");
  const [combinedReportError, setCombinedReportError] = useState("");
  const [combinedReportRows, setCombinedReportRows] = useState<CrpD1CombinedReportRow[]>([]);
  const [showSqlGuide, setShowSqlGuide] = useState(false);

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
    ],
    []
  );

  const handleRunCrpD1CombinedReport = async () => {
    try {
      setRunningCombinedReport(true);
      setCombinedReportError("");
      setCombinedReportMessage("");

      const result = await getCrpD1CombinedReport();
      setCombinedReportRows(result.rows);
      setCombinedReportMessage(`Run successful. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setCombinedReportRows([]);
      setCombinedReportError(
        error instanceof Error ? error.message : "Failed to run CRP D1 Combined Report."
      );
    } finally {
      setRunningCombinedReport(false);
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

        <div className="summary-card">
          {layer.highlights.map((item) => (
            <div key={item} className="summary-row">
              <span className="summary-value">{item}</span>
            </div>
          ))}
        </div>

        {layer.code === "P00" ? (
          <div style={{ marginTop: "12px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
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
        ) : null}
        {layer.code === "P00" && showSqlGuide ? (
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

        {layer.code === "P00" && runningCombinedReport ? (
          <p style={{ color: "blue" }}>Running CRP D1 Combined Report...</p>
        ) : null}
        {layer.code === "P00" && combinedReportMessage ? (
          <p style={{ color: "green" }}>{combinedReportMessage}</p>
        ) : null}
        {layer.code === "P00" && combinedReportError ? (
          <p style={{ color: "red" }}>Error: {combinedReportError}</p>
        ) : null}
        {layer.code === "P00" && combinedReportRows.length > 0 ? (
          <div className="section summary-card" style={{ marginTop: "16px" }}>
            <strong>CRP D1 Combined Report</strong>
            <FilterableTable
              columns={combinedReportColumns}
              rows={combinedReportRows}
              maxHeight="420px"
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
