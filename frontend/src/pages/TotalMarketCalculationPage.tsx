import { useMemo, useState } from "react";
import FilterableTable from "../components/table/FilterableTable";
import { getTotalMarketCalculationEligibleOthRows } from "../api/uploads";
import type { OthDeletionFlagRow } from "../types/upload";

function TotalMarketCalculationPage() {
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [rows, setRows] = useState<OthDeletionFlagRow[]>([]);
  const [sourceRowCount, setSourceRowCount] = useState(0);
  const [splitMachineLines, setSplitMachineLines] = useState<string[]>([]);

  const columns = useMemo(
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
      { key: "artificial_machine_line", label: "Artificial Machine Line" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "fid", label: "FID" },
      { key: "deletion_flag", label: "Deletion Flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "reporter_flag", label: "Reporter Flag" },
    ],
    []
  );

  async function handleLoad() {
    setLoading(true);
    setError("");
    setMessage("Loading total-market candidate rows...");

    try {
      const result = await getTotalMarketCalculationEligibleOthRows();
      setRows(result.rows);
      setSourceRowCount(result.source_row_count);
      setSplitMachineLines(result.split_machine_lines);
      setMessage(
        `Loaded ${result.row_count} OTH reporter rows from the split-ready machine lines.`
      );
    } catch (fetchError) {
      setError(
        fetchError instanceof Error
          ? fetchError.message
          : "Failed to load Total Market Calculation rows."
      );
      setMessage("");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="page">
      <section className="section section--layer-detail-wide">
        <div className="section-header">
          <p className="section-tag">Total Market Calculation</p>
          <h1 className="section-title">Total Market Calculation</h1>
          <p className="section-description">
            Review all OTH rows where <code>Reporter Flag = Y</code> and the artificial
            machine line is already inside the split scope.
          </p>
        </div>

        <div className="overview-actions" style={{ marginBottom: "20px" }}>
          <div className="overview-actions__buttons">
            <button
              type="button"
              className="btn btn--primary"
              onClick={handleLoad}
              disabled={loading}
            >
              {loading ? "Loading..." : "Show Total Market Calculation Rows"}
            </button>
          </div>
        </div>

        {message ? <p style={{ color: "#0a8f3d", marginBottom: "12px" }}>{message}</p> : null}
        {error ? <p style={{ color: "#d62828", marginBottom: "12px" }}>{error}</p> : null}

        <div className="card-grid card-grid--three" style={{ marginBottom: "16px" }}>
          <article className="card">
            <h4 className="card__title">Loaded Rows</h4>
            <p className="card__text">{rows.length.toLocaleString()}</p>
          </article>
          <article className="card">
            <h4 className="card__title">Source OTH Rows</h4>
            <p className="card__text">{sourceRowCount.toLocaleString()}</p>
          </article>
          <article className="card">
            <h4 className="card__title">Split Machine Lines</h4>
            <p className="card__text">
              {splitMachineLines.length > 0 ? splitMachineLines.join(", ") : "-"}
            </p>
          </article>
        </div>

        <div className="section summary-card" style={{ marginTop: "8px" }}>
          <strong>Total Market Calculation Input</strong>
          <FilterableTable
            columns={columns}
            rows={rows}
            maxHeight="620px"
            emptyMessage="No eligible OTH reporter rows found yet."
          />
        </div>
      </section>
    </div>
  );
}

export default TotalMarketCalculationPage;
