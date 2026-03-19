import { useMemo, useState } from "react";
import UploadForm from "../components/upload/UploadForm";
import FilterableTable from "../components/table/FilterableTable";
import {
  getLatestCrpTmaReportCleanData,
  runCrpTmaReportCleanData,
} from "../api/uploads";
import type { CrpTmaReportRow, CrpTmaReportRun } from "../types/upload";

const VOLVO_SALE_COLUMNS = [
  "Calendar",
  "Region",
  "Market",
  "Country",
  "Machine",
  "Machine Line",
  "Size Class",
  "Brand Owner code",
  "Brand Owner",
  "Brand",
  "Brand Nationality",
  "Source",
  "FID",
];
const TMA_DATA_COLUMNS = [
  "Year",
  "Geographical Region",
  "Geographical Market Area",
  "End Country",
  "End Country Code",
  "Machine Family",
  "Machine Line",
  "Machine Line Code",
  "Size Class",
  "Size Class Mapping",
  "Total Market FID Sales",
];

function CrpUploadPage() {
  const [runningCrpTmaReport, setRunningCrpTmaReport] = useState(false);
  const [crpTmaReportMessage, setCrpTmaReportMessage] = useState("");
  const [crpTmaReportError, setCrpTmaReportError] = useState("");
  const [crpTmaReportRun, setCrpTmaReportRun] = useState<CrpTmaReportRun | null>(null);
  const [crpTmaReportRows, setCrpTmaReportRows] = useState<CrpTmaReportRow[]>([]);
  const [showCrpTmaReportPanel, setShowCrpTmaReportPanel] = useState(false);

  const crpTmaReportColumnKeys = useMemo(
    () => [
      "year",
      "geographical_region",
      "geographical_market_area",
      "end_country_code",
      "country",
      "machine_line",
      "machine_line_code",
      "size_class_mapping",
      "fid_sum",
      "source",
    ],
    []
  );

  const handleRunCrpTmaReport = async () => {
    try {
      setRunningCrpTmaReport(true);
      setCrpTmaReportError("");
      setCrpTmaReportMessage("");
      const runResult = await runCrpTmaReportCleanData();
      setCrpTmaReportMessage(
        `Run successful. Run ID: ${runResult.report_run_id}, Row Count: ${runResult.row_count}`
      );
    } catch (runError) {
      console.error(runError);
      setCrpTmaReportError(
        runError instanceof Error ? runError.message : "Failed to run CRP TMA Report - Clean Data."
      );
    } finally {
      setRunningCrpTmaReport(false);
    }
  };

  const handleShowCrpTmaReport = async () => {
    try {
      setRunningCrpTmaReport(true);
      setCrpTmaReportError("");
      const latestResult = await getLatestCrpTmaReportCleanData();
      setCrpTmaReportRun(latestResult.run);
      setCrpTmaReportRows(latestResult.rows);
      setShowCrpTmaReportPanel(true);
    } catch (showError) {
      console.error(showError);
      setCrpTmaReportRun(null);
      setCrpTmaReportRows([]);
      setCrpTmaReportError(
        showError instanceof Error
          ? showError.message
          : "Failed to show latest CRP TMA Report - Clean Data."
      );
    } finally {
      setRunningCrpTmaReport(false);
    }
  };

  const handleCloseCrpTmaReportPanel = () => {
    setShowCrpTmaReportPanel(false);
    setCrpTmaReportRun(null);
    setCrpTmaReportRows([]);
  };

  return (
    <div className="page">
      <section className="section">
        <div className="section-header">
          <p className="section-tag">CRP Data</p>
          <h2 className="section-title">Upload CRP Data</h2>
          <p className="section-description">
            This is a dedicated page for CRP data upload and validation.
          </p>
        </div>

        <div className="matrix-form">
          <UploadForm label="volvo_sale_data" title="Volvo Sale Data CSV" />
          <UploadForm label="tma_data" title="TMA Data CSV" />
        </div>

        <div className="crp-columns">
          <h3 className="crp-columns__title">Volvo Sale Data Columns</h3>
          <p className="crp-columns__description">
            Upload file should contain these columns in order:
          </p>
          <div className="crp-columns__chips">
            {VOLVO_SALE_COLUMNS.map((column) => (
              <span key={column} className="crp-columns__chip">
                {column}
              </span>
            ))}
          </div>
        </div>

        <div className="crp-columns">
          <h3 className="crp-columns__title">TMA Data Columns</h3>
          <p className="crp-columns__description">
            Upload file should contain these columns in order:
          </p>
          <div className="crp-columns__chips">
            {TMA_DATA_COLUMNS.map((column) => (
              <span key={column} className="crp-columns__chip">
                {column}
              </span>
            ))}
          </div>
        </div>

        <div style={{ marginTop: "16px", display: "flex", gap: "8px", flexWrap: "wrap" }}>
          <button type="button" onClick={handleRunCrpTmaReport}>
            Run CRP TMA Report - Clean Data
          </button>
          <button type="button" onClick={handleShowCrpTmaReport}>
            Show CRP TMA Report - Clean Data
          </button>
        </div>

        {runningCrpTmaReport && <p style={{ color: "blue" }}>Processing CRP TMA report...</p>}
        {crpTmaReportMessage && <p style={{ color: "green" }}>{crpTmaReportMessage}</p>}
        {crpTmaReportError && <p style={{ color: "red" }}>Error: {crpTmaReportError}</p>}

        {showCrpTmaReportPanel && crpTmaReportRun && (
          <div className="section summary-card" style={{ marginTop: "16px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <strong>CRP TMA Report - Clean Data</strong>
              <button
                type="button"
                onClick={handleCloseCrpTmaReportPanel}
                aria-label="Close CRP TMA report panel"
                title="Close"
              >
                x
              </button>
            </div>

            <div className="summary-row">
              <span className="summary-label">Run ID</span>
              <span className="summary-value">{crpTmaReportRun.id}</span>
            </div>
            <div className="summary-row">
              <span className="summary-label">Status</span>
              <span className="summary-value">{crpTmaReportRun.status ?? "-"}</span>
            </div>
            <div className="summary-row">
              <span className="summary-label">Row Count</span>
              <span className="summary-value">{crpTmaReportRun.row_count ?? 0}</span>
            </div>

            <FilterableTable
              columns={crpTmaReportColumnKeys.map((column) => ({ key: column, label: column }))}
              rows={crpTmaReportRows}
              maxHeight="420px"
            />
          </div>
        )}
      </section>
    </div>
  );
}

export default CrpUploadPage;
