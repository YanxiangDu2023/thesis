import { useMemo, useState } from "react";
import UploadForm from "../components/upload/UploadForm";
import FilterableTable from "../components/table/FilterableTable";
import {
  getLatestControlReportCleanData,
  getUploadCompleteness,
  runControlReportCleanData,
} from "../api/uploads";
import type {
  ControlReportCleanRow,
  ControlReportCleanRun,
  UploadCompletenessResponse,
} from "../types/upload";

const MATRIX_TYPE_LABELS: Record<string, string> = {
  source_matrix: "Source Matrix",
  reporter_list: "Reporter List",
  size_class: "Size Class",
  brand_mapping: "Brand Mapping",
  group_country: "Group Country",
  machine_line_mapping: "Machine Line Mapping",
  oth_data: "OTH Data",
};

function OthUploadPage() {
  const [checking, setChecking] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState<UploadCompletenessResponse | null>(null);
  const [showCheckPanel, setShowCheckPanel] = useState(false);

  const [runningControlReport, setRunningControlReport] = useState(false);
  const [downloadingControlReport, setDownloadingControlReport] = useState(false);
  const [controlReportMessage, setControlReportMessage] = useState("");
  const [controlReportError, setControlReportError] = useState("");
  const [controlReportRun, setControlReportRun] = useState<ControlReportCleanRun | null>(null);
  const [controlReportRows, setControlReportRows] = useState<ControlReportCleanRow[]>([]);
  const [showControlReportPanel, setShowControlReportPanel] = useState(false);

  const orderedItems = useMemo(() => result?.items ?? [], [result]);

  const checkRows = useMemo(
    () =>
      orderedItems.map((item) => ({
        id: item.matrix_type,
        type: MATRIX_TYPE_LABELS[item.matrix_type] ?? item.matrix_type,
        uploaded: item.uploaded ? "Yes" : "No",
        latest_success_upload_id: item.latest_success_upload?.id ?? "-",
        latest_success_time: item.latest_success_upload?.uploaded_at ?? "-",
      })),
    [orderedItems]
  );

  const controlReportColumnKeys = useMemo(
    () => [
      "year",
      "source",
      "country_code",
      "country",
      "country_grouping",
      "region",
      "market_area",
      "machine_line_name",
      "machine_line_code",
      "brand_name",
      "brand_code",
      "size_class_flag",
      "fid",
      "ms_percent",
    ],
    []
  );

  const handleCheckCompleteness = async () => {
    try {
      setChecking(true);
      setError("");
      const completeness = await getUploadCompleteness();
      setResult(completeness);
      setShowCheckPanel(true);
    } catch (checkError) {
      console.error(checkError);
      setResult(null);
      setError(checkError instanceof Error ? checkError.message : "Failed to check uploads.");
    } finally {
      setChecking(false);
    }
  };

  const handleCloseCheckPanel = () => {
    setShowCheckPanel(false);
    setResult(null);
    setError("");
  };

  const handleRunControlReport = async () => {
    try {
      setRunningControlReport(true);
      setControlReportError("");
      setControlReportMessage("");
      const runResult = await runControlReportCleanData();
      setControlReportMessage(
        `Run successful. Run ID: ${runResult.control_run_id}, Row Count: ${runResult.row_count}`
      );
    } catch (runError) {
      console.error(runError);
      setControlReportError(
        runError instanceof Error ? runError.message : "Failed to run Control Report - Clean Data."
      );
    } finally {
      setRunningControlReport(false);
    }
  };

  const handleShowControlReport = async () => {
    try {
      setRunningControlReport(true);
      setControlReportError("");
      const latestResult = await getLatestControlReportCleanData();
      setControlReportRun(latestResult.run);
      setControlReportRows(latestResult.rows);
      setShowControlReportPanel(true);
    } catch (showError) {
      console.error(showError);
      setControlReportRun(null);
      setControlReportRows([]);
      setControlReportError(
        showError instanceof Error
          ? showError.message
          : "Failed to show latest Control Report - Clean Data."
      );
    } finally {
      setRunningControlReport(false);
    }
  };

  const handleCloseControlReportPanel = () => {
    setShowControlReportPanel(false);
    setControlReportRun(null);
    setControlReportRows([]);
  };

  const handleDownloadControlReport = async () => {
    try {
      setDownloadingControlReport(true);
      setControlReportError("");

      let runForDownload = controlReportRun;
      let rowsForDownload = controlReportRows;

      if (!runForDownload || rowsForDownload.length === 0) {
        const latestResult = await getLatestControlReportCleanData();
        runForDownload = latestResult.run;
        rowsForDownload = latestResult.rows;
        setControlReportRun(latestResult.run);
        setControlReportRows(latestResult.rows);
      }

      if (rowsForDownload.length === 0) {
        throw new Error("No row data found for Control Report - Clean Data.");
      }

      const csvHeader = controlReportColumnKeys.join(",");
      const csvRows = rowsForDownload.map((row) =>
        controlReportColumnKeys
          .map((column) => {
            const value = row[column as keyof ControlReportCleanRow];
            const text = value === null || value === undefined ? "" : String(value);
            if (/[",\r\n]/.test(text)) {
              return `"${text.replace(/"/g, "\"\"")}"`;
            }
            return text;
          })
          .join(",")
      );
      const csvContent = [csvHeader, ...csvRows].join("\r\n");

      const blob = new Blob(["\uFEFF", csvContent], { type: "text/csv;charset=utf-8;" });
      const objectUrl = URL.createObjectURL(blob);
      const fileName = `control_report_clean_data_${runForDownload?.id ?? "latest"}.csv`;

      const downloadLink = document.createElement("a");
      downloadLink.href = objectUrl;
      downloadLink.download = fileName;
      document.body.appendChild(downloadLink);
      downloadLink.click();
      downloadLink.remove();
      URL.revokeObjectURL(objectUrl);
    } catch (downloadError) {
      console.error(downloadError);
      setControlReportError(
        downloadError instanceof Error
          ? downloadError.message
          : "Failed to download Control Report - Clean Data."
      );
    } finally {
      setDownloadingControlReport(false);
    }
  };

  return (
    <div className="page-container">
      <h1>Upload OTH Data</h1>
      <p>Upload the CSV file required for OTH data configuration.</p>

      <div className="matrix-form">
        <UploadForm label="oth_data" title="OTH Data CSV" compact />
      </div>

      <div style={{ marginTop: "16px" }}>
        <button type="button" onClick={handleCheckCompleteness}>
          Check All Uploads
        </button>
        {checking && <p style={{ color: "blue" }}>Checking upload status...</p>}
        {error && <p style={{ color: "red" }}>Error: {error}</p>}
      </div>

      {showCheckPanel && result && (
        <div className="section summary-card" style={{ marginTop: "16px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <strong>Upload Completeness</strong>
            <button type="button" onClick={handleCloseCheckPanel} aria-label="Close upload completeness" title="Close">
              x
            </button>
          </div>
          <div className="summary-row">
            <span className="summary-label">Overall status</span>
            <span className="summary-value">
              {result.all_uploaded ? "All required files uploaded" : "Missing uploads"}
            </span>
          </div>
          {!result.all_uploaded && (
            <div className="summary-row">
              <span className="summary-label">Missing types</span>
              <span className="summary-value">
                {result.missing_types.map((type) => MATRIX_TYPE_LABELS[type] ?? type).join(", ")}
              </span>
            </div>
          )}

          <FilterableTable
            columns={[
              { key: "type", label: "Type" },
              { key: "uploaded", label: "Uploaded" },
              { key: "latest_success_upload_id", label: "Latest Success Upload ID" },
              { key: "latest_success_time", label: "Latest Success Time" },
            ]}
            rows={checkRows}
          />
        </div>
      )}

      <div style={{ marginTop: "16px", display: "flex", gap: "8px", flexWrap: "wrap" }}>
        <button type="button" onClick={handleRunControlReport}>
          Run Control Report - Clean Data
        </button>
        <button type="button" onClick={handleShowControlReport}>
          Show Control Report - Clean Data
        </button>
        <button type="button" onClick={handleDownloadControlReport} disabled={downloadingControlReport}>
          {downloadingControlReport ? "Downloading..." : "Download Control Report - Clean Data"}
        </button>
      </div>

      {runningControlReport && <p style={{ color: "blue" }}>Processing control report...</p>}
      {controlReportMessage && <p style={{ color: "green" }}>{controlReportMessage}</p>}
      {controlReportError && <p style={{ color: "red" }}>Error: {controlReportError}</p>}

      {showControlReportPanel && controlReportRun && (
        <div className="section summary-card" style={{ marginTop: "16px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <strong>Control Report - Clean Data</strong>
            <button
              type="button"
              onClick={handleCloseControlReportPanel}
              aria-label="Close control report panel"
              title="Close"
            >
              x
            </button>
          </div>

          <div className="summary-row">
            <span className="summary-label">Run ID</span>
            <span className="summary-value">{controlReportRun.id}</span>
          </div>
          <div className="summary-row">
            <span className="summary-label">Status</span>
            <span className="summary-value">{controlReportRun.status ?? "-"}</span>
          </div>
          <div className="summary-row">
            <span className="summary-label">Row Count</span>
            <span className="summary-value">{controlReportRun.row_count ?? 0}</span>
          </div>

          <FilterableTable
            columns={controlReportColumnKeys.map((column) => ({ key: column, label: column }))}
            rows={controlReportRows}
            maxHeight="420px"
            compact
          />
        </div>
      )}
    </div>
  );
}

export default OthUploadPage;
