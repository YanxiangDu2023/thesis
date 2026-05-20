import { useMemo, useState } from "react";
import {
  getLatestCrpSalReportCleanData,
  getLatestCrpTmaReportCleanData,
  runCrpSalReportCleanData,
  runCrpTmaReportCleanData,
} from "../api/uploads";
import FilterableTable from "../components/table/FilterableTable";
import UploadForm from "../components/upload/UploadForm";
import type { CrpSalReportRow, CrpSalReportRun, CrpTmaReportRow, CrpTmaReportRun } from "../types/upload";

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

function CrpUploadPage() {
  const [runningControlTma, setRunningControlTma] = useState(false);
  const [downloadingControlTma, setDownloadingControlTma] = useState(false);
  const [controlTmaMessage, setControlTmaMessage] = useState("");
  const [controlTmaError, setControlTmaError] = useState("");
  const [controlTmaRun, setControlTmaRun] = useState<CrpTmaReportRun | null>(null);
  const [controlTmaRows, setControlTmaRows] = useState<CrpTmaReportRow[]>([]);
  const [showControlTmaPanel, setShowControlTmaPanel] = useState(false);
  const [runningControlSal, setRunningControlSal] = useState(false);
  const [downloadingControlSal, setDownloadingControlSal] = useState(false);
  const [controlSalMessage, setControlSalMessage] = useState("");
  const [controlSalError, setControlSalError] = useState("");
  const [controlSalRun, setControlSalRun] = useState<CrpSalReportRun | null>(null);
  const [controlSalRows, setControlSalRows] = useState<CrpSalReportRow[]>([]);
  const [showControlSalPanel, setShowControlSalPanel] = useState(false);

  const controlTmaColumnKeys = useMemo(
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
  const controlSalColumnKeys = useMemo(
    () => [
      "calendar",
      "region",
      "market",
      "country",
      "machine",
      "machine_line",
      "size_class",
      "brand_owner_code",
      "brand_owner",
      "brand",
      "brand_nationality",
      "source",
      "fid",
    ],
    []
  );

  const toCsvCell = (value: string | number | null | undefined) => {
    const text = value === null || value === undefined ? "" : String(value);
    if (/[",\r\n]/.test(text)) {
      return `"${text.replace(/"/g, "\"\"")}"`;
    }
    return text;
  };

  const handleRunControlTma = async () => {
    try {
      setRunningControlTma(true);
      setControlTmaError("");
      setControlTmaMessage("");
      const runResult = await runCrpTmaReportCleanData();
      setControlTmaMessage(
        `Run successful. Run ID: ${runResult.report_run_id}, Row Count: ${runResult.row_count}`
      );
    } catch (error) {
      console.error(error);
      setControlTmaError(error instanceof Error ? error.message : "Failed to run Control TMA.");
    } finally {
      setRunningControlTma(false);
    }
  };

  const handleShowControlTma = async () => {
    try {
      setRunningControlTma(true);
      setControlTmaError("");
      const latestResult = await getLatestCrpTmaReportCleanData();
      setControlTmaRun(latestResult.run);
      setControlTmaRows(latestResult.rows);
      setShowControlTmaPanel(true);
    } catch (error) {
      console.error(error);
      setControlTmaRun(null);
      setControlTmaRows([]);
      setControlTmaError(error instanceof Error ? error.message : "Failed to show latest Control TMA.");
    } finally {
      setRunningControlTma(false);
    }
  };

  const handleDownloadControlTma = async () => {
    try {
      setDownloadingControlTma(true);
      setControlTmaError("");
      let runForDownload = controlTmaRun;
      let rowsForDownload = controlTmaRows;

      if (!runForDownload || rowsForDownload.length === 0) {
        const latestResult = await getLatestCrpTmaReportCleanData();
        runForDownload = latestResult.run;
        rowsForDownload = latestResult.rows;
        setControlTmaRun(latestResult.run);
        setControlTmaRows(latestResult.rows);
      }

      if (rowsForDownload.length === 0) {
        throw new Error("No row data found for Control TMA.");
      }

      const csvContent = [
        controlTmaColumnKeys.join(","),
        ...rowsForDownload.map((row) =>
          controlTmaColumnKeys
            .map((column) => toCsvCell(row[column as keyof CrpTmaReportRow]))
            .join(",")
        ),
      ].join("\r\n");
      const blob = new Blob(["\uFEFF", csvContent], { type: "text/csv;charset=utf-8;" });
      const objectUrl = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = objectUrl;
      link.download = `control_tma_${runForDownload?.id ?? "latest"}.csv`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(objectUrl);
    } catch (error) {
      console.error(error);
      setControlTmaError(error instanceof Error ? error.message : "Failed to download Control TMA.");
    } finally {
      setDownloadingControlTma(false);
    }
  };

  const handleRunControlSal = async () => {
    try {
      setRunningControlSal(true);
      setControlSalError("");
      setControlSalMessage("");
      const runResult = await runCrpSalReportCleanData();
      setControlSalMessage(
        `Run successful. Run ID: ${runResult.report_run_id}, Row Count: ${runResult.row_count}`
      );
    } catch (error) {
      console.error(error);
      setControlSalError(error instanceof Error ? error.message : "Failed to run Control Volvo SAL.");
    } finally {
      setRunningControlSal(false);
    }
  };

  const handleShowControlSal = async () => {
    try {
      setRunningControlSal(true);
      setControlSalError("");
      const latestResult = await getLatestCrpSalReportCleanData();
      setControlSalRun(latestResult.run);
      setControlSalRows(latestResult.rows);
      setShowControlSalPanel(true);
    } catch (error) {
      console.error(error);
      setControlSalRun(null);
      setControlSalRows([]);
      setControlSalError(error instanceof Error ? error.message : "Failed to show latest Control Volvo SAL.");
    } finally {
      setRunningControlSal(false);
    }
  };

  const handleDownloadControlSal = async () => {
    try {
      setDownloadingControlSal(true);
      setControlSalError("");
      let runForDownload = controlSalRun;
      let rowsForDownload = controlSalRows;

      if (!runForDownload || rowsForDownload.length === 0) {
        const latestResult = await getLatestCrpSalReportCleanData();
        runForDownload = latestResult.run;
        rowsForDownload = latestResult.rows;
        setControlSalRun(latestResult.run);
        setControlSalRows(latestResult.rows);
      }

      if (rowsForDownload.length === 0) {
        throw new Error("No row data found for Control Volvo SAL.");
      }

      const csvContent = [
        controlSalColumnKeys.join(","),
        ...rowsForDownload.map((row) =>
          controlSalColumnKeys
            .map((column) => toCsvCell(row[column as keyof CrpSalReportRow]))
            .join(",")
        ),
      ].join("\r\n");
      const blob = new Blob(["\uFEFF", csvContent], { type: "text/csv;charset=utf-8;" });
      const objectUrl = URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = objectUrl;
      link.download = `control_volvo_sal_${runForDownload?.id ?? "latest"}.csv`;
      document.body.appendChild(link);
      link.click();
      link.remove();
      URL.revokeObjectURL(objectUrl);
    } catch (error) {
      console.error(error);
      setControlSalError(error instanceof Error ? error.message : "Failed to download Control Volvo SAL.");
    } finally {
      setDownloadingControlSal(false);
    }
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
          <UploadForm label="volvo_sale_data" title="Volvo Sale Data CSV" compact />
          <UploadForm label="tma_data" title="TMA Data CSV" compact />
        </div>

        <div style={{ marginTop: "16px", display: "flex", gap: "8px", flexWrap: "wrap" }}>
          <button type="button" onClick={handleRunControlTma} disabled={runningControlTma}>
            {runningControlTma ? "Running..." : "Run Control TMA"}
          </button>
          <button type="button" onClick={handleShowControlTma}>
            Show Control TMA
          </button>
          <button type="button" onClick={handleDownloadControlTma} disabled={downloadingControlTma}>
            {downloadingControlTma ? "Downloading..." : "Download Control TMA"}
          </button>
        </div>

        {runningControlTma ? <p style={{ color: "blue" }}>Processing Control TMA...</p> : null}
        {controlTmaMessage ? <p style={{ color: "green" }}>{controlTmaMessage}</p> : null}
        {controlTmaError ? <p style={{ color: "red" }}>Error: {controlTmaError}</p> : null}

        {showControlTmaPanel && controlTmaRun ? (
          <div className="section summary-card" style={{ marginTop: "16px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <strong>Control TMA</strong>
              <button
                type="button"
                onClick={() => {
                  setShowControlTmaPanel(false);
                  setControlTmaRun(null);
                  setControlTmaRows([]);
                }}
                aria-label="Close Control TMA"
                title="Close"
              >
                x
              </button>
            </div>
            <div className="summary-row">
              <span className="summary-label">TMA Upload ID</span>
              <span className="summary-value">{controlTmaRun.tma_upload_run_id}</span>
            </div>
            <div className="summary-row">
              <span className="summary-label">Source Matrix Upload ID</span>
              <span className="summary-value">{controlTmaRun.source_matrix_upload_run_id ?? "-"}</span>
            </div>
            <FilterableTable
              columns={controlTmaColumnKeys.map((column) => ({ key: column, label: column }))}
              rows={controlTmaRows}
              maxHeight="420px"
              compact
            />
          </div>
        ) : null}

        <div style={{ marginTop: "16px", display: "flex", gap: "8px", flexWrap: "wrap" }}>
          <button type="button" onClick={handleRunControlSal} disabled={runningControlSal}>
            {runningControlSal ? "Running..." : "Run Control Volvo SAL"}
          </button>
          <button type="button" onClick={handleShowControlSal}>
            Show Control Volvo SAL
          </button>
          <button type="button" onClick={handleDownloadControlSal} disabled={downloadingControlSal}>
            {downloadingControlSal ? "Downloading..." : "Download Control Volvo SAL"}
          </button>
        </div>

        {runningControlSal ? <p style={{ color: "blue" }}>Processing Control Volvo SAL...</p> : null}
        {controlSalMessage ? <p style={{ color: "green" }}>{controlSalMessage}</p> : null}
        {controlSalError ? <p style={{ color: "red" }}>Error: {controlSalError}</p> : null}

        {showControlSalPanel && controlSalRun ? (
          <div className="section summary-card" style={{ marginTop: "16px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
              <strong>Control Volvo SAL</strong>
              <button
                type="button"
                onClick={() => {
                  setShowControlSalPanel(false);
                  setControlSalRun(null);
                  setControlSalRows([]);
                }}
                aria-label="Close Control Volvo SAL"
                title="Close"
              >
                x
              </button>
            </div>
            <div className="summary-row">
              <span className="summary-label">Volvo SAL Upload ID</span>
              <span className="summary-value">{controlSalRun.volvo_upload_run_id}</span>
            </div>
            <div className="summary-row">
              <span className="summary-label">Source Matrix Upload ID</span>
              <span className="summary-value">{controlSalRun.source_matrix_upload_run_id ?? "-"}</span>
            </div>
            <FilterableTable
              columns={controlSalColumnKeys.map((column) => ({ key: column, label: column }))}
              rows={controlSalRows}
              maxHeight="420px"
              compact
            />
          </div>
        ) : null}

        <div className="crp-columns">
          <h3 className="crp-columns__title">Required Columns</h3>
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
      </section>
    </div>
  );
}

export default CrpUploadPage;
