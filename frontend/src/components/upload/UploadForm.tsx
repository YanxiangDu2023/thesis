import { useMemo, useState } from "react";
import { getLatestUploadByMatrixType, uploadCsv } from "../../api/uploads";
import type { UploadRow, UploadRun, UploadStatus } from "../../types/upload";
import FilterableTable from "../table/FilterableTable";

type UploadFormProps = {
  label: string;
  title: string;
};

const PREFERRED_COLUMN_ORDER: Record<string, string[]> = {
  group_country: [
    "year",
    "country_grouping",
    "group_code",
    "country_code",
    "country_name",
    "market_area",
    "market_area_code",
    "region",
    "change_indicator",
  ],
};

function UploadForm({ label, title }: UploadFormProps) {
  const [file, setFile] = useState<File | null>(null);
  const [message, setMessage] = useState("");
  const [status, setStatus] = useState<UploadStatus>("idle");

  const [latestUpload, setLatestUpload] = useState<UploadRun | null>(null);
  const [latestRows, setLatestRows] = useState<UploadRow[]>([]);
  const [latestLoading, setLatestLoading] = useState(false);
  const [latestError, setLatestError] = useState("");
  const [showLatestPanel, setShowLatestPanel] = useState(false);

  const latestColumns = useMemo(() => {
    if (latestRows.length === 0) {
      return [];
    }

    const rowKeys = Object.keys(latestRows[0]);
    const preferredOrder = PREFERRED_COLUMN_ORDER[label] ?? [];
    const orderedPreferred = preferredOrder.filter((column) => rowKeys.includes(column));
    const remainingColumns = rowKeys.filter((column) => !orderedPreferred.includes(column));

    return [...orderedPreferred, ...remainingColumns];
  }, [label, latestRows]);

  const handleUpload = async () => {
    if (!file) {
      alert("Please select a CSV file first.");
      return;
    }

    try {
      setStatus("uploading");
      setMessage("Uploading...");

      const result = await uploadCsv(label, file);
      setStatus("success");
      setMessage(`Upload successful. Upload ID: ${result.upload_run_id}`);
    } catch (error) {
      console.error(error);
      setStatus("error");
      setMessage("Upload failed.");
    }
  };

  const handleShowLatest = async () => {
    try {
      setShowLatestPanel(true);
      setLatestLoading(true);
      setLatestError("");

      const result = await getLatestUploadByMatrixType(label);
      setLatestUpload(result.upload_run);
      setLatestRows(result.rows);
    } catch (error) {
      console.error(error);
      setLatestUpload(null);
      setLatestRows([]);
      setLatestError(error instanceof Error ? error.message : "Failed to load latest upload data.");
    } finally {
      setLatestLoading(false);
    }
  };

  const handleCloseLatest = () => {
    setShowLatestPanel(false);
    setLatestLoading(false);
    setLatestError("");
    setLatestUpload(null);
    setLatestRows([]);
  };

  return (
    <div className="upload-card">
      <h3>{title}</h3>

      <input
        type="file"
        accept=".csv"
        onChange={(e) => {
          const selectedFile = e.target.files?.[0] || null;
          setFile(selectedFile);
          setMessage("");
          setStatus("idle");
        }}
      />

      {file && <p>Selected file: {file.name}</p>}

      <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
        <button type="button" onClick={handleUpload}>
          Upload
        </button>
        <button type="button" onClick={handleShowLatest}>
          Show Latest
        </button>
      </div>

      {status === "uploading" && <p style={{ color: "blue" }}>Uploading...</p>}
      {status === "success" && <p style={{ color: "green" }}>Success: {message}</p>}
      {status === "error" && <p style={{ color: "red" }}>Error: {message}</p>}

      {showLatestPanel && (
        <div style={{ marginTop: "12px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <strong>Latest Data</strong>
            <button type="button" onClick={handleCloseLatest} aria-label="Close latest data" title="Close">
              x
            </button>
          </div>

          {latestLoading && <p style={{ color: "blue" }}>Loading latest upload data...</p>}
          {latestError && <p style={{ color: "red" }}>Error: {latestError}</p>}

          {latestUpload && (
            <>
              <p>
                Latest upload: ID {latestUpload.id}, rows {latestUpload.row_count ?? 0}, status{" "}
                {latestUpload.status ?? "unknown"}
              </p>

              {latestRows.length === 0 ? (
                <p>No row data found in SQL for this upload.</p>
              ) : (
                <FilterableTable
                  columns={latestColumns.map((column) => ({ key: column, label: column }))}
                  rows={latestRows}
                  maxHeight="360px"
                />
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}

export default UploadForm;
