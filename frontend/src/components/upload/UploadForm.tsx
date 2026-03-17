import { useMemo, useState } from "react";
import { getLatestUploadByMatrixType, uploadCsv } from "../../api/uploads";
import type { UploadRow, UploadRun, UploadStatus } from "../../types/upload";

type UploadFormProps = {
  label: string;
  title: string;
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

  const latestColumns = useMemo(
    () => (latestRows.length > 0 ? Object.keys(latestRows[0]) : []),
    [latestRows]
  );

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
                <div className="table-wrapper" style={{ maxHeight: "360px" }}>
                  <table className="data-table">
                    <thead>
                      <tr>
                        {latestColumns.map((column) => (
                          <th key={column}>{column}</th>
                        ))}
                      </tr>
                    </thead>
                    <tbody>
                      {latestRows.map((row, rowIndex) => (
                        <tr key={`${String(row.id ?? rowIndex)}-${rowIndex}`}>
                          {latestColumns.map((column) => (
                            <td key={`${rowIndex}-${column}`}>{row[column] ?? ""}</td>
                          ))}
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}

export default UploadForm;
