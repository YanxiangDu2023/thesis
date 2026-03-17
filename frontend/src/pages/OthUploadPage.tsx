import { useMemo, useState } from "react";
import UploadForm from "../components/upload/UploadForm";
import { getUploadCompleteness } from "../api/uploads";
import type { UploadCompletenessResponse } from "../types/upload";

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

  const orderedItems = useMemo(() => result?.items ?? [], [result]);

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

  return (
    <div className="page-container">
      <h1>Upload OTH Data</h1>
      <p>Upload the CSV file required for OTH data configuration.</p>

      <div className="matrix-form">
        <UploadForm label="oth_data" title="OTH Data CSV" />
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

          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Uploaded</th>
                  <th>Latest Success Upload ID</th>
                  <th>Latest Success Time</th>
                </tr>
              </thead>
              <tbody>
                {orderedItems.map((item) => (
                  <tr key={item.matrix_type}>
                    <td>{MATRIX_TYPE_LABELS[item.matrix_type] ?? item.matrix_type}</td>
                    <td>{item.uploaded ? "Yes" : "No"}</td>
                    <td>{item.latest_success_upload?.id ?? "-"}</td>
                    <td>{item.latest_success_upload?.uploaded_at ?? "-"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

export default OthUploadPage;
