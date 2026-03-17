import { useEffect, useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";
import { getUpload } from "../api/uploads";
import type { UploadCsvResponse, UploadRun } from "../types/upload";

type UploadResultState = UploadCsvResponse | null;

function UploadResultPage() {
  const { uploadRunId } = useParams();
  const location = useLocation();
  const state = (location.state as UploadResultState) ?? null;

  const [uploadRun, setUploadRun] = useState<UploadRun | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(!state);

  useEffect(() => {
    if (!uploadRunId) {
      setError("Upload ID is missing.");
      setLoading(false);
      return;
    }

    if (state) {
      setUploadRun({
        id: state.upload_run_id,
        matrix_type: state.matrix_type,
        original_file_name: state.original_file_name,
        stored_file_name: "",
        stored_path: "",
        uploaded_at: "",
        row_count: state.row_count,
        status: state.status,
        message: state.message,
      });
      setLoading(false);
      return;
    }

    const fetchUpload = async () => {
      try {
        const result = await getUpload(Number(uploadRunId));
        setUploadRun(result);
      } catch (fetchError) {
        console.error(fetchError);
        setError("Failed to load upload result.");
      } finally {
        setLoading(false);
      }
    };

    void fetchUpload();
  }, [uploadRunId, state]);

  if (loading) {
    return (
      <div className="page-container">
        <h1>Upload Result</h1>
        <p>Loading upload result...</p>
      </div>
    );
  }

  if (error || !uploadRun) {
    return (
      <div className="page-container">
        <h1>Upload Result</h1>
        <p>{error || "Upload result is not available."}</p>
        <Link to="/matrix">Back to Matrix Submission</Link>
      </div>
    );
  }

  return (
    <div className="page-container">
      <h1>Upload Result</h1>
      <p>The CSV file has been processed. Details are shown below.</p>

      <div className="section summary-card">
        <div className="summary-row">
          <span className="summary-label">Upload ID</span>
          <span className="summary-value">{uploadRun.id}</span>
        </div>
        <div className="summary-row">
          <span className="summary-label">Matrix Type</span>
          <span className="summary-value">{uploadRun.matrix_type}</span>
        </div>
        <div className="summary-row">
          <span className="summary-label">File Name</span>
          <span className="summary-value">{uploadRun.original_file_name}</span>
        </div>
        <div className="summary-row">
          <span className="summary-label">Row Count</span>
          <span className="summary-value">{uploadRun.row_count ?? 0}</span>
        </div>
        <div className="summary-row">
          <span className="summary-label">Status</span>
          <span className="summary-value">{uploadRun.status ?? "unknown"}</span>
        </div>
        <div className="summary-row">
          <span className="summary-label">Message</span>
          <span className="summary-value">{uploadRun.message ?? "-"}</span>
        </div>
      </div>

      <Link to="/matrix" className="btn btn--primary">
        Back to Matrix Submission
      </Link>
    </div>
  );
}

export default UploadResultPage;
