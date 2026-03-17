import type {
  LatestUploadResponse,
  UploadCompletenessResponse,
  UploadCsvResponse,
  UploadRun,
} from "../types/upload";

const API_BASE_URL = "http://127.0.0.1:8001";

export async function uploadCsv(matrixType: string, file: File): Promise<UploadCsvResponse> {
  const formData = new FormData();
  formData.append("matrix_type", matrixType);
  formData.append("file", file);

  const response = await fetch(`${API_BASE_URL}/uploads/csv`, {
    method: "POST",
    body: formData,
  });

  const result = await response.json();
  if (!response.ok) {
    throw new Error(result.detail || "Upload failed");
  }

  return result as UploadCsvResponse;
}

export async function getUpload(uploadRunId: number): Promise<UploadRun> {
  const response = await fetch(`${API_BASE_URL}/uploads/${uploadRunId}`);
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch upload result");
  }

  return result as UploadRun;
}

export async function getLatestUploadByMatrixType(matrixType: string): Promise<LatestUploadResponse> {
  const response = await fetch(`${API_BASE_URL}/uploads/latest/${encodeURIComponent(matrixType)}`);
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch latest upload");
  }

  return result as LatestUploadResponse;
}

export async function getUploadCompleteness(): Promise<UploadCompletenessResponse> {
  const response = await fetch(`${API_BASE_URL}/uploads/completeness`);
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to check upload completeness");
  }

  return result as UploadCompletenessResponse;
}
