import type {
  CrpD1CombinedReportResponse,
  LatestCrpTmaReportCleanDataResponse,
  LatestControlReportCleanDataResponse,
  LatestUploadResponse,
  RunCrpTmaReportCleanDataResponse,
  RunControlReportCleanDataResponse,
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

export async function runControlReportCleanData(): Promise<RunControlReportCleanDataResponse> {
  const response = await fetch(`${API_BASE_URL}/reports/control-report-clean-data/run`, {
    method: "POST",
  });
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to run Control Report - Clean Data");
  }

  return result as RunControlReportCleanDataResponse;
}

export async function getLatestControlReportCleanData(): Promise<LatestControlReportCleanDataResponse> {
  const response = await fetch(`${API_BASE_URL}/reports/control-report-clean-data/latest`);
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch latest Control Report - Clean Data");
  }

  return result as LatestControlReportCleanDataResponse;
}

export async function runCrpTmaReportCleanData(): Promise<RunCrpTmaReportCleanDataResponse> {
  const response = await fetch(`${API_BASE_URL}/reports/crp-tma-clean-data/run`, {
    method: "POST",
  });
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to run CRP TMA Report - Clean Data");
  }

  return result as RunCrpTmaReportCleanDataResponse;
}

export async function getLatestCrpTmaReportCleanData(): Promise<LatestCrpTmaReportCleanDataResponse> {
  const response = await fetch(`${API_BASE_URL}/reports/crp-tma-clean-data/latest`);
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch latest CRP TMA Report - Clean Data");
  }

  return result as LatestCrpTmaReportCleanDataResponse;
}

export async function getCrpD1CombinedReport(): Promise<CrpD1CombinedReportResponse> {
  const response = await fetch(`${API_BASE_URL}/reports/crp-d1-combined`);
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch CRP D1 Combined Report");
  }

  return result as CrpD1CombinedReportResponse;
}
