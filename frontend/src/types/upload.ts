export type UploadStatus = "idle" | "uploading" | "success" | "error";

export type UploadCsvResponse = {
  message: string;
  upload_run_id: number;
  row_count: number;
  matrix_type: string;
  original_file_name: string;
  status: "success";
};

export type UploadRun = {
  id: number;
  matrix_type: string;
  original_file_name: string;
  stored_file_name: string;
  stored_path: string;
  uploaded_at: string;
  row_count: number | null;
  status: string | null;
  message: string | null;
};

export type UploadRow = Record<string, string | number | null>;

export type LatestUploadResponse = {
  upload_run: UploadRun;
  rows: UploadRow[];
};

export type UploadCompletenessItem = {
  matrix_type: string;
  uploaded: boolean;
  latest_upload: UploadRun | null;
  latest_success_upload: UploadRun | null;
};

export type UploadCompletenessResponse = {
  all_uploaded: boolean;
  missing_types: string[];
  items: UploadCompletenessItem[];
};
