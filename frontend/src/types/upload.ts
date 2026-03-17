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

export type ControlReportCleanRun = {
  id: number;
  oth_upload_run_id: number;
  group_country_upload_run_id: number;
  machine_line_mapping_upload_run_id: number;
  brand_mapping_upload_run_id: number;
  created_at: string;
  row_count: number | null;
  status: string | null;
  message: string | null;
};

export type ControlReportCleanRow = {
  id: number;
  control_run_id: number;
  row_index: number;
  year: string;
  source: string;
  country_code: string;
  country: string;
  country_grouping: string;
  region: string;
  market_area: string;
  machine_line_name: string;
  machine_line_code: string;
  brand_name: string;
  brand_code: string;
  size_class_flag: string;
  fid: string;
  ms_percent: string | null;
};

export type RunControlReportCleanDataResponse = {
  message: string;
  control_run_id: number;
  row_count: number;
};

export type LatestControlReportCleanDataResponse = {
  run: ControlReportCleanRun;
  rows: ControlReportCleanRow[];
};
