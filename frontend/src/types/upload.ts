export type UploadStatus = "idle" | "uploading" | "success" | "error";

export type UploadCsvResponse = {
  message: string;
  upload_run_id: number;
  row_count: number;
  matrix_type: string;
  original_file_name: string;
  status: "success";
};

export type SaveEditedUploadResponse = UploadCsvResponse;

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

export type CrpTmaReportRun = {
  id: number;
  tma_upload_run_id: number;
  created_at: string;
  row_count: number | null;
  status: string | null;
  message: string | null;
};

export type CrpTmaReportRow = {
  id: number;
  report_run_id: number;
  row_index: number;
  year: string;
  geographical_region: string;
  geographical_market_area: string;
  end_country_code: string;
  country: string;
  machine_line: string;
  machine_line_code: string;
  size_class_mapping: string;
  fid_sum: number | null;
  source: string;
};

export type RunCrpTmaReportCleanDataResponse = {
  message: string;
  report_run_id: number;
  row_count: number;
};

export type LatestCrpTmaReportCleanDataResponse = {
  run: CrpTmaReportRun;
  rows: CrpTmaReportRow[];
};

export type CrpD1CombinedReportRow = {
  year: string;
  country_group_code: string;
  country_grouping: string;
  country: string;
  region: string;
  machine_line_code: string;
  machine_line_name: string;
  size_class: string;
  artificial_machine_line: string;
  brand_code: string;
  reporter_flag: string;
  pri_sec: string;
  source: string;
  deletion_flag: string;
  fid: number | null;
  tm: number | null;
  vce_fid: number | null;
  tm_non_vce: number | null;
};

export type CrpD1CombinedReportResponse = {
  row_count: number;
  rows: CrpD1CombinedReportRow[];
  tma_upload_run_id: number;
  volvo_upload_run_id: number;
  group_country_upload_run_id: number;
  source_matrix_upload_run_id: number;
  machine_line_mapping_upload_run_id: number;
};

export type A10AdjustmentRow = {
  year: string;
  country_group_code: string;
  country_grouping: string;
  country: string;
  region: string;
  machine_line_code: string;
  machine_line_name: string;
  artificial_machine_line: string;
  size_class: string;
  brand_code: string;
  reporter_flag: string;
  vce_flag: string;
  source: string;
  pri_sec: string;
  calculation_step: string;
  fid: number | null;
  tm_fid: number | null;
  tm_non_vce: number | null;
};

export type A10AdjustmentResponse = {
  row_count: number;
  rows: A10AdjustmentRow[];
  tma_upload_run_id: number;
  volvo_upload_run_id: number;
  group_country_upload_run_id: number;
  source_matrix_upload_run_id: number;
};

export type OthDeletionFlagRow = {
  year: string;
  source: string;
  country_code: string;
  country: string;
  country_grouping: string;
  region: string;
  market_area: string;
  machine_line_name: string;
  machine_line_code: string;
  artificial_machine_line: string;
  brand_name: string;
  brand_code: string;
  size_class_flag: string;
  fid: string | number | null;
  ms_percent: string | null;
  deletion_flag: string;
  pri_sec: string;
  reporter_flag: string;
};

export type OthDeletionFlagResponse = {
  row_count: number;
  rows: OthDeletionFlagRow[];
  oth_upload_run_id: number;
  group_country_upload_run_id: number;
  machine_line_mapping_upload_run_id: number;
  brand_mapping_upload_run_id: number;
  source_matrix_upload_run_id: number;
  reporter_list_upload_run_id: number;
};

export type P00ThreeCheckRow = {
  year: string;
  country: string;
  country_grouping: string;
  region: string;
  machine_line_name: string;
  machine_line_code: string;
  artificial_machine_line: string;
  brand_name: string;
  brand_code: string;
  size_class: string;
  source: string;
  fid: number;
  tm: number | "";
  vce_fid: number | "";
  tm_non_vce: number | "";
  reporter_flag: string;
  deletion_flag: string;
  pri_sec: string;
};

export type P00ThreeCheckResponse = {
  row_count: number;
  rows: P00ThreeCheckRow[];
  tma_upload_run_id: number;
  volvo_upload_run_id: number;
  group_country_upload_run_id: number;
  source_matrix_upload_run_id: number;
  machine_line_mapping_upload_run_id: number;
  oth_upload_run_id: number;
};

export type P10VceNonVceRow = {
  year: string;
  country_group_code: string;
  country_grouping: string;
  country: string;
  region: string;
  machine_line_code: string;
  machine_line_name: string;
  artificial_machine_line: string;
  size_class: string;
  total_market: number;
  vce: number;
  non_vce: number;
  vce_share_pct: string;
};

export type P10VceNonVceResponse = {
  row_count: number;
  rows: P10VceNonVceRow[];
  summary: {
    total_market_sum: number;
    vce_sum: number;
    non_vce_sum: number;
  };
  source_row_count: number;
  tma_upload_run_id: number;
  volvo_upload_run_id: number;
  group_country_upload_run_id: number;
  source_matrix_upload_run_id: number;
};
