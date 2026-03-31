import type {
  A10AdjustmentResponse,
  CrpD1CombinedReportResponse,
  LatestCrpTmaReportCleanDataResponse,
  LatestControlReportCleanDataResponse,
  LatestUploadResponse,
  OthDeletionFlagRow,
  OthDeletionFlagResponse,
  P00ThreeCheckResponse,
  UploadRow,
  P10VceNonVceResponse,
  RunCrpTmaReportCleanDataResponse,
  RunControlReportCleanDataResponse,
  SaveEditedUploadResponse,
  UploadCompletenessResponse,
  UploadCsvResponse,
  UploadRun,
} from "../types/upload";
import { apiFetch } from "./client";

function toText(value: string | number | null | undefined): string {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value).trim();
}

function toKey(value: string | number | null | undefined): string {
  return toText(value).toUpperCase();
}

function buildOthDeletionFlagRowsFallback(
  othRows: UploadRow[],
  groupCountryRows: UploadRow[],
  machineLineRows: UploadRow[],
  brandRows: UploadRow[],
  sourceMatrixRows: UploadRow[],
  reporterListRows: UploadRow[]
): OthDeletionFlagRow[] {
  const groupCountryByCodeYear = new Map<string, UploadRow>();
  groupCountryRows.forEach((row) => {
    const key = `${toKey(row.country_code)}|${toKey(row.year)}`;
    if (!groupCountryByCodeYear.has(key)) {
      groupCountryByCodeYear.set(key, row);
    }
  });

  const machineLineByNameOrCode = new Map<string, UploadRow>();
  machineLineRows.forEach((row) => {
    const byName = toKey(row.machine_line_name);
    const byCode = toKey(row.machine_line_code);
    if (byName && !machineLineByNameOrCode.has(byName)) {
      machineLineByNameOrCode.set(byName, row);
    }
    if (byCode && !machineLineByNameOrCode.has(byCode)) {
      machineLineByNameOrCode.set(byCode, row);
    }
  });

  const brandByName = new Map<string, UploadRow>();
  brandRows.forEach((row) => {
    const key = toKey(row.brand_name);
    if (key && !brandByName.has(key)) {
      brandByName.set(key, row);
    }
  });

  const sourceMatrixKeys = new Set<string>();
  const sourceMatrixPriSecByKey = new Map<string, string>();
  const sourceMatrixCrpSourcesByKey = new Map<string, Set<string>>();
  sourceMatrixRows.forEach((row) => {
    const country = toKey(row.country_name);
    const machineLine = toKey(row.machine_line_name);
    if (country && machineLine) {
      sourceMatrixKeys.add(`${country}|${machineLine}`);
    }

    const primarySource = toKey(row.primary_source);
    const secondarySource = toKey(row.secondary_source);
    if (country && machineLine && primarySource) {
      sourceMatrixPriSecByKey.set(`${country}|${machineLine}|${primarySource}`, "P");
    }
    if (country && machineLine && secondarySource) {
      const key = `${country}|${machineLine}|${secondarySource}`;
      if (!sourceMatrixPriSecByKey.has(key)) {
        sourceMatrixPriSecByKey.set(key, "S");
      }
    }

    const crpSource = toKey(row.crp_source);
    if (country && machineLine && crpSource) {
      const key = `${country}|${machineLine}`;
      if (!sourceMatrixCrpSourcesByKey.has(key)) {
        sourceMatrixCrpSourcesByKey.set(key, new Set<string>());
      }
      sourceMatrixCrpSourcesByKey.get(key)?.add(crpSource);
    }
  });

  const reporterListKeys = new Set<string>();
  reporterListRows.forEach((row) => {
    const sourceCode = toKey(row.source_code);
    const machineLine = toKey(row.machine_line);
    const brandCode = toKey(row.brand_code);
    if (sourceCode && machineLine && brandCode) {
      reporterListKeys.add(`${sourceCode}|${machineLine}|${brandCode}`);
    }
  });

  const sortedOth = [...othRows].sort((a, b) => {
    const ia = Number(toText(a.row_index) || "0");
    const ib = Number(toText(b.row_index) || "0");
    return ia - ib;
  });

  return sortedOth.map((o) => {
    const groupKey = `${toKey(o.country)}|${toKey(o.year)}`;
    const g = groupCountryByCodeYear.get(groupKey);
    const m = machineLineByNameOrCode.get(toKey(o.machine_line));
    const b = brandByName.get(toKey(o.brand_name));

    const country = toText(g?.country_name) || toText(o.country);
    const machineLineName = toText(m?.machine_line_name) || toText(o.machine_line);
    const machineLineCode = toText(m?.machine_line_code);

    const sourceMatrixKey = `${toKey(country)}|${toKey(machineLineName)}`;
    const sourcePriSecKey = `${toKey(country)}|${toKey(machineLineName)}|${toKey(o.source)}`;
    const sourceMatrixCrpKey = `${toKey(country)}|${toKey(machineLineName)}`;
    let deletionFlag = "";
    if (toText(machineLineCode) === "390") {
      deletionFlag = "Y";
    } else if (country && machineLineName && !sourceMatrixKeys.has(sourceMatrixKey)) {
      deletionFlag = "Y";
    }

    let reporterFlag = "";
    const crpSources = sourceMatrixCrpSourcesByKey.get(sourceMatrixCrpKey);
    const brandCode = toKey(b?.brand_code);
    if (crpSources && brandCode) {
      for (const crpSource of crpSources) {
        const reporterKey = `${crpSource}|${toKey(machineLineName)}|${brandCode}`;
        if (reporterListKeys.has(reporterKey)) {
          reporterFlag = "Y";
          break;
        }
      }
    }

    return {
      year: toText(o.year),
      source: toText(o.source),
      country_code: toText(o.country),
      country,
      country_grouping: toText(g?.country_grouping),
      region: toText(g?.region),
      market_area: toText(g?.market_area),
      machine_line_name: machineLineName,
      machine_line_code: machineLineCode,
      artificial_machine_line: toText(m?.artificial_machine_line),
      brand_name: toText(b?.brand_name) || toText(o.brand_name),
      brand_code: toText(b?.brand_code),
      size_class_flag: toText(o.size_class),
      fid: toText(o.quantity),
      ms_percent: null,
      deletion_flag: deletionFlag,
      pri_sec: sourceMatrixPriSecByKey.get(sourcePriSecKey) ?? "",
      reporter_flag: reporterFlag,
    };
  });
}

async function getOthDeletionFlagReportFallback(): Promise<OthDeletionFlagResponse> {
  const [oth, groupCountry, machineLine, brand, sourceMatrix, reporterList] = await Promise.all([
    getLatestUploadByMatrixType("oth_data"),
    getLatestUploadByMatrixType("group_country"),
    getLatestUploadByMatrixType("machine_line_mapping"),
    getLatestUploadByMatrixType("brand_mapping"),
    getLatestUploadByMatrixType("source_matrix"),
    getLatestUploadByMatrixType("reporter_list"),
  ]);

  const rows = buildOthDeletionFlagRowsFallback(
    oth.rows,
    groupCountry.rows,
    machineLine.rows,
    brand.rows,
    sourceMatrix.rows,
    reporterList.rows
  );

  return {
    row_count: rows.length,
    rows,
    oth_upload_run_id: oth.upload_run.id,
    group_country_upload_run_id: groupCountry.upload_run.id,
    machine_line_mapping_upload_run_id: machineLine.upload_run.id,
    brand_mapping_upload_run_id: brand.upload_run.id,
    source_matrix_upload_run_id: sourceMatrix.upload_run.id,
    reporter_list_upload_run_id: reporterList.upload_run.id,
  };
}

export async function uploadCsv(matrixType: string, file: File): Promise<UploadCsvResponse> {
  const formData = new FormData();
  formData.append("matrix_type", matrixType);
  formData.append("file", file);

  let response: Response;
  try {
    response = await apiFetch("/uploads/csv", {
      method: "POST",
      body: formData,
    });
  } catch (error) {
    throw new Error(
      "Failed to reach upload API. Please re-select the file and try again. " 
    );
  }

  const result = await response.json();
  if (!response.ok) {
    throw new Error(result.detail || "Upload failed");
  }

  return result as UploadCsvResponse;
}

export async function saveEditedUpload(
  matrixType: string,
  rows: UploadRow[],
  sourceUploadRunId?: number
): Promise<SaveEditedUploadResponse> {
  const response = await apiFetch("/uploads/save-edited", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      matrix_type: matrixType,
      rows,
      source_upload_run_id: sourceUploadRunId ?? null,
    }),
  });
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to save edited rows");
  }

  return result as SaveEditedUploadResponse;
}

export async function getUpload(uploadRunId: number): Promise<UploadRun> {
  const response = await apiFetch(`/uploads/${uploadRunId}`);
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch upload result");
  }

  return result as UploadRun;
}

export async function getLatestUploadByMatrixType(matrixType: string): Promise<LatestUploadResponse> {
  const response = await apiFetch(`/uploads/latest/${encodeURIComponent(matrixType)}`);
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch latest upload");
  }

  return result as LatestUploadResponse;
}

export async function getUploadCompleteness(): Promise<UploadCompletenessResponse> {
  const response = await apiFetch("/uploads/completeness");
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to check upload completeness");
  }

  return result as UploadCompletenessResponse;
}

export async function runControlReportCleanData(): Promise<RunControlReportCleanDataResponse> {
  const response = await apiFetch("/reports/control-report-clean-data/run", {
    method: "POST",
  });
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to run Control Report - Clean Data");
  }

  return result as RunControlReportCleanDataResponse;
}

export async function getLatestControlReportCleanData(): Promise<LatestControlReportCleanDataResponse> {
  const response = await apiFetch("/reports/control-report-clean-data/latest");
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch latest Control Report - Clean Data");
  }

  return result as LatestControlReportCleanDataResponse;
}

export async function runCrpTmaReportCleanData(): Promise<RunCrpTmaReportCleanDataResponse> {
  const response = await apiFetch("/reports/crp-tma-clean-data/run", {
    method: "POST",
  });
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to run CRP TMA Report - Clean Data");
  }

  return result as RunCrpTmaReportCleanDataResponse;
}

export async function getLatestCrpTmaReportCleanData(): Promise<LatestCrpTmaReportCleanDataResponse> {
  const response = await apiFetch("/reports/crp-tma-clean-data/latest");
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch latest CRP TMA Report - Clean Data");
  }

  return result as LatestCrpTmaReportCleanDataResponse;
}

export async function getCrpD1CombinedReport(): Promise<CrpD1CombinedReportResponse> {
  const response = await apiFetch("/reports/crp-d1-combined");
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch CRP D1 Combined Report");
  }

  return result as CrpD1CombinedReportResponse;
}

export async function getA10AdjustmentReport(): Promise<A10AdjustmentResponse> {
  const response = await apiFetch("/reports/a10-adjustment");
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch A10 Adjustment Report");
  }

  return result as A10AdjustmentResponse;
}

export async function getOthDeletionFlagReport(): Promise<OthDeletionFlagResponse> {
  const response = await apiFetch("/reports/oth-deletion-flag");
  const result = await response.json();

  if (!response.ok) {
    if (response.status === 404) {
      return await getOthDeletionFlagReportFallback();
    }
    throw new Error(result.detail || "Failed to fetch OTH Deletion Flag Report");
  }

  return result as OthDeletionFlagResponse;
}

export async function getP10VceNonVceReport(): Promise<P10VceNonVceResponse> {
  const response = await apiFetch("/reports/p10-vce-non-vce");
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch P10 VCE / Non-VCE Report");
  }

  return result as P10VceNonVceResponse;
}

export async function getP00ThreeCheckReport(): Promise<P00ThreeCheckResponse> {
  const response = await apiFetch("/reports/p00-three-check");
  const result = await response.json();

  if (!response.ok) {
    throw new Error(result.detail || "Failed to fetch P00 3 Check Report");
  }

  return result as P00ThreeCheckResponse;
}
