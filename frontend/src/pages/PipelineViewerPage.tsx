import { useEffect, useMemo, useState } from "react";
import {
  getA10AdjustmentReport,
  getExcavatorsSplitCexReport,
  getLatestControlReportCleanData,
  getLatestCrpTmaReportCleanData,
  getLatestUploadByMatrixType,
  getP00RunTimes,
  getP00ThreeCheckReport,
  getP10VceNonVceReport,
  getUpload,
  getUploadCompleteness,
} from "../api/uploads";
import type { LatestUploadResponse, UploadCompletenessItem, UploadRun } from "../types/upload";

type ExecutionStatus = "Completed" | "Blocked" | "Pending";
type SignalStatus = "Healthy" | "Warning" | "Review";
type DependencyStatus = "Fresh" | "Aging" | "Missing";

type Dependency = { name: string; updatedAt: string; status: DependencyStatus };
type Check = { label: string; detail: string; status: SignalStatus };
type SampleColumn = { key: string; label: string };
type SampleRow = Record<string, string | number | null>;

type Step = {
  code: string;
  name: string;
  description: string;
  owner: string;
  executionStatus: ExecutionStatus;
  validationStatus: SignalStatus;
  reviewStatus: SignalStatus;
  timeLabel: string;
  timeValue: string;
  timeHint: string;
  issueSummary: string;
  issueHint: string;
  records: number | null;
  deltaVsPrevious: string;
  inputs: Dependency[];
  checks: Check[];
  sampleColumns: SampleColumn[];
  sampleRows: SampleRow[];
};

type AsyncResult<T> = { data: T | null; error: string | null };

const DEFAULT_REQUEST_TIMEOUT_MS = 8000;
const P00_REQUEST_TIMEOUT_MS = 20000;

const MATRIX_TYPE_LABELS: Record<string, string> = {
  tma_data: "TMA upload",
  volvo_sale_data: "Volvo sales upload",
  group_country: "Group country upload",
  source_matrix: "Source matrix upload",
  machine_line_mapping: "Machine-line mapping",
  oth_data: "OTH upload",
  brand_mapping: "Brand mapping",
  reporter_list: "Reporter list",
};

const P00_REQUIRED_TYPES = [
  "tma_data",
  "volvo_sale_data",
  "group_country",
  "source_matrix",
  "machine_line_mapping",
  "oth_data",
] as const;

const P10_REQUIRED_TYPES = [
  "tma_data",
  "volvo_sale_data",
  "group_country",
  "source_matrix",
] as const;

const SPL_REQUIRED_TYPES = [
  "oth_data",
  "source_matrix",
  "machine_line_mapping",
  "tma_data",
  "volvo_sale_data",
] as const;

const EMPTY_SAMPLE_COLUMNS: SampleColumn[] = [];
const EMPTY_SAMPLE_ROWS: SampleRow[] = [];

function normalizeError(error: unknown): string {
  return error instanceof Error && error.message ? error.message : "Unknown error";
}

async function settle<T>(
  fn: () => Promise<T>,
  timeoutMs: number = DEFAULT_REQUEST_TIMEOUT_MS
): Promise<AsyncResult<T>> {
  try {
    const data = await Promise.race<T>([
      fn(),
      new Promise<T>((_, reject) =>
        setTimeout(() => reject(new Error("Request timed out while loading live pipeline status.")), timeoutMs)
      ),
    ]);
    return { data, error: null };
  } catch (error) {
    return { data: null, error: normalizeError(error) };
  }
}

function formatTimestamp(value: string | null | undefined): string {
  if (!value) return "-";
  const normalized = value.replace("T", " ").replace("Z", "");
  const match = normalized.match(/^(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2})/);
  return match ? `${match[1]} ${match[2]}` : normalized;
}

function getLatestTimestamp(values: Array<string | null | undefined>): string | null {
  const validValues = values.filter(
    (value): value is string => typeof value === "string" && /^\d{4}-\d{2}-\d{2}/.test(value)
  );
  return validValues.length ? [...validValues].sort().at(-1) ?? null : null;
}

function formatRowCount(value: number | null): string {
  return value === null ? "Unavailable" : value.toLocaleString();
}

function buildCompletenessMap(items: UploadCompletenessItem[]): Map<string, UploadCompletenessItem> {
  return new Map(items.map((item) => [item.matrix_type, item]));
}

function upsertCompletenessItem(
  completenessMap: Map<string, UploadCompletenessItem>,
  matrixType: string,
  latestUploadResult: AsyncResult<LatestUploadResponse>
) {
  if (!latestUploadResult.data) return;

  const uploadRun = latestUploadResult.data.upload_run;
  const isSuccess = (uploadRun.status ?? "").toLowerCase() === "success";

  completenessMap.set(matrixType, {
    matrix_type: matrixType,
    uploaded: isSuccess,
    latest_upload: uploadRun,
    latest_success_upload: isSuccess ? uploadRun : null,
  });
}

function getMissingTypes(
  completenessMap: Map<string, UploadCompletenessItem>,
  requiredTypes: readonly string[]
): string[] {
  return requiredTypes.filter((type) => !completenessMap.get(type)?.latest_success_upload);
}

function buildUploadDependency(
  matrixType: string,
  completenessMap: Map<string, UploadCompletenessItem>
): Dependency {
  const item = completenessMap.get(matrixType);
  const latestSuccess = item?.latest_success_upload;
  const latestAttempt = item?.latest_upload;
  if (!latestSuccess) {
    return { name: MATRIX_TYPE_LABELS[matrixType] ?? matrixType, updatedAt: "No successful upload yet", status: "Missing" };
  }
  const hasFailedNewerAttempt =
    latestAttempt &&
    latestAttempt.id !== latestSuccess.id &&
    latestAttempt.status &&
    latestAttempt.status.toLowerCase() !== "success";
  return {
    name: MATRIX_TYPE_LABELS[matrixType] ?? matrixType,
    updatedAt: formatTimestamp(latestSuccess.uploaded_at),
    status: hasFailedNewerAttempt ? "Aging" : "Fresh",
  };
}

function buildUploadDependencies(
  requiredTypes: readonly string[],
  completenessMap: Map<string, UploadCompletenessItem>
): Dependency[] {
  return requiredTypes.map((type) => buildUploadDependency(type, completenessMap));
}

function buildReferencedUploadDependencies(
  uploads: Array<{ id: number; name: string }>,
  uploadRuns: Map<number, UploadRun>
): Dependency[] {
  return uploads.map(({ id, name }) => {
    const run = uploadRuns.get(id);
    if (!run) return { name, updatedAt: "Snapshot unavailable", status: "Missing" };
    return {
      name,
      updatedAt: formatTimestamp(run.uploaded_at),
      status: (run.status ?? "").toLowerCase() === "success" || !run.status ? "Fresh" : "Aging",
    };
  });
}

function resolveSignalStatus(checks: Check[]): SignalStatus {
  if (checks.some((check) => check.status === "Review")) return "Review";
  if (checks.some((check) => check.status === "Warning")) return "Warning";
  return "Healthy";
}

function buildIssueSummary(checks: Check[]): { summary: string; hint: string } {
  const reviewCount = checks.filter((check) => check.status === "Review").length;
  const warningCount = checks.filter((check) => check.status === "Warning").length;
  if (reviewCount === 0 && warningCount === 0) {
    return { summary: "Healthy", hint: "No active validation issues detected in the live checks." };
  }
  if (reviewCount > 0) {
    return {
      summary: `${reviewCount} review item${reviewCount > 1 ? "s" : ""}`,
      hint: `${warningCount} warning check${warningCount === 1 ? "" : "s"} also need attention.`,
    };
  }
  return {
    summary: `${warningCount} warning${warningCount > 1 ? "s" : ""}`,
    hint: "Live output exists, but some checks need attention.",
  };
}

function getExecutionBadgeClass(status: ExecutionStatus) {
  return status === "Completed"
    ? "status-badge status-badge--completed"
    : status === "Blocked"
      ? "status-badge status-badge--blocked"
      : "status-badge status-badge--pending";
}

function getSignalBadgeClass(status: SignalStatus) {
  return status === "Healthy"
    ? "status-badge status-badge--healthy"
    : status === "Warning"
      ? "status-badge status-badge--warning"
      : "status-badge status-badge--review";
}

function getDependencyBadgeClass(status: DependencyStatus) {
  return status === "Fresh"
    ? "status-badge status-badge--healthy"
    : status === "Aging"
      ? "status-badge status-badge--warning"
      : "status-badge status-badge--blocked";
}

function buildStaticPendingStep(config: {
  code: string;
  name: string;
  description: string;
  owner: string;
  issueDetail: string;
  inputs: Dependency[];
}): Step {
  const checks: Check[] = [{ label: "Live status coverage", detail: config.issueDetail, status: "Review" }];
  return {
    code: config.code,
    name: config.name,
    description: config.description,
    owner: config.owner,
    executionStatus: "Pending",
    validationStatus: "Review",
    reviewStatus: "Review",
    timeLabel: "Last run",
    timeValue: "Unavailable",
    timeHint: "No persisted live run metadata is available for this step yet.",
    issueSummary: "Live endpoint missing",
    issueHint: config.issueDetail,
    records: null,
    deltaVsPrevious: "No live output captured yet.",
    inputs: config.inputs,
    checks,
    sampleColumns: EMPTY_SAMPLE_COLUMNS,
    sampleRows: EMPTY_SAMPLE_ROWS,
  };
}

function resolveExecutionStatus(hasMissingDependencies: boolean, hasLiveOutput: boolean): ExecutionStatus {
  if (hasMissingDependencies) {
    return "Blocked";
  }
  return hasLiveOutput ? "Completed" : "Pending";
}

function buildInitialSteps(): Step[] {
  const baseInputs = [{ name: "Live status", updatedAt: "Loading...", status: "Aging" as const }];

  return [
    buildStaticPendingStep({
      code: "P00",
      name: "Preparation Raw Layer",
      description: "Loading live preparation status.",
      owner: "Preparation inputs",
      issueDetail: "Waiting for live pipeline status.",
      inputs: baseInputs,
    }),
    buildStaticPendingStep({
      code: "P10",
      name: "Prepared Layer",
      description: "Loading live prepared-layer status.",
      owner: "Core TMC logic",
      issueDetail: "Waiting for live pipeline status.",
      inputs: baseInputs,
    }),
    buildStaticPendingStep({
      code: "A10",
      name: "Adjustment Layer",
      description: "Loading live adjustment status.",
      owner: "Adjustment logic",
      issueDetail: "Waiting for live pipeline status.",
      inputs: baseInputs,
    }),
    buildStaticPendingStep({
      code: "SPL",
      name: "Machine Line Split",
      description: "Loading live split status.",
      owner: "Split logic",
      issueDetail: "Waiting for live pipeline status.",
      inputs: baseInputs,
    }),
    buildStaticPendingStep({
      code: "RES",
      name: "Restatement",
      description: "Loading live restatement status.",
      owner: "Restatement logic",
      issueDetail: "Waiting for live pipeline status.",
      inputs: baseInputs,
    }),
    buildStaticPendingStep({
      code: "RPT",
      name: "Reporting",
      description: "Loading live reporting status.",
      owner: "Reporting output",
      issueDetail: "Waiting for live pipeline status.",
      inputs: baseInputs,
    }),
  ];
}

function PipelineViewerPage() {
  const [steps, setSteps] = useState<Step[]>(() => buildInitialSteps());
  const [selectedStepCode, setSelectedStepCode] = useState("P00");
  const [loading, setLoading] = useState(true);
  const [pageError, setPageError] = useState("");

  useEffect(() => {
    let isActive = true;

    async function loadPipelineHealth() {
      setLoading(true);
      setPageError("");
      try {
        const [
          completenessResult,
          latestTmaUploadResult,
          latestVolvoUploadResult,
          p00Result,
          p10Result,
          a10Result,
          splitResult,
          controlRunResult,
          crpRunResult,
          p00RunTimesResult,
        ] =
          await Promise.all([
            settle(() => getUploadCompleteness()),
            settle(() => getLatestUploadByMatrixType("tma_data")),
            settle(() => getLatestUploadByMatrixType("volvo_sale_data")),
            settle(() => getP00ThreeCheckReport(), P00_REQUEST_TIMEOUT_MS),
            settle(() => getP10VceNonVceReport()),
            settle(() => getA10AdjustmentReport()),
            settle(() => getExcavatorsSplitCexReport()),
            settle(() => getLatestControlReportCleanData()),
            settle(() => getLatestCrpTmaReportCleanData()),
            settle(() => getP00RunTimes()),
          ]);

        const completenessMap = buildCompletenessMap(completenessResult.data?.items ?? []);
        upsertCompletenessItem(completenessMap, "tma_data", latestTmaUploadResult);
        upsertCompletenessItem(completenessMap, "volvo_sale_data", latestVolvoUploadResult);
        const referencedUploadIds = new Set<number>();

      if (p00Result.data) {
        [
          p00Result.data.tma_upload_run_id,
          p00Result.data.volvo_upload_run_id,
          p00Result.data.group_country_upload_run_id,
          p00Result.data.source_matrix_upload_run_id,
          p00Result.data.machine_line_mapping_upload_run_id,
          p00Result.data.oth_upload_run_id,
        ].forEach((id) => referencedUploadIds.add(id));
      }
      if (p10Result.data) {
        [
          p10Result.data.tma_upload_run_id,
          p10Result.data.volvo_upload_run_id,
          p10Result.data.group_country_upload_run_id,
          p10Result.data.source_matrix_upload_run_id,
        ].forEach((id) => referencedUploadIds.add(id));
      }
      if (a10Result.data) {
        [
          a10Result.data.tma_upload_run_id,
          a10Result.data.volvo_upload_run_id,
          a10Result.data.group_country_upload_run_id,
          a10Result.data.source_matrix_upload_run_id,
        ].forEach((id) => referencedUploadIds.add(id));
      }

      const uploadRunMap = new Map<number, UploadRun>();
      await Promise.all(
        [...referencedUploadIds].map(async (uploadRunId) => {
          try {
            uploadRunMap.set(uploadRunId, await getUpload(uploadRunId));
          } catch {
            // Keep step rendering alive even if one upload lookup fails.
          }
        })
      );

      const builtSteps: Step[] = [];

      const p00Inputs = p00Result.data
        ? buildReferencedUploadDependencies(
            [
              { id: p00Result.data.tma_upload_run_id, name: "TMA upload" },
              { id: p00Result.data.volvo_upload_run_id, name: "Volvo sales upload" },
              { id: p00Result.data.group_country_upload_run_id, name: "Group country upload" },
              { id: p00Result.data.source_matrix_upload_run_id, name: "Source matrix upload" },
              { id: p00Result.data.machine_line_mapping_upload_run_id, name: "Machine-line mapping" },
              { id: p00Result.data.oth_upload_run_id, name: "OTH upload" },
            ],
            uploadRunMap
          )
        : buildUploadDependencies(P00_REQUIRED_TYPES, completenessMap);
      const p00Missing = getMissingTypes(completenessMap, P00_REQUIRED_TYPES);
      const p00Checks: Check[] = [];

      p00Checks.push(
        p00Missing.length
          ? {
              label: "Required uploads",
              detail: `Missing latest successful upload for: ${p00Missing
                .map((type) => MATRIX_TYPE_LABELS[type] ?? type)
                .join(", ")}.`,
              status: "Review",
            }
          : { label: "Required uploads", detail: "All live uploads required for P00 are available.", status: "Healthy" }
      );

      if (p00Result.data) {
        const mappingGapCount = p00Result.data.rows.filter(
          (row) => !String(row.country_grouping ?? "").trim() || !String(row.machine_line_code ?? "").trim()
        ).length;
        const blankBrandCount = p00Result.data.rows.filter((row) => !String(row.brand_code ?? "").trim()).length;
        p00Checks.push({
          label: "Mapping completeness",
          detail:
            mappingGapCount === 0
              ? "Country grouping and machine-line mappings are populated in the live P00 sample."
              : `${mappingGapCount} live rows are missing country grouping or machine-line mapping.`,
          status: mappingGapCount === 0 ? "Healthy" : "Warning",
        });
        p00Checks.push({
          label: "Brand coverage",
          detail:
            blankBrandCount === 0
              ? "All sampled P00 rows contain brand codes."
              : `${blankBrandCount} sampled P00 rows are missing brand codes.`,
          status: blankBrandCount === 0 ? "Healthy" : "Warning",
        });
      }

      if (controlRunResult.data) {
        p00Checks.push({
          label: "OTH clean run",
          detail: `Latest clean run at ${formatTimestamp(controlRunResult.data.run.created_at)} with ${controlRunResult.data.run.row_count ?? 0} rows.`,
          status: (controlRunResult.data.run.status ?? "").toLowerCase() === "success" ? "Healthy" : "Warning",
        });
      }
      if (crpRunResult.data) {
        p00Checks.push({
          label: "CRP clean run",
          detail: `Latest clean run at ${formatTimestamp(crpRunResult.data.run.created_at)} with ${crpRunResult.data.run.row_count ?? 0} rows.`,
          status: (crpRunResult.data.run.status ?? "").toLowerCase() === "success" ? "Healthy" : "Warning",
        });
      }

      const p00RunTimes = p00RunTimesResult.data;
      const hasAllTrackedP00Runs = Boolean(
        p00RunTimes?.crp_d1_combined_run_at &&
          p00RunTimes?.oth_deletion_flag_run_at &&
          p00RunTimes?.p00_three_check_run_at
      );
      const hasSuccessfulP00CleanRuns = Boolean(
        controlRunResult.data &&
          (controlRunResult.data.run.status ?? "").toLowerCase() === "success" &&
          crpRunResult.data &&
          (crpRunResult.data.run.status ?? "").toLowerCase() === "success"
      );
      p00Checks.push({
        label: "CRP D1 Combined run",
        detail: p00RunTimes?.crp_d1_combined_run_at
          ? `Last run at ${formatTimestamp(p00RunTimes.crp_d1_combined_run_at)}.`
          : "No tracked run yet. Trigger this from Layer Detail > Run CRP D1 Combined Report.",
        status: p00RunTimes?.crp_d1_combined_run_at ? "Healthy" : "Review",
      });
      p00Checks.push({
        label: "OTH Deletion Flag run",
        detail: p00RunTimes?.oth_deletion_flag_run_at
          ? `Last run at ${formatTimestamp(p00RunTimes.oth_deletion_flag_run_at)}.`
          : "No tracked run yet. Trigger this from Layer Detail > Run OTH Deletion Flag Report.",
        status: p00RunTimes?.oth_deletion_flag_run_at ? "Healthy" : "Review",
      });
      p00Checks.push({
        label: "Check Report run",
        detail: p00RunTimes?.p00_three_check_run_at
          ? `Last run at ${formatTimestamp(p00RunTimes.p00_three_check_run_at)}.`
          : "No tracked run yet. Trigger this from Layer Detail > Run Check Report.",
        status: p00RunTimes?.p00_three_check_run_at ? "Healthy" : "Review",
      });

      const p00TrackedRunTimestamp = getLatestTimestamp([
        p00RunTimes?.crp_d1_combined_run_at,
        p00RunTimes?.oth_deletion_flag_run_at,
        p00RunTimes?.p00_three_check_run_at,
      ]);
      const hasP00CompletionEvidence =
        Boolean(p00Result.data) || (hasAllTrackedP00Runs && hasSuccessfulP00CleanRuns);

      const p00Issue = buildIssueSummary(p00Checks);
      builtSteps.push({
        code: "P00",
        name: "Preparation Raw Layer",
        description: "Build the raw preparation output from the latest uploaded source files and the clean-data prechecks.",
        owner: "Preparation inputs",
        executionStatus: resolveExecutionStatus(p00Missing.length > 0, hasP00CompletionEvidence),
        validationStatus: resolveSignalStatus(p00Checks),
        reviewStatus: resolveSignalStatus(p00Checks) === "Healthy" ? "Healthy" : "Review",
        timeLabel: "Latest P00 run",
        timeValue: formatTimestamp(
          p00TrackedRunTimestamp ?? getLatestTimestamp(p00Inputs.map((item) => item.updatedAt))
        ),
        timeHint: p00TrackedRunTimestamp
          ? "Uses tracked timestamps from the P00 run actions."
          : "No tracked P00 run yet; falling back to the latest input snapshot.",
        issueSummary: p00Issue.summary,
        issueHint: p00Issue.hint,
        records: p00Result.data?.row_count ?? null,
        deltaVsPrevious: "Base preparation layer from the current upload snapshot.",
        inputs: p00Inputs,
        checks: p00Checks,
        sampleColumns: [
          { key: "country_grouping", label: "Country Group" },
          { key: "machine_line_name", label: "Machine Line" },
          { key: "brand_name", label: "Brand" },
          { key: "fid", label: "FID" },
          { key: "tm", label: "Total Market" },
        ],
        sampleRows: p00Result.data
          ? p00Result.data.rows.slice(0, 5).map((row) => ({
              country_grouping: row.country_grouping,
              machine_line_name: row.machine_line_name,
              brand_name: row.brand_name,
              fid: row.fid,
              tm: row.tm === "" ? null : row.tm,
            }))
          : EMPTY_SAMPLE_ROWS,
      });

      const p10Inputs = p10Result.data
        ? buildReferencedUploadDependencies(
            [
              { id: p10Result.data.tma_upload_run_id, name: "TMA upload" },
              { id: p10Result.data.volvo_upload_run_id, name: "Volvo sales upload" },
              { id: p10Result.data.group_country_upload_run_id, name: "Group country upload" },
              { id: p10Result.data.source_matrix_upload_run_id, name: "Source matrix upload" },
            ],
            uploadRunMap
          )
        : buildUploadDependencies(P10_REQUIRED_TYPES, completenessMap);
      const p10Missing = getMissingTypes(completenessMap, P10_REQUIRED_TYPES);
      const p10Checks: Check[] = [
        p10Missing.length
          ? {
              label: "Required uploads",
              detail: `Missing latest successful upload for: ${p10Missing
                .map((type) => MATRIX_TYPE_LABELS[type] ?? type)
                .join(", ")}.`,
              status: "Review",
            }
          : { label: "Required uploads", detail: "All live uploads required for P10 are available.", status: "Healthy" },
      ];

      if (p10Result.data) {
        const reconciliationCount = p10Result.data.rows.filter(
          (row) => Math.abs(row.total_market - row.vce - row.non_vce) > 0.001
        ).length;
        const negativeValueCount = p10Result.data.rows.filter(
          (row) => row.total_market < 0 || row.vce < 0 || row.non_vce < 0
        ).length;
        p10Checks.push({
          label: "Value reconciliation",
          detail:
            reconciliationCount === 0
              ? "Total market equals VCE + Non-VCE for the live P10 sample."
              : `${reconciliationCount} sampled rows fail the total market reconciliation.`,
          status: reconciliationCount === 0 ? "Healthy" : "Warning",
        });
        p10Checks.push({
          label: "Negative values",
          detail:
            negativeValueCount === 0
              ? "No negative totals were found in the live P10 sample."
              : `${negativeValueCount} sampled P10 rows contain negative values.`,
          status: negativeValueCount === 0 ? "Healthy" : "Warning",
        });
      } else {
        p10Checks.push({
          label: "P10 report availability",
          detail: p10Result.error ?? "P10 report is not available.",
          status: "Review",
        });
      }

      const p10Issue = buildIssueSummary(p10Checks);
      builtSteps.push({
        code: "P10",
        name: "Prepared Layer",
        description: "Inspect prepared output after core TMC calculations and confirm that totals reconcile cleanly.",
        owner: "Core TMC logic",
        executionStatus: resolveExecutionStatus(p10Missing.length > 0, Boolean(p10Result.data)),
        validationStatus: resolveSignalStatus(p10Checks),
        reviewStatus: resolveSignalStatus(p10Checks) === "Healthy" ? "Healthy" : "Review",
        timeLabel: "Latest input snapshot",
        timeValue: formatTimestamp(getLatestTimestamp(p10Inputs.map((item) => item.updatedAt))),
        timeHint: "P10 currently exposes live output plus the upload snapshot that produced it.",
        issueSummary: p10Issue.summary,
        issueHint: p10Issue.hint,
        records: p10Result.data?.row_count ?? null,
        deltaVsPrevious: p10Result.data
          ? `${Math.max(0, p10Result.data.source_row_count - p10Result.data.row_count).toLocaleString()} rows were filtered or reshaped from the source stage.`
          : "P10 output is unavailable.",
        inputs: p10Inputs,
        checks: p10Checks,
        sampleColumns: [
          { key: "country_grouping", label: "Country Group" },
          { key: "machine_line_name", label: "Machine Line" },
          { key: "total_market", label: "Total Market" },
          { key: "vce", label: "VCE" },
          { key: "non_vce", label: "Non-VCE" },
        ],
        sampleRows: p10Result.data
          ? p10Result.data.rows.slice(0, 5).map((row) => ({
              country_grouping: row.country_grouping,
              machine_line_name: row.machine_line_name,
              total_market: row.total_market,
              vce: row.vce,
              non_vce: row.non_vce,
            }))
          : EMPTY_SAMPLE_ROWS,
      });

      const a10Inputs = a10Result.data
        ? buildReferencedUploadDependencies(
            [
              { id: a10Result.data.tma_upload_run_id, name: "TMA upload" },
              { id: a10Result.data.volvo_upload_run_id, name: "Volvo sales upload" },
              { id: a10Result.data.group_country_upload_run_id, name: "Group country upload" },
              { id: a10Result.data.source_matrix_upload_run_id, name: "Source matrix upload" },
            ],
            uploadRunMap
          )
        : buildUploadDependencies(P10_REQUIRED_TYPES, completenessMap);
      const a10Checks: Check[] = [];

      if (a10Result.data) {
        const missingCalculationStepCount = a10Result.data.rows.filter(
          (row) => !String(row.calculation_step ?? "").trim()
        ).length;
        const missingSourceCount = a10Result.data.rows.filter((row) => !String(row.source ?? "").trim()).length;
        a10Checks.push({
          label: "Adjustment rows",
          detail:
            a10Result.data.row_count > 0
              ? `Live A10 output contains ${a10Result.data.row_count.toLocaleString()} rows.`
              : "Live A10 output is empty.",
          status: a10Result.data.row_count > 0 ? "Healthy" : "Review",
        });
        a10Checks.push({
          label: "Calculation-step tagging",
          detail:
            missingCalculationStepCount === 0
              ? "All sampled A10 rows have a calculation-step label."
              : `${missingCalculationStepCount} sampled A10 rows are missing calculation-step labels.`,
          status: missingCalculationStepCount === 0 ? "Healthy" : "Warning",
        });
        a10Checks.push({
          label: "Source tagging",
          detail:
            missingSourceCount === 0
              ? "All sampled A10 rows contain a source value."
              : `${missingSourceCount} sampled A10 rows are missing source values.`,
          status: missingSourceCount === 0 ? "Healthy" : "Warning",
        });
      } else {
        a10Checks.push({
          label: "A10 report availability",
          detail: a10Result.error ?? "A10 report is not available.",
          status: "Review",
        });
      }

      const a10Issue = buildIssueSummary(a10Checks);
      builtSteps.push({
        code: "A10",
        name: "Adjustment Layer",
        description: "Check whether adjustment-ready rows were produced cleanly and tagged well enough for downstream review.",
        owner: "Adjustment logic",
        executionStatus: resolveExecutionStatus(false, Boolean(a10Result.data)),
        validationStatus: resolveSignalStatus(a10Checks),
        reviewStatus: resolveSignalStatus(a10Checks) === "Healthy" ? "Healthy" : "Review",
        timeLabel: "Latest input snapshot",
        timeValue: formatTimestamp(getLatestTimestamp(a10Inputs.map((item) => item.updatedAt))),
        timeHint: "A10 does not yet persist a dedicated run timestamp, so this card shows the snapshot feeding the output.",
        issueSummary: a10Issue.summary,
        issueHint: a10Issue.hint,
        records: a10Result.data?.row_count ?? null,
        deltaVsPrevious:
          a10Result.data && p10Result.data
            ? `${Math.max(0, p10Result.data.row_count - a10Result.data.row_count).toLocaleString()} fewer rows than P10 after adjustment grouping.`
            : "A10 output is unavailable.",
        inputs: a10Inputs,
        checks: a10Checks,
        sampleColumns: [
          { key: "country_grouping", label: "Country Group" },
          { key: "machine_line_name", label: "Machine Line" },
          { key: "brand_code", label: "Brand Code" },
          { key: "fid", label: "FID" },
          { key: "tm_fid", label: "TM FID" },
        ],
        sampleRows: a10Result.data
          ? a10Result.data.rows.slice(0, 5).map((row) => ({
              country_grouping: row.country_grouping,
              machine_line_name: row.machine_line_name,
              brand_code: row.brand_code,
              fid: row.fid,
              tm_fid: row.tm_fid,
            }))
          : EMPTY_SAMPLE_ROWS,
      });

      const splitInputs = buildUploadDependencies(SPL_REQUIRED_TYPES, completenessMap);
      const splitMissing = getMissingTypes(completenessMap, SPL_REQUIRED_TYPES);
      const splitChecks: Check[] = [
        splitMissing.length
          ? {
              label: "Required uploads",
              detail: `Missing latest successful upload for: ${splitMissing
                .map((type) => MATRIX_TYPE_LABELS[type] ?? type)
                .join(", ")}.`,
              status: "Review",
            }
          : { label: "Required uploads", detail: "Live uploads for the split case are available.", status: "Healthy" },
      ];

      if (splitResult.data) {
        const unmatchedGroups = Math.max(0, splitResult.data.summary.grouped_rows - splitResult.data.summary.matched_rows);
        const imbalanceCount = splitResult.data.detail_rows.filter((row) => {
          const value = row.before_after_difference;
          if (value === "" || value === null || value === undefined) return false;
          const numericValue = Number(value);
          return Number.isFinite(numericValue) && Math.abs(numericValue) > 0.001;
        }).length;
        splitChecks.push({
          label: "Ratio match coverage",
          detail:
            unmatchedGroups === 0
              ? "All grouped rows found matching split logic."
              : `${unmatchedGroups} grouped rows still do not match split logic.`,
          status: unmatchedGroups === 0 ? "Healthy" : "Warning",
        });
        splitChecks.push({
          label: "Before/after balance",
          detail:
            imbalanceCount === 0
              ? "Split detail rows reconcile before and after the split."
              : `${imbalanceCount} detail rows show a before/after split imbalance.`,
          status: imbalanceCount === 0 ? "Healthy" : "Warning",
        });
      } else {
        splitChecks.push({
          label: "Split case availability",
          detail: splitResult.error ?? "Split case report is not available.",
          status: "Review",
        });
      }

      const splitIssue = buildIssueSummary(splitChecks);
      builtSteps.push({
        code: "SPL",
        name: "Machine Line Split",
        description: "Verify whether the split case can run, whether rows match split logic, and whether the split preserves totals.",
        owner: "Split logic",
        executionStatus: resolveExecutionStatus(splitMissing.length > 0, Boolean(splitResult.data)),
        validationStatus: resolveSignalStatus(splitChecks),
        reviewStatus: resolveSignalStatus(splitChecks) === "Healthy" ? "Healthy" : "Review",
        timeLabel: "Latest input snapshot",
        timeValue: formatTimestamp(getLatestTimestamp(splitInputs.map((item) => item.updatedAt))),
        timeHint: "Split currently uses the latest dependency uploads plus the live split case report.",
        issueSummary: splitIssue.summary,
        issueHint: splitIssue.hint,
        records: splitResult.data?.summary.grouped_rows ?? null,
        deltaVsPrevious: splitResult.data
          ? `${splitResult.data.summary.matched_rows.toLocaleString()} matched rows out of ${splitResult.data.summary.grouped_rows.toLocaleString()} grouped rows.`
          : "Split output is unavailable.",
        inputs: splitInputs,
        checks: splitChecks,
        sampleColumns: [
          { key: "year", label: "Year" },
          { key: "machine_line_name", label: "Machine Line" },
          { key: "source", label: "Source" },
          { key: "gross_fid", label: "Gross FID" },
          { key: "net_fid", label: "Net FID" },
        ],
        sampleRows: splitResult.data
          ? splitResult.data.summary_rows.slice(0, 5).map((row) => ({
              year: row.year,
              machine_line_name: row.machine_line_name,
              source: row.source,
              gross_fid: row.gross_fid,
              net_fid: row.net_fid,
            }))
          : EMPTY_SAMPLE_ROWS,
      });

      builtSteps.push(
        buildStaticPendingStep({
          code: "RES",
          name: "Restatement",
          description: "Restatement is in the target flow, but the current prototype does not expose a live status endpoint for it yet.",
          owner: "Restatement logic",
          issueDetail: "No backend endpoint currently exposes restatement run time or validation results.",
          inputs: [{ name: "Split output", updatedAt: "Waiting for a persisted split run", status: "Missing" }],
        })
      );
      builtSteps.push(
        buildStaticPendingStep({
          code: "RPT",
          name: "Reporting",
          description: "Reporting remains downstream in the flow, but the prototype does not yet expose a live reporting or publish-status endpoint.",
          owner: "Reporting output",
          issueDetail: "No backend endpoint currently exposes reporting publish time or final approval status.",
          inputs: [
            { name: "Restated dataset", updatedAt: "Waiting for live restatement output", status: "Missing" },
            { name: "Business sign-off", updatedAt: "Not connected", status: "Missing" },
          ],
        })
      );

        if (!isActive) return;

        const allFailed =
          !completenessResult.data &&
          !latestTmaUploadResult.data &&
          !latestVolvoUploadResult.data &&
          !p00Result.data &&
          !p10Result.data &&
          !a10Result.data &&
          !splitResult.data &&
          !controlRunResult.data &&
          !crpRunResult.data &&
          !p00RunTimesResult.data;
        if (allFailed) {
          setPageError("Live pipeline status could not be loaded. Check whether the backend is running and your session is authenticated.");
        }

        setSteps(builtSteps);
      } catch (error) {
        if (!isActive) return;
        setPageError(normalizeError(error));
        setSteps(buildInitialSteps());
      } finally {
        if (isActive) {
          setLoading(false);
        }
      }
    }

    void loadPipelineHealth();
    return () => {
      isActive = false;
    };
  }, []);

  useEffect(() => {
    if (steps.length > 0 && !steps.some((step) => step.code === selectedStepCode)) {
      setSelectedStepCode(steps[0].code);
    }
  }, [selectedStepCode, steps]);

  const selectedStep = useMemo(() => steps.find((step) => step.code === selectedStepCode) ?? null, [selectedStepCode, steps]);
  const workflowSummary = useMemo(
    () => ({
      completed: steps.filter((step) => step.executionStatus === "Completed").length,
      pending: steps.filter((step) => step.executionStatus === "Pending").length,
      blocked: steps.filter((step) => step.executionStatus === "Blocked").length,
      review: steps.filter((step) => step.validationStatus !== "Healthy" || step.reviewStatus !== "Healthy").length,
    }),
    [steps]
  );

  return (
    <div className="page">
      <section className="section">
        <div className="section-header">
          <p className="section-tag">Pipeline Viewer</p>
          <h2 className="section-title">Calculation Step Health</h2>
          <p className="section-description">Track the latest live snapshot behind each TMC step and see whether the step has open issues.</p>
          <p className="section-description">When the backend persists real run metadata, this page shows it directly. Otherwise it falls back to the latest input snapshot feeding that step.</p>
        </div>

        {pageError && <p className="summary-description">{pageError}</p>}
        {loading && steps.length === 0 && <p className="summary-description">Loading live pipeline status...</p>}

        <div className="pipeline-overview-grid">
          <article className="mini-metric-card"><span className="mini-metric-card__label">Completed</span><strong className="mini-metric-card__value">{workflowSummary.completed}</strong><span className="mini-metric-card__hint">Steps with live output available</span></article>
          <article className="mini-metric-card"><span className="mini-metric-card__label">Pending</span><strong className="mini-metric-card__value">{workflowSummary.pending}</strong><span className="mini-metric-card__hint">Downstream steps without live endpoints</span></article>
          <article className="mini-metric-card"><span className="mini-metric-card__label">Blocked</span><strong className="mini-metric-card__value">{workflowSummary.blocked}</strong><span className="mini-metric-card__hint">Missing dependencies or unavailable live output</span></article>
          <article className="mini-metric-card"><span className="mini-metric-card__label">Needs Review</span><strong className="mini-metric-card__value">{workflowSummary.review}</strong><span className="mini-metric-card__hint">Validation checks still need attention</span></article>
        </div>

        <div className="pipeline-layout">
          <aside className="panel">
            <h3 className="panel__title">Step List</h3>
            <div className="step-list">
              {steps.map((step) => (
                <button type="button" key={step.code} className={`step-item ${step.code === selectedStep?.code ? "step-item--active" : ""}`} onClick={() => setSelectedStepCode(step.code)}>
                  <div className="step-item__top"><span className="step-item__code">{step.code}</span><span className={getExecutionBadgeClass(step.executionStatus)}>{step.executionStatus}</span></div>
                  <p className="step-item__name">{step.name}</p>
                  <div className="step-item__meta"><span className={getSignalBadgeClass(step.validationStatus)}>{step.validationStatus}</span><span className="step-item__meta-text">{step.records === null ? "Live rows unavailable" : `${formatRowCount(step.records)} rows`}</span></div>
                </button>
              ))}
            </div>
          </aside>

          <section className="panel">
            <h3 className="panel__title">Step Health</h3>
            {selectedStep ? (
              <div className="step-health">
                <div className="step-health__hero">
                  <div><p className="step-health__eyebrow">{selectedStep.code}</p><h4 className="step-health__title">{selectedStep.name}</h4><p className="summary-description">{selectedStep.description}</p></div>
                  <div className="step-health__badges"><span className={getExecutionBadgeClass(selectedStep.executionStatus)}>{selectedStep.executionStatus}</span><span className={getSignalBadgeClass(selectedStep.validationStatus)}>Validation: {selectedStep.validationStatus}</span><span className={getSignalBadgeClass(selectedStep.reviewStatus)}>Review: {selectedStep.reviewStatus}</span></div>
                </div>
                <div className="step-health__metrics">
                  <article className="health-metric-card"><span className="health-metric-card__label">{selectedStep.timeLabel}</span><strong className="health-metric-card__value">{selectedStep.timeValue}</strong><span className="health-metric-card__hint">{selectedStep.timeHint}</span></article>
                  <article className="health-metric-card"><span className="health-metric-card__label">Issue Status</span><strong className="health-metric-card__value">{selectedStep.issueSummary}</strong><span className="health-metric-card__hint">{selectedStep.issueHint}</span></article>
                  <article className="health-metric-card"><span className="health-metric-card__label">Output Rows</span><strong className="health-metric-card__value">{formatRowCount(selectedStep.records)}</strong><span className="health-metric-card__hint">{selectedStep.deltaVsPrevious}</span></article>
                </div>
                <div className="health-detail-grid">
                  <article className="health-detail-card"><h5 className="health-detail-card__title">Input Dependencies</h5><div className="dependency-list">{selectedStep.inputs.map((input) => <div key={`${selectedStep.code}-${input.name}`} className="dependency-item"><div><strong className="dependency-item__name">{input.name}</strong><p className="dependency-item__time">{input.updatedAt}</p></div><span className={getDependencyBadgeClass(input.status)}>{input.status}</span></div>)}</div></article>
                  <article className="health-detail-card"><h5 className="health-detail-card__title">Validation Checks</h5><div className="check-list">{selectedStep.checks.map((check) => <div key={`${selectedStep.code}-${check.label}`} className="check-item"><div className="check-item__copy"><strong>{check.label}</strong><p>{check.detail}</p></div><span className={getSignalBadgeClass(check.status)}>{check.status}</span></div>)}</div></article>
                </div>
              </div>
            ) : <p className="summary-description">Select a step from the left list to display its health status.</p>}
          </section>
        </div>
      </section>

      <section className="section">
        <div className="section-header">
          <p className="section-tag">Sample Output</p>
          <h3 className="section-title">Preview Table</h3>
          <p className="section-description">The preview below uses live rows when the selected step exposes output data.</p>
        </div>
        {selectedStep && selectedStep.sampleRows.length > 0 ? (
          <div className="table-wrapper">
            <table className="data-table">
              <thead><tr>{selectedStep.sampleColumns.map((column) => <th key={`${selectedStep.code}-${column.key}`}>{column.label}</th>)}</tr></thead>
              <tbody>{selectedStep.sampleRows.map((row, index) => <tr key={`${selectedStep.code}-row-${index}`}>{selectedStep.sampleColumns.map((column) => <td key={`${selectedStep.code}-${index}-${column.key}`}>{row[column.key] ?? "-"}</td>)}</tr>)}</tbody>
            </table>
          </div>
        ) : <p className="summary-description">No live sample rows are available for this step yet.</p>}
      </section>
    </div>
  );
}

export default PipelineViewerPage;
