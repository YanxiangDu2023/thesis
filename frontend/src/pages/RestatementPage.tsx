import { useEffect, useMemo, useState } from "react";
import FilterableTable from "../components/table/FilterableTable";
import {
  createPlanningYear,
  getPlanningYears,
  getLatestTotalMarketCalculationEligibleOthRows,
  getLatestTotalMarketCalculationCalculatedRows,
  getTotalMarketCalculationEligibleOthRows,
  saveTotalMarketCalculationEligibleOthSnapshot,
} from "../api/uploads";
import type { OthDeletionFlagRow } from "../types/upload";

type DoubleBrandCheckRow = OthDeletionFlagRow & {
  source_flag: string;
  distinct_source_count: number;
  distinct_sources: string;
};

type OcnOtnCaseRow = OthDeletionFlagRow & {
  source_flag: string;
  original_fid: string | number | null;
  ocn_otn_decision: string;
};

type CnxCaseRow = OthDeletionFlagRow & {
  source_flag: string;
  original_fid: string | number | null;
  cnx_decision: string;
};

type OhrPinCaseRow = OthDeletionFlagRow & {
  source_flag: string;
  original_fid: string | number | null;
  ohr_pin_decision: string;
};

type RestatementPreviewRow = OthDeletionFlagRow & {
  before_restatement: number;
  after_restatement: number;
};

type FinalRestatementResultRow = {
  country_grouping: string;
  country: string;
  country_code: string;
  region: string;
  machine_line_code: string;
  machine_line_name: string;
  artificial_machine_line: string;
  size_class_flag: string;
  row_type: string;
  source: string;
  brand_code: string;
  brand_name: string;
  fid: number;
};

function toKey(value: string | number | null | undefined): string {
  return String(value ?? "").trim().toUpperCase();
}

function toNumber(value: string | number | null | undefined): number {
  if (value === null || value === undefined || value === "") {
    return 0;
  }
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : 0;
  }
  const parsed = Number(String(value).replace(/,/g, "").trim());
  return Number.isFinite(parsed) ? parsed : 0;
}

function toDisplayText(value: string | number | null | undefined, fallback = ""): string {
  const text = String(value ?? "").trim();
  return text.length > 0 ? text : fallback;
}

function isOthNonVceRow(row: OthDeletionFlagRow): boolean {
  const sourceKey = toKey(row.source);
  if (sourceKey === "SAL" || sourceKey === "TMA") {
    return false;
  }
  const brandCodeKey = toKey(row.brand_code);
  const brandNameKey = toKey(row.brand_name);
  if (brandCodeKey === "VCE" || brandCodeKey === "VOL" || brandCodeKey === "VOLVO") {
    return false;
  }
  if (brandNameKey.includes("VOLVO")) {
    return false;
  }
  return true;
}

function isOthNonVceNonZeroRow(row: OthDeletionFlagRow): boolean {
  if (!isOthNonVceRow(row)) {
    return false;
  }
  return Math.abs(toNumber(row.fid)) > 0;
}

function getGroupKey(row: OthDeletionFlagRow): string {
  return [
    toKey(row.year),
    toKey(row.country),
    toKey(row.artificial_machine_line || row.machine_line_code || row.machine_line_name),
    toKey(row.size_class_flag),
    toKey(row.brand_code || row.brand_name),
  ].join("||");
}

function buildSnapshotRowKey(row: OthDeletionFlagRow): string {
  return [
    toKey(row.year),
    toKey(row.source),
    toKey(row.country_code),
    toKey(row.country),
    toKey(row.country_grouping),
    toKey(row.region),
    toKey(row.market_area),
    toKey(row.machine_line_name),
    toKey(row.machine_line_code),
    toKey(row.artificial_machine_line),
    toKey(row.brand_name),
    toKey(row.brand_code),
    toKey(row.size_class_flag),
  ].join("||");
}

function getRestatementGroupKey(row: OthDeletionFlagRow): string {
  return [
    toKey(row.country),
    toKey(row.artificial_machine_line),
    toKey(row.size_class_flag),
  ].join("||");
}

function getRestatementBaseKey(row: OthDeletionFlagRow): string {
  return [
    toKey(row.country),
    toKey(row.artificial_machine_line),
  ].join("||");
}

function getFinalRestatementGroupKey(row: OthDeletionFlagRow): string {
  return [
    toKey(row.country),
    toKey(row.artificial_machine_line),
    toKey(row.size_class_flag),
  ].join("||");
}

function filterRowsByRequiredSourceGroups(
  baseRows: OthDeletionFlagRow[],
  requiredSourceGroups: string[][]
): OthDeletionFlagRow[] {
  const normalizedGroups = requiredSourceGroups.map((group) => new Set(group.map(toKey)));
  const allowedSources = new Set(normalizedGroups.flatMap((group) => Array.from(group)));
  const candidateRows = baseRows.filter((row) => allowedSources.has(toKey(row.source)));
  const groupSources = new Map<string, Set<string>>();

  for (const row of candidateRows) {
    const key = getGroupKey(row);
    if (!groupSources.has(key)) {
      groupSources.set(key, new Set<string>());
    }
    groupSources.get(key)?.add(toKey(row.source));
  }

  const qualifiedGroupKeys = new Set<string>();
  for (const [groupKey, sourcesInGroup] of groupSources.entries()) {
    const hasAllRequiredGroups = normalizedGroups.every((requiredGroup) =>
      Array.from(requiredGroup).some((requiredSource) => sourcesInGroup.has(requiredSource))
    );
    if (hasAllRequiredGroups) {
      qualifiedGroupKeys.add(groupKey);
    }
  }

  return candidateRows.filter((row) => qualifiedGroupKeys.has(getGroupKey(row)));
}

function mergeCaseRowsIntoBaseRows(
  baseRows: OthDeletionFlagRow[],
  editedRows: OthDeletionFlagRow[],
  isCaseRow: (row: OthDeletionFlagRow) => boolean
): OthDeletionFlagRow[] {
  const queuesByKey = new Map<string, OthDeletionFlagRow[]>();
  for (const row of editedRows) {
    const key = buildSnapshotRowKey(row);
    if (!queuesByKey.has(key)) {
      queuesByKey.set(key, []);
    }
    queuesByKey.get(key)?.push(row);
  }

  const mergedRows: OthDeletionFlagRow[] = [];
  for (const row of baseRows) {
    if (!isCaseRow(row)) {
      mergedRows.push(row);
      continue;
    }
    const key = buildSnapshotRowKey(row);
    const queue = queuesByKey.get(key);
    if (!queue || queue.length === 0) {
      // Keep untouched case rows. Only rows explicitly edited in the active
      // case panel should overwrite values; others must remain unchanged.
      mergedRows.push(row);
      continue;
    }
    const edited = queue.shift();
    if (!edited) {
      mergedRows.push(row);
      continue;
    }
    mergedRows.push({
      ...row,
      fid: edited.fid,
      deletion_flag: edited.deletion_flag,
      pri_sec: edited.pri_sec,
      reporter_flag: edited.reporter_flag,
    });
  }
  return mergedRows;
}

function RestatementPage() {
  const [fullRows, setFullRows] = useState<OthDeletionFlagRow[]>([]);
  const [rows, setRows] = useState<OthDeletionFlagRow[]>([]);
  const [loading, setLoading] = useState(false);
  const [runLoading, setRunLoading] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [sourceRowCount, setSourceRowCount] = useState(0);
  const [splitMachineLines, setSplitMachineLines] = useState<string[]>([]);
  const [splitInputRows, setSplitInputRows] = useState<number | undefined>(undefined);
  const [splitOutputRows, setSplitOutputRows] = useState<number | undefined>(undefined);
  const [sourceReportRunId, setSourceReportRunId] = useState<number | undefined>(undefined);
  const [sourceReportCreatedAt, setSourceReportCreatedAt] = useState<string | undefined>(undefined);
  const [threeCheckReportRunId, setThreeCheckReportRunId] = useState<number | undefined>(undefined);
  const [threeCheckReportCreatedAt, setThreeCheckReportCreatedAt] = useState<string | undefined>(undefined);
  const [planningYears, setPlanningYears] = useState<number[]>([]);
  const [planningYearsLoading, setPlanningYearsLoading] = useState(false);
  const [planningYearsError, setPlanningYearsError] = useState("");
  const [selectedPlanningYear, setSelectedPlanningYear] = useState("");
  const [newPlanningYearInput, setNewPlanningYearInput] = useState("");
  const [creatingPlanningYear, setCreatingPlanningYear] = useState(false);

  const [activePanel, setActivePanel] = useState<"data" | "report" | "delete" | "restatement">("data");
  const [doubleBrandRows, setDoubleBrandRows] = useState<DoubleBrandCheckRow[]>([]);
  const [doubleBrandGroupCount, setDoubleBrandGroupCount] = useState(0);
  const [doubleBrandSourceRowCount, setDoubleBrandSourceRowCount] = useState(0);
  const [doubleBrandMessage, setDoubleBrandMessage] = useState("");
  const [doubleBrandError, setDoubleBrandError] = useState("");

  const [selectedDeleteCase, setSelectedDeleteCase] = useState("OCN/OTN Case");
  const [snapshotSaving, setSnapshotSaving] = useState(false);

  const [ocnOtnRows, setOcnOtnRows] = useState<OcnOtnCaseRow[]>([]);
  const [ocnOtnSavedRows, setOcnOtnSavedRows] = useState<OcnOtnCaseRow[]>([]);
  const [ocnOtnMessage, setOcnOtnMessage] = useState("");
  const [ocnOtnError, setOcnOtnError] = useState("");
  const [ocnOtnRuleApplying, setOcnOtnRuleApplying] = useState(false);
  const [ocnOtnEditMode, setOcnOtnEditMode] = useState(false);
  const [ocnOtnDirty, setOcnOtnDirty] = useState(false);

  const [cnxRows, setCnxRows] = useState<CnxCaseRow[]>([]);
  const [cnxSavedRows, setCnxSavedRows] = useState<CnxCaseRow[]>([]);
  const [cnxMessage, setCnxMessage] = useState("");
  const [cnxError, setCnxError] = useState("");
  const [cnxRuleApplying, setCnxRuleApplying] = useState(false);
  const [cnxEditMode, setCnxEditMode] = useState(false);
  const [cnxDirty, setCnxDirty] = useState(false);
  const [cnxHasOtherSourceByGroup, setCnxHasOtherSourceByGroup] = useState<Record<string, boolean>>({});

  const [ohrPinRows, setOhrPinRows] = useState<OhrPinCaseRow[]>([]);
  const [ohrPinSavedRows, setOhrPinSavedRows] = useState<OhrPinCaseRow[]>([]);
  const [ohrPinMessage, setOhrPinMessage] = useState("");
  const [ohrPinError, setOhrPinError] = useState("");
  const [ohrPinRuleApplying, setOhrPinRuleApplying] = useState(false);
  const [ohrPinEditMode, setOhrPinEditMode] = useState(false);
  const [ohrPinDirty, setOhrPinDirty] = useState(false);

  const [restatementRows, setRestatementRows] = useState<RestatementPreviewRow[]>([]);
  const [restatementApplied, setRestatementApplied] = useState(false);
  const [restatementApplying, setRestatementApplying] = useState(false);
  const [restatementMessage, setRestatementMessage] = useState("");
  const [finalRestatementRows, setFinalRestatementRows] = useState<FinalRestatementResultRow[]>([]);
  const [showFinalRestatementResult, setShowFinalRestatementResult] = useState(false);
  const selectedPlanningYearNumber = selectedPlanningYear ? Number(selectedPlanningYear) : undefined;
  const hasSelectedPlanningYear =
    selectedPlanningYearNumber !== undefined && Number.isFinite(selectedPlanningYearNumber);

  const dataColumns = useMemo(
    () => [
      { key: "country_grouping", label: "Country Grouping" },
      { key: "country", label: "Country" },
      { key: "country_code", label: "Country Code" },
      { key: "region", label: "Region" },
      { key: "machine_line_code", label: "Machine Line" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "artificial_machine_line", label: "Artificial Machine Line" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "brand_code", label: "Brand Code" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "source", label: "Source" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "fid", label: "FID" },
    ],
    []
  );

  const reportColumns = useMemo(
    () => [
      ...dataColumns,
      { key: "distinct_source_count", label: "Distinct Sources" },
      { key: "distinct_sources", label: "Source Set" },
    ],
    [dataColumns]
  );

  const ocnOtnColumns = useMemo(
    () => [...dataColumns, { key: "original_fid", label: "Original FID" }, { key: "ocn_otn_decision", label: "OCN/OTN Decision" }],
    [dataColumns]
  );
  const cnxColumns = useMemo(
    () => [...dataColumns, { key: "original_fid", label: "Original FID" }, { key: "cnx_decision", label: "CNX Decision" }],
    [dataColumns]
  );
  const ohrPinColumns = useMemo(
    () => [...dataColumns, { key: "original_fid", label: "Original FID" }, { key: "ohr_pin_decision", label: "OHR/PIN Decision" }],
    [dataColumns]
  );
  const restatementColumns = useMemo(
    () =>
      restatementApplied
        ? [
            ...dataColumns,
            { key: "before_restatement", label: "Before Restatement" },
            { key: "after_restatement", label: "After Restatement" },
          ]
        : dataColumns,
    [dataColumns, restatementApplied]
  );
  const finalRestatementColumns = useMemo(
    () => [
      { key: "country_grouping", label: "Country Grouping" },
      { key: "country", label: "Country" },
      { key: "country_code", label: "Country Code" },
      { key: "region", label: "Region" },
      { key: "machine_line_code", label: "Machine Line" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "artificial_machine_line", label: "Artificial Machine Line" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "row_type", label: "Row Type" },
      { key: "source", label: "Source" },
      { key: "brand_code", label: "Brand Code" },
      { key: "brand_name", label: "Brand Name" },
      { key: "fid", label: "FID" },
    ],
    []
  );

  useEffect(() => {
    let active = true;
    const loadPlanningYears = async () => {
      try {
        setPlanningYearsLoading(true);
        setPlanningYearsError("");
        const result = await getPlanningYears();
        if (!active) {
          return;
        }
        setPlanningYears(result.years);
        setSelectedPlanningYear("");
      } catch (err) {
        if (!active) {
          return;
        }
        setPlanningYearsError(err instanceof Error ? err.message : "Failed to load planning years.");
      } finally {
        if (active) {
          setPlanningYearsLoading(false);
        }
      }
    };

    void loadPlanningYears();
    return () => {
      active = false;
    };
  }, []);

  function requirePlanningYear(): number | null {
    if (!hasSelectedPlanningYear || selectedPlanningYearNumber === undefined) {
      setError("Please select a planning year first.");
      setMessage("");
      return null;
    }
    return selectedPlanningYearNumber;
  }

  async function loadLatestRestatementInput(
    planningYear: number,
    options?: { preferCalculated?: boolean; allowComputeFallback?: boolean }
  ) {
    const preferCalculated = options?.preferCalculated ?? false;
    const allowComputeFallback = options?.allowComputeFallback ?? false;

    let result;
    let sourceLabel = "OTH Non VCE Total Market Data";

    if (preferCalculated) {
      try {
        result = await getLatestTotalMarketCalculationCalculatedRows(planningYear);
        sourceLabel = "Calculate Total Market snapshot";
      } catch (calculatedError) {
        if (!allowComputeFallback) {
          throw calculatedError;
        }
      }
    }

    if (!result) {
      try {
        result = await getLatestTotalMarketCalculationEligibleOthRows(planningYear);
      } catch (latestError) {
        if (!allowComputeFallback) {
          throw latestError;
        }
        result = await getTotalMarketCalculationEligibleOthRows(planningYear);
        sourceLabel = "newly generated OTH Non VCE run";
      }
    }

    setFullRows(result.rows);
    const filtered = result.rows.filter(isOthNonVceNonZeroRow);
    setRows(filtered);
    setRestatementRows([]);
    setRestatementApplied(false);
    setRestatementMessage("");
    setFinalRestatementRows([]);
    setShowFinalRestatementResult(false);
    setSourceRowCount(result.row_count);
    setSplitMachineLines(result.split_machine_lines);
    setSplitInputRows(result.split_input_rows);
    setSplitOutputRows(result.split_output_rows);
    setSourceReportRunId(result.source_report_run_id);
    setSourceReportCreatedAt(result.source_report_created_at);
    setThreeCheckReportRunId(result.three_check_report_run_id);
    setThreeCheckReportCreatedAt(result.three_check_report_created_at);
    return { filteredCount: filtered.length, runId: result.run_id, sourceLabel };
  }

  async function handleRun() {
    const planningYear = requirePlanningYear();
    if (planningYear === null) {
      return;
    }
    setActivePanel("data");
    setLoading(true);
    setRunLoading(true);
    setError("");
    setMessage("Loading latest saved OTH Non VCE result...");
    try {
      const loaded = await loadLatestRestatementInput(planningYear, {
        // Restatement should prioritize already saved post-delete output.
        preferCalculated: true,
        allowComputeFallback: true,
      });
      setMessage(
        `Loaded from ${loaded.sourceLabel}. ${loaded.filteredCount} OTH rows (FID != 0)${loaded.runId ? ` (Run #${loaded.runId})` : ""}.`
      );
    } catch (fetchError) {
      setError(fetchError instanceof Error ? fetchError.message : "Failed to load OTH Non VCE Total Market Data.");
      setMessage("");
    } finally {
      setLoading(false);
      setRunLoading(false);
    }
  }

  async function handleShowLatest() {
    const planningYear = requirePlanningYear();
    if (planningYear === null) {
      return;
    }
    setActivePanel("data");
    setLoading(true);
    setError("");
    setMessage("Loading latest OTH Non VCE Total Market Data...");
    try {
      const loaded = await loadLatestRestatementInput(planningYear, { preferCalculated: true });
      setMessage(
        `Latest loaded from ${loaded.sourceLabel}. ${loaded.filteredCount} rows${loaded.runId ? ` (Run #${loaded.runId})` : ""}.`
      );
    } catch (fetchError) {
      setError(fetchError instanceof Error ? fetchError.message : "Failed to load latest data.");
      setMessage("");
    } finally {
      setLoading(false);
    }
  }

  async function handleOpenRestatementPanel() {
    const planningYear = requirePlanningYear();
    if (planningYear === null) {
      return;
    }
    setActivePanel("restatement");
    setDoubleBrandError("");
    setLoading(true);
    setError("");
    setMessage("Loading latest restatement result...");
    try {
      const loaded = await loadLatestRestatementInput(planningYear, { preferCalculated: true });
      setMessage(
        `Latest loaded from ${loaded.sourceLabel}. ${loaded.filteredCount} rows${loaded.runId ? ` (Run #${loaded.runId})` : ""}.`
      );
      setDoubleBrandMessage(
        'Restatement panel shows the final OTH result after Delete Double Brand. Click "Save" in Delete Double Brand to persist updates.'
      );
    } catch (fetchError) {
      setError(fetchError instanceof Error ? fetchError.message : "Failed to load latest restatement data.");
      setDoubleBrandMessage("");
    } finally {
      setLoading(false);
    }
  }

  async function handleApplyRestatementRule() {
    if (rows.length === 0) {
      setRestatementMessage("No OTH rows available to restate.");
      return;
    }
    setRestatementApplying(true);
    setRestatementMessage("");
    try {
      const tmaByGroup = new Map<string, number>();
      const vceSalByGroup = new Map<string, number>();
      const othByGroup = new Map<string, number>();
      const tmaByBase = new Map<string, number>();
      const vceSalByBase = new Map<string, number>();
      const othByBase = new Map<string, number>();

      for (const row of fullRows) {
        const groupKey = getRestatementGroupKey(row);
        const baseKey = getRestatementBaseKey(row);
        const source = toKey(row.source);
        const fid = toNumber(row.fid);
        if (source === "TMA") {
          tmaByGroup.set(groupKey, (tmaByGroup.get(groupKey) ?? 0) + fid);
          tmaByBase.set(baseKey, (tmaByBase.get(baseKey) ?? 0) + fid);
          continue;
        }
        const isVceSal =
          source === "SAL" &&
          (toKey(row.brand_code) === "VCE" ||
            toKey(row.brand_name).includes("VOLVO"));
        if (isVceSal) {
          vceSalByGroup.set(groupKey, (vceSalByGroup.get(groupKey) ?? 0) + fid);
          vceSalByBase.set(baseKey, (vceSalByBase.get(baseKey) ?? 0) + fid);
        }
      }

      for (const row of rows) {
        const groupKey = getRestatementGroupKey(row);
        const baseKey = getRestatementBaseKey(row);
        othByGroup.set(groupKey, (othByGroup.get(groupKey) ?? 0) + toNumber(row.fid));
        othByBase.set(baseKey, (othByBase.get(baseKey) ?? 0) + toNumber(row.fid));
      }

      let fallbackGroupCount = 0;
      const fallbackUsedKeys = new Set<string>();
      const groupMeta = new Map<
        string,
        {
          baseKey: string;
          useFallback: boolean;
          targetNonVce: number;
          othSum: number;
        }
      >();

      for (const row of rows) {
        const groupKey = getRestatementGroupKey(row);
        if (groupMeta.has(groupKey)) {
          continue;
        }
        const baseKey = getRestatementBaseKey(row);
        const hasFullTma = tmaByGroup.has(groupKey);
        const hasFullSal = vceSalByGroup.has(groupKey);
        const useFallback = !hasFullTma && !hasFullSal;
        if (useFallback && !fallbackUsedKeys.has(groupKey)) {
          fallbackUsedKeys.add(groupKey);
          fallbackGroupCount += 1;
        }
        const tma = useFallback ? (tmaByBase.get(baseKey) ?? 0) : (tmaByGroup.get(groupKey) ?? 0);
        const vceSal = useFallback ? (vceSalByBase.get(baseKey) ?? 0) : (vceSalByGroup.get(groupKey) ?? 0);
        const targetNonVce = Math.max(tma - vceSal, 0);
        const othSum = useFallback ? (othByBase.get(baseKey) ?? 0) : (othByGroup.get(groupKey) ?? 0);
        groupMeta.set(groupKey, { baseKey, useFallback, targetNonVce, othSum });
      }

      const nextRows: RestatementPreviewRow[] = rows.map((row) => {
        const groupKey = getRestatementGroupKey(row);
        const meta = groupMeta.get(groupKey);
        const before = toNumber(row.fid);
        const targetNonVce = meta?.targetNonVce ?? 0;
        const othSum = meta?.othSum ?? 0;

        let after = before;
        // Case 1: shrink non-VCE OTH proportionally when above target.
        if (othSum > 0 && othSum > targetNonVce) {
          after = (before / othSum) * targetNonVce;
        }

        return {
          ...row,
          fid: Number(after.toFixed(2)),
          before_restatement: Number(before.toFixed(2)),
          after_restatement: Number(after.toFixed(2)),
        };
      });

      const rowsByGroup = new Map<string, number[]>();
      nextRows.forEach((row, index) => {
        const key = getRestatementGroupKey(row);
        if (!rowsByGroup.has(key)) {
          rowsByGroup.set(key, []);
        }
        rowsByGroup.get(key)?.push(index);
      });

      let adjustedGroupCount = 0;
      let toppedUpGroupCount = 0;
      let createdUrsRowCount = 0;
      const topUpRowsByGroup = new Map<string, RestatementPreviewRow[]>();

      for (const [groupKey, meta] of groupMeta.entries()) {
        const othSum = meta.othSum;
        const targetNonVce = meta.targetNonVce;
        if (othSum > targetNonVce) {
          adjustedGroupCount += 1;
          continue;
        }
        if (targetNonVce - othSum <= 0.0001) {
          continue;
        }
        toppedUpGroupCount += 1;
        const gap = targetNonVce - othSum;
        const groupIndexes = rowsByGroup.get(groupKey) ?? [];

        const template = groupIndexes.length > 0 ? nextRows[groupIndexes[0]] : undefined;
        if (!template) {
          continue;
        }
        // Always add a dedicated top-up row for shortage groups, as requested.
        const newRow: RestatementPreviewRow = {
          ...template,
          brand_name: "Other Reporting Brands",
          brand_code: "Other Reporting Brands",
          source: "OTH",
          source_flag: "OTH",
          fid: Number(gap.toFixed(2)),
          before_restatement: 0,
          after_restatement: Number(gap.toFixed(2)),
        };
        if (!topUpRowsByGroup.has(groupKey)) {
          topUpRowsByGroup.set(groupKey, []);
        }
        topUpRowsByGroup.get(groupKey)?.push(newRow);
        createdUrsRowCount += 1;
      }

      const lastIndexByGroup = new Map<string, number>();
      nextRows.forEach((row, index) => {
        lastIndexByGroup.set(getRestatementGroupKey(row), index);
      });

      const orderedNextRows: RestatementPreviewRow[] = [];
      nextRows.forEach((row, index) => {
        const groupKey = getRestatementGroupKey(row);
        orderedNextRows.push(row);
        if (lastIndexByGroup.get(groupKey) === index) {
          const topUps = topUpRowsByGroup.get(groupKey) ?? [];
          if (topUps.length > 0) {
            orderedNextRows.push(...topUps);
          }
        }
      });

      setRestatementRows(orderedNextRows);
      setRestatementApplied(true);
      setFinalRestatementRows([]);
      setShowFinalRestatementResult(false);
      setRestatementMessage(
        `Restatement rule applied. Adjusted groups: ${adjustedGroupCount}. Top-up groups: ${toppedUpGroupCount}. Added URS rows: ${createdUrsRowCount}. Fallback groups (country + artificial machine line): ${fallbackGroupCount}.`
      );
    } finally {
      setRestatementApplying(false);
    }
  }

  function handleShowFinalRestatementResult() {
    if (!restatementApplied || restatementRows.length === 0) {
      setRestatementMessage('Please click "Apply Restatement Rule" first.');
      return;
    }

    type GroupMeta = Pick<
      FinalRestatementResultRow,
      "country_grouping" | "country" | "country_code" | "region" | "machine_line_code" | "machine_line_name" | "artificial_machine_line" | "size_class_flag"
    >;
    const groupMetaByKey = new Map<string, GroupMeta>();
    const restatedBrandRowsByKey = new Map<string, RestatementPreviewRow[]>();

    const ensureGroupMeta = (row: OthDeletionFlagRow) => {
      const groupKey = getFinalRestatementGroupKey(row);
      if (!groupMetaByKey.has(groupKey)) {
        groupMetaByKey.set(groupKey, {
          country_grouping: row.country_grouping,
          country: row.country,
          country_code: row.country_code,
          region: row.region,
          machine_line_code: row.machine_line_code,
          machine_line_name: row.machine_line_name,
          artificial_machine_line: row.artificial_machine_line,
          size_class_flag: row.size_class_flag,
        });
      }
      return groupKey;
    };

    for (const row of fullRows) {
      const sourceKey = toKey(row.source);
      const isVceSal =
        sourceKey === "SAL" &&
        (toKey(row.brand_code) === "VCE" || toKey(row.brand_name).includes("VOLVO"));
      if (sourceKey === "TMA" || isVceSal) {
        ensureGroupMeta(row);
      }
    }

    for (const row of restatementRows) {
      const groupKey = ensureGroupMeta(row);
      if (!restatedBrandRowsByKey.has(groupKey)) {
        restatedBrandRowsByKey.set(groupKey, []);
      }
      restatedBrandRowsByKey.get(groupKey)?.push(row);
    }

    const tmaByKey = new Map<string, number>();
    const salByKey = new Map<string, number>();
    for (const row of fullRows) {
      const groupKey = getFinalRestatementGroupKey(row);
      if (!groupMetaByKey.has(groupKey)) {
        continue;
      }
      const sourceKey = toKey(row.source);
      if (sourceKey === "TMA") {
        tmaByKey.set(groupKey, (tmaByKey.get(groupKey) ?? 0) + toNumber(row.fid));
        continue;
      }
      const isVceSal =
        sourceKey === "SAL" &&
        (toKey(row.brand_code) === "VCE" || toKey(row.brand_name).includes("VOLVO"));
      if (isVceSal) {
        salByKey.set(groupKey, (salByKey.get(groupKey) ?? 0) + toNumber(row.fid));
      }
    }

    const groupOrder = Array.from(groupMetaByKey.entries())
      .sort(([, a], [, b]) => {
        return (
          a.country_grouping.localeCompare(b.country_grouping) ||
          a.country.localeCompare(b.country) ||
          a.machine_line_code.localeCompare(b.machine_line_code) ||
          a.machine_line_name.localeCompare(b.machine_line_name) ||
          a.artificial_machine_line.localeCompare(b.artificial_machine_line) ||
          a.size_class_flag.localeCompare(b.size_class_flag)
        );
      })
      .map(([key]) => key);

    const resultRows: FinalRestatementResultRow[] = [];
    for (const groupKey of groupOrder) {
      const meta = groupMetaByKey.get(groupKey);
      if (!meta) {
        continue;
      }

      resultRows.push({
        ...meta,
        row_type: "TMA",
        source: "TMA",
        brand_code: "#",
        brand_name: "TOTAL MARKET",
        fid: Number((tmaByKey.get(groupKey) ?? 0).toFixed(2)),
      });

      resultRows.push({
        ...meta,
        row_type: "SAL",
        source: "SAL",
        brand_code: "VCE",
        brand_name: "VOLVO CE",
        fid: Number((salByKey.get(groupKey) ?? 0).toFixed(2)),
      });

      const brands = (restatedBrandRowsByKey.get(groupKey) ?? []).slice().sort((a, b) => {
        return (
          toKey(a.brand_code).localeCompare(toKey(b.brand_code)) ||
          toKey(a.brand_name).localeCompare(toKey(b.brand_name)) ||
          toKey(a.source).localeCompare(toKey(b.source))
        );
      });

      for (const brandRow of brands) {
        resultRows.push({
          ...meta,
          row_type: "Restated OTH",
          source: toDisplayText(brandRow.source, "OTH"),
          brand_code: toDisplayText(brandRow.brand_code),
          brand_name: toDisplayText(brandRow.brand_name),
          fid: Number(toNumber(brandRow.after_restatement).toFixed(2)),
        });
      }
    }

    setFinalRestatementRows(resultRows);
    setShowFinalRestatementResult(true);
    setRestatementMessage(`Final Restatement Result generated: ${groupOrder.length} groups, ${resultRows.length} rows.`);
    window.setTimeout(() => {
      const section = document.getElementById("restatement-panel-top");
      if (section) {
        section.scrollIntoView({ behavior: "smooth", block: "start" });
      }
    }, 0);
  }

  function handleReportCheckDoubleBrand() {
    setActivePanel("report");
    const groups = new Map<string, { rows: OthDeletionFlagRow[]; sources: Set<string> }>();
    for (const row of rows) {
      const key = [
        toKey(row.country),
        toKey(row.machine_line_code),
        toKey(row.artificial_machine_line),
        toKey(row.size_class_flag),
        toKey(row.brand_code),
      ].join("||");
      if (!groups.has(key)) {
        groups.set(key, { rows: [], sources: new Set<string>() });
      }
      const g = groups.get(key);
      g?.rows.push(row);
      g?.sources.add(toKey(row.source));
    }

    const resultRows: DoubleBrandCheckRow[] = [];
    let duplicateGroupCount = 0;
    for (const group of groups.values()) {
      if (group.sources.size <= 1) {
        continue;
      }
      duplicateGroupCount += 1;
      const sourceSet = Array.from(group.sources).sort().join(", ");
      for (const row of group.rows) {
        resultRows.push({
          ...row,
          source_flag: "OTH",
          distinct_source_count: group.sources.size,
          distinct_sources: sourceSet,
        });
      }
    }
    setDoubleBrandRows(resultRows);
    setDoubleBrandGroupCount(duplicateGroupCount);
    setDoubleBrandSourceRowCount(rows.length);
    setDoubleBrandMessage(
      `Found ${resultRows.length} rows across ${duplicateGroupCount} duplicate groups (same country + machine line code + artificial machine line + size class + brand code, different source).`
    );
    setDoubleBrandError("");
  }

  function resetDeleteStates() {
    setOcnOtnRows([]);
    setOcnOtnSavedRows([]);
    setOcnOtnMessage("");
    setOcnOtnError("");
    setOcnOtnEditMode(false);
    setOcnOtnDirty(false);
    setCnxRows([]);
    setCnxSavedRows([]);
    setCnxMessage("");
    setCnxError("");
    setCnxEditMode(false);
    setCnxDirty(false);
    setCnxHasOtherSourceByGroup({});
    setOhrPinRows([]);
    setOhrPinSavedRows([]);
    setOhrPinMessage("");
    setOhrPinError("");
    setOhrPinEditMode(false);
    setOhrPinDirty(false);
  }

  function resetYearScopedData() {
    setActivePanel("data");
    setFullRows([]);
    setRows([]);
    setLoading(false);
    setRunLoading(false);
    setMessage("");
    setError("");
    setSourceRowCount(0);
    setSplitMachineLines([]);
    setSplitInputRows(undefined);
    setSplitOutputRows(undefined);
    setSourceReportRunId(undefined);
    setSourceReportCreatedAt(undefined);
    setThreeCheckReportRunId(undefined);
    setThreeCheckReportCreatedAt(undefined);
    setDoubleBrandRows([]);
    setDoubleBrandGroupCount(0);
    setDoubleBrandSourceRowCount(0);
    setDoubleBrandMessage("");
    setDoubleBrandError("");
    resetDeleteStates();
    setRestatementRows([]);
    setRestatementApplied(false);
    setRestatementApplying(false);
    setRestatementMessage("");
    setFinalRestatementRows([]);
    setShowFinalRestatementResult(false);
  }

  function handlePlanningYearChange(value: string) {
    setSelectedPlanningYear(value);
    resetYearScopedData();
  }

  async function handleCreatePlanningYear() {
    const parsed = Number(newPlanningYearInput);
    if (!Number.isInteger(parsed) || parsed < 1900 || parsed > 2999) {
      setPlanningYearsError("Please enter a valid year between 1900 and 2999.");
      return;
    }
    try {
      setCreatingPlanningYear(true);
      setPlanningYearsError("");
      const result = await createPlanningYear(parsed);
      const nextYears = Array.from(new Set([...planningYears, result.year])).sort((a, b) => b - a);
      setPlanningYears(nextYears);
      setSelectedPlanningYear(String(result.year));
      setNewPlanningYearInput("");
      resetYearScopedData();
      setMessage(`Planning year ${result.year} created and selected.`);
      setError("");
    } catch (err) {
      setPlanningYearsError(err instanceof Error ? err.message : "Failed to create planning year.");
    } finally {
      setCreatingPlanningYear(false);
    }
  }

  function handleDeleteDoubleBrand() {
    setActivePanel("delete");
    setDoubleBrandMessage("Delete Double Brand action is ready. Select case buttons below.");
    setDoubleBrandError("");
    resetDeleteStates();
    void handleLoadOcnOtnCase();
  }

  function handleSelectDeleteCase(caseName: string) {
    setSelectedDeleteCase(caseName);
    if (caseName === "OCN/OTN Case") {
      void handleLoadOcnOtnCase();
    } else if (caseName === "CNX Case") {
      void handleLoadCnxCase();
    } else if (caseName === "OHR/PIN Case") {
      void handleLoadOhrPinCase();
    }
  }

  async function handleLoadOcnOtnCase() {
    resetDeleteStates();
    const filtered = filterRowsByRequiredSourceGroups(rows, [["OCN"], ["OTN"]]).map((row) => ({
      ...row,
      source_flag: "OTH",
      original_fid: row.fid,
      ocn_otn_decision: "PENDING",
    }));
    setOcnOtnRows(filtered);
    setOcnOtnSavedRows(filtered.map((r) => ({ ...r })));
    setOcnOtnMessage(`Loaded ${filtered.length} rows where OCN and OTN both exist in the same group.`);
    setDoubleBrandMessage("Selected OCN/OTN Case.");
  }

  async function handleLoadCnxCase() {
    resetDeleteStates();
    const sourcesByGroup = new Map<string, Set<string>>();
    for (const row of rows) {
      const key = getGroupKey(row);
      if (!sourcesByGroup.has(key)) {
        sourcesByGroup.set(key, new Set<string>());
      }
      sourcesByGroup.get(key)?.add(toKey(row.source));
    }
    const hasOtherByGroup: Record<string, boolean> = {};
    for (const [k, sources] of sourcesByGroup.entries()) {
      hasOtherByGroup[k] = Array.from(sources).some((s) => s !== "CNX");
    }
    const filtered = rows
      .filter((row) => toKey(row.source) === "CNX" && (hasOtherByGroup[getGroupKey(row)] ?? false))
      .map((row) => ({
        ...row,
        source_flag: "OTH",
        original_fid: row.fid,
        cnx_decision: "PENDING",
      }));
    setCnxHasOtherSourceByGroup(hasOtherByGroup);
    setCnxRows(filtered);
    setCnxSavedRows(filtered.map((r) => ({ ...r })));
    setCnxMessage(`Loaded ${filtered.length} CNX rows where non-CNX source also exists in same group.`);
    setDoubleBrandMessage("Selected CNX Case.");
  }

  async function handleLoadOhrPinCase() {
    resetDeleteStates();
    const filtered = filterRowsByRequiredSourceGroups(rows, [["OHR"], ["PIN"]]).map((row) => ({
      ...row,
      source_flag: "OTH",
      original_fid: row.fid,
      ohr_pin_decision: "PENDING",
    }));
    setOhrPinRows(filtered);
    setOhrPinSavedRows(filtered.map((r) => ({ ...r })));
    setOhrPinMessage(`Loaded ${filtered.length} rows where OHR and PIN both exist in same group.`);
    setDoubleBrandMessage("Selected OHR/PIN Case.");
  }

  async function handleApplyOcnOtnRule() {
    setOcnOtnRuleApplying(true);
    setOcnOtnError("");
    try {
      const groupedRows = new Map<string, OcnOtnCaseRow[]>();
      const sourceTotals = new Map<string, { ocnTotal: number; otnTotal: number; otnAssigned: boolean }>();
      for (const row of ocnOtnRows) {
        const key = getGroupKey(row);
        if (!groupedRows.has(key)) {
          groupedRows.set(key, []);
        }
        groupedRows.get(key)?.push(row);
      }
      for (const [groupKey, groupRows] of groupedRows.entries()) {
        const ocnTotal = groupRows.filter((i) => toKey(i.source) === "OCN").reduce((t, i) => t + toNumber(i.original_fid ?? i.fid), 0);
        const otnTotal = groupRows.filter((i) => toKey(i.source) === "OTN").reduce((t, i) => t + toNumber(i.original_fid ?? i.fid), 0);
        sourceTotals.set(groupKey, { ocnTotal, otnTotal, otnAssigned: false });
      }
      const nextRows = ocnOtnRows.map((row) => ({ ...row }));
      let keepCount = 0;
      let adjustCount = 0;
      let dropCount = 0;
      let singleSourceCount = 0;
      nextRows.forEach((row) => {
        const groupKey = getGroupKey(row);
        const groupRows = groupedRows.get(groupKey) ?? [];
        const totals = sourceTotals.get(groupKey);
        const source = toKey(row.source);
        const hasOcn = groupRows.some((item) => toKey(item.source) === "OCN");
        const hasOtn = groupRows.some((item) => toKey(item.source) === "OTN");
        row.original_fid = row.original_fid ?? row.fid;
        if (!(hasOcn && hasOtn) || !totals) {
          row.fid = row.original_fid;
          row.ocn_otn_decision = "KEEP_SINGLE_SOURCE";
          singleSourceCount += 1;
          return;
        }
        if (source === "OCN") {
          row.fid = row.original_fid;
          row.ocn_otn_decision = "KEEP_OCN_ORIGINAL";
          keepCount += 1;
          return;
        }
        if (source === "OTN") {
          const adjustedTotal = Math.max(totals.otnTotal - totals.ocnTotal, 0);
          if (adjustedTotal <= 0) {
            row.fid = 0;
            row.ocn_otn_decision = "DROP_OTN_FID_0_OCN_GE_OTN";
            dropCount += 1;
            return;
          }
          if (!totals.otnAssigned) {
            row.fid = adjustedTotal;
            row.ocn_otn_decision = "ADJUST_OTN_DIFF";
            totals.otnAssigned = true;
            adjustCount += 1;
          } else {
            row.fid = 0;
            row.ocn_otn_decision = "DROP_EXTRA_OTN_ROW_FID_0";
            dropCount += 1;
          }
          return;
        }
        row.fid = row.original_fid;
        row.ocn_otn_decision = "KEEP_SINGLE_SOURCE";
        singleSourceCount += 1;
      });
      setOcnOtnRows(nextRows);
      setOcnOtnDirty(true);
      setOcnOtnMessage(
        `OCN/OTN rule applied. Keep OCN: ${keepCount}, Adjusted OTN: ${adjustCount}, Dropped OTN(FID=0): ${dropCount}, Single-source kept: ${singleSourceCount}.`
      );
    } catch (err) {
      setOcnOtnError(err instanceof Error ? err.message : "Failed to apply OCN/OTN rule.");
    } finally {
      setOcnOtnRuleApplying(false);
    }
  }

  async function handleApplyCnxRule() {
    setCnxRuleApplying(true);
    setCnxError("");
    try {
      const nextRows = cnxRows.map((row) => ({ ...row }));
      let dropCount = 0;
      let keepCount = 0;
      for (const row of nextRows) {
        row.original_fid = row.original_fid ?? row.fid;
        const key = getGroupKey(row);
        const hasOther = cnxHasOtherSourceByGroup[key] ?? false;
        if (hasOther) {
          row.fid = 0;
          row.cnx_decision = "DROP_CNX_DOUBLE_SOURCE_FID_0";
          dropCount += 1;
        } else {
          row.fid = row.original_fid;
          row.cnx_decision = "KEEP_CNX_SINGLE_SOURCE";
          keepCount += 1;
        }
      }
      setCnxRows(nextRows);
      setCnxDirty(true);
      setCnxMessage(`CNX rule applied. Dropped CNX(FID=0): ${dropCount}, Kept single-source CNX: ${keepCount}.`);
    } catch (err) {
      setCnxError(err instanceof Error ? err.message : "Failed to apply CNX rule.");
    } finally {
      setCnxRuleApplying(false);
    }
  }

  async function handleApplyOhrPinRule() {
    setOhrPinRuleApplying(true);
    setOhrPinError("");
    try {
      const groupedRows = new Map<string, OhrPinCaseRow[]>();
      for (const row of ohrPinRows) {
        const key = getGroupKey(row);
        if (!groupedRows.has(key)) {
          groupedRows.set(key, []);
        }
        groupedRows.get(key)?.push(row);
      }
      const nextRows = ohrPinRows.map((row) => ({ ...row }));
      let keepCount = 0;
      let dropCount = 0;
      let singleSourceCount = 0;
      nextRows.forEach((row) => {
        const groupRows = groupedRows.get(getGroupKey(row)) ?? [];
        const source = toKey(row.source);
        const hasOhr = groupRows.some((item) => toKey(item.source) === "OHR");
        const hasPin = groupRows.some((item) => toKey(item.source) === "PIN");
        row.original_fid = row.original_fid ?? row.fid;
        if (!(hasOhr && hasPin)) {
          row.fid = row.original_fid;
          row.ohr_pin_decision = "KEEP_SINGLE_SOURCE";
          singleSourceCount += 1;
          return;
        }
        if (source === "OHR") {
          row.fid = row.original_fid;
          row.ohr_pin_decision = "KEEP_OHR_TRUSTED";
          keepCount += 1;
          return;
        }
        if (source === "PIN") {
          row.fid = 0;
          row.ohr_pin_decision = "DROP_PIN_FID_0_OHR_TRUSTED";
          dropCount += 1;
          return;
        }
        row.fid = row.original_fid;
        row.ohr_pin_decision = "KEEP_SINGLE_SOURCE";
        singleSourceCount += 1;
      });
      setOhrPinRows(nextRows);
      setOhrPinDirty(true);
      setOhrPinMessage(
        `OHR/PIN rule applied. Kept OHR: ${keepCount}, Dropped PIN(FID=0): ${dropCount}, Single-source kept: ${singleSourceCount}.`
      );
    } catch (err) {
      setOhrPinError(err instanceof Error ? err.message : "Failed to apply OHR/PIN rule.");
    } finally {
      setOhrPinRuleApplying(false);
    }
  }

  async function handleSaveDeleteCaseSelection() {
    const planningYear = requirePlanningYear();
    if (planningYear === null) {
      return;
    }
    type SaveTarget = {
      rows: OthDeletionFlagRow[];
      dirty: boolean;
      label: string;
      isCaseRow: (row: OthDeletionFlagRow) => boolean;
      onSaved: () => void;
      onError: (message: string) => void;
    };
    let target: SaveTarget | null = null;
    if (selectedDeleteCase === "OCN/OTN Case") {
      target = {
        rows: ocnOtnRows,
        dirty: ocnOtnDirty,
        label: "OCN/OTN Case",
        isCaseRow: (row) => {
          const source = toKey(row.source);
          return source === "OCN" || source === "OTN";
        },
        onSaved: () => {
          setOcnOtnSavedRows(ocnOtnRows.map((r) => ({ ...r })));
          setOcnOtnDirty(false);
          setOcnOtnMessage("OCN/OTN edits saved to backend working table.");
        },
        onError: setOcnOtnError,
      };
    } else if (selectedDeleteCase === "CNX Case") {
      target = {
        rows: cnxRows,
        dirty: cnxDirty,
        label: "CNX Case",
        isCaseRow: (row) => toKey(row.source) === "CNX",
        onSaved: () => {
          setCnxSavedRows(cnxRows.map((r) => ({ ...r })));
          setCnxDirty(false);
          setCnxMessage("CNX edits saved to backend working table.");
        },
        onError: setCnxError,
      };
    } else if (selectedDeleteCase === "OHR/PIN Case") {
      target = {
        rows: ohrPinRows,
        dirty: ohrPinDirty,
        label: "OHR/PIN Case",
        isCaseRow: (row) => {
          const source = toKey(row.source);
          return source === "OHR" || source === "PIN";
        },
        onSaved: () => {
          setOhrPinSavedRows(ohrPinRows.map((r) => ({ ...r })));
          setOhrPinDirty(false);
          setOhrPinMessage("OHR/PIN edits saved to backend working table.");
        },
        onError: setOhrPinError,
      };
    }
    if (!target) {
      setDoubleBrandMessage("No loaded delete-case data to save.");
      return;
    }
    if (!target.dirty) {
      setDoubleBrandMessage(`${target.label} has no pending changes to save.`);
      return;
    }
    setSnapshotSaving(true);
    try {
      const mergedRows = mergeCaseRowsIntoBaseRows(fullRows, target.rows, target.isCaseRow);
      const snapshot = await saveTotalMarketCalculationEligibleOthSnapshot({
        rows: mergedRows,
        message: `${target.label} snapshot saved from Restatement`,
        planning_year: planningYear,
        source_row_count: sourceRowCount > 0 ? sourceRowCount : mergedRows.length,
        split_machine_lines: splitMachineLines,
        split_input_rows: splitInputRows,
        split_output_rows: splitOutputRows,
        source_report_run_id: sourceReportRunId,
        source_report_created_at: sourceReportCreatedAt,
        three_check_report_run_id: threeCheckReportRunId,
        three_check_report_created_at: threeCheckReportCreatedAt,
      });
      const loaded = await loadLatestRestatementInput(planningYear);
      target.onSaved();
      setDoubleBrandMessage(
        `${target.label} saved (Run #${snapshot.run_id}). Rows: ${snapshot.previous_row_count ?? mergedRows.length} -> ${loaded.filteredCount}. Reloaded from backend calculated snapshot.`
      );
    } catch (err) {
      const m = err instanceof Error ? err.message : "Failed to save delete-case changes.";
      target.onError(m);
      setDoubleBrandError(m);
    } finally {
      setSnapshotSaving(false);
    }
  }

  const totalFid = rows.reduce((sum, row) => sum + toNumber(row.fid), 0);
  const restatementDisplayRows = restatementApplied ? restatementRows : rows;
  const restatementDisplayTotal = restatementApplied
    ? restatementRows.reduce((sum, row) => sum + toNumber(row.after_restatement), 0)
    : totalFid;

  return (
    <div className="page">
      <section className="section section--layer-detail-wide">
        <div className="section-header">
          <p className="section-tag">Restatement</p>
          <h1 className="section-title">Restatement</h1>
          <p className="section-description">Split-applied OTH Non VCE rows for restatement review.</p>
        </div>

        <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", alignItems: "center", marginBottom: "10px" }}>
          <label htmlFor="restatement-planning-year" style={{ fontWeight: 700 }}>
            Planning Year
          </label>
          <select
            id="restatement-planning-year"
            value={selectedPlanningYear}
            onChange={(event) => handlePlanningYearChange(event.target.value)}
            disabled={planningYearsLoading}
          >
            <option value="">Select year</option>
            {planningYears.map((year) => (
              <option key={year} value={String(year)}>
                {year}
              </option>
            ))}
          </select>
          <input
            type="number"
            min={1900}
            max={2999}
            step={1}
            placeholder="New year"
            value={newPlanningYearInput}
            onChange={(event) => setNewPlanningYearInput(event.target.value)}
            style={{ width: "110px" }}
          />
          <button type="button" onClick={handleCreatePlanningYear} disabled={creatingPlanningYear}>
            {creatingPlanningYear ? "Creating..." : "Create Year"}
          </button>
        </div>
        {planningYearsLoading ? <p style={{ color: "blue", marginBottom: "10px" }}>Loading years...</p> : null}
        {planningYearsError ? <p style={{ color: "red", marginBottom: "10px" }}>Error: {planningYearsError}</p> : null}
        {!hasSelectedPlanningYear ? (
          <p style={{ color: "#6b7280", marginBottom: "10px" }}>
            Select a planning year first, then run or show latest reports.
          </p>
        ) : null}

        {hasSelectedPlanningYear ? (
          <>
        <div className="overview-actions" style={{ marginBottom: "16px" }}>
          <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", alignItems: "flex-start" }}>
            <div style={{ display: "inline-flex", flexDirection: "column", gap: "8px", alignItems: "flex-start" }}>
              <button type="button" className="btn btn--overview" onClick={handleRun} disabled={loading}>
                {runLoading ? "Running..." : "OTH Non VCE Total Market Data"}
              </button>
              <button type="button" className="btn btn--tiny" onClick={handleShowLatest} disabled={loading}>
                Show latest
              </button>
            </div>
            <button type="button" className="btn btn--overview" onClick={handleReportCheckDoubleBrand}>
              Report check double brand
            </button>
            <button type="button" className="btn btn--overview" onClick={handleDeleteDoubleBrand}>
              Delete Double Brand
            </button>
            <button type="button" className="btn btn--overview" onClick={handleOpenRestatementPanel}>
              Restatement
            </button>
          </div>
        </div>

        {message ? <p style={{ color: "#0a8f3d", marginBottom: "12px" }}>{message}</p> : null}
        {error ? <p style={{ color: "#d62828", marginBottom: "12px" }}>{error}</p> : null}

        {activePanel === "data" ? (
          <>
            <div className="card-grid card-grid--three" style={{ marginBottom: "16px" }}>
              <article className="card">
                <h4 className="card__title">Loaded Rows</h4>
                <p className="card__text">{rows.length.toLocaleString()}</p>
              </article>
              <article className="card">
                <h4 className="card__title">Source Rows</h4>
                <p className="card__text">{sourceRowCount.toLocaleString()}</p>
              </article>
              <article className="card">
                <h4 className="card__title">FID Sum</h4>
                <p className="card__text">{totalFid.toLocaleString(undefined, { maximumFractionDigits: 2 })}</p>
              </article>
            </div>

            <div className="section summary-card" style={{ marginTop: "8px" }}>
              <strong>OTH Non VCE Total Market Data (Split Output)</strong>
              <FilterableTable
                columns={dataColumns}
                rows={rows}
                maxHeight="560px"
                compact
                emptyMessage='No OTH Non VCE rows loaded yet. Click "OTH Non VCE Total Market Data" to run, then "Show latest".'
              />
            </div>
          </>
        ) : null}

        {activePanel === "report" ? (
          <div id="restatement-panel-top" className="section summary-card" style={{ marginTop: "16px" }}>
            {doubleBrandMessage ? <p style={{ color: "#0a8f3d", marginBottom: "12px" }}>{doubleBrandMessage}</p> : null}
            {doubleBrandError ? <p style={{ color: "#d62828", marginBottom: "12px" }}>{doubleBrandError}</p> : null}
            <div className="card-grid card-grid--three" style={{ marginBottom: "16px" }}>
              <article className="card">
                <h4 className="card__title">Duplicate Rows</h4>
                <p className="card__text">{doubleBrandRows.length.toLocaleString()}</p>
              </article>
              <article className="card">
                <h4 className="card__title">Duplicate Groups</h4>
                <p className="card__text">{doubleBrandGroupCount.toLocaleString()}</p>
              </article>
              <article className="card">
                <h4 className="card__title">Scanned OTH Rows</h4>
                <p className="card__text">{doubleBrandSourceRowCount.toLocaleString()}</p>
              </article>
            </div>
            <strong>Report Check Double Brand</strong>
            <FilterableTable
              columns={reportColumns}
              rows={doubleBrandRows}
              maxHeight="520px"
              compact
              emptyMessage="No cross-source duplicate OTH rows found for the same country + machine line code + artificial machine line + size class + brand code."
            />
          </div>
        ) : null}

        {activePanel === "delete" ? (
          <div className="section summary-card" style={{ marginTop: "16px" }}>
            {doubleBrandMessage ? <p style={{ color: "#0a8f3d", marginBottom: "12px" }}>{doubleBrandMessage}</p> : null}
            {doubleBrandError ? <p style={{ color: "#d62828", marginBottom: "12px" }}>{doubleBrandError}</p> : null}

            <div className="tmc-delete-case-bar" style={{ marginBottom: "12px" }}>
              {["OCN/OTN Case", "CNX Case", "OHR/PIN Case"].map((caseName) => (
                <button
                  key={caseName}
                  type="button"
                  className={`tmc-delete-case-btn${selectedDeleteCase === caseName ? " tmc-delete-case-btn--active" : ""}`}
                  onClick={() => handleSelectDeleteCase(caseName)}
                >
                  {caseName}
                </button>
              ))}
              <button
                type="button"
                className="tmc-delete-case-btn tmc-delete-case-btn--save"
                onClick={handleSaveDeleteCaseSelection}
                disabled={snapshotSaving}
              >
                {snapshotSaving ? "Saving..." : "Save"}
              </button>
            </div>

            {selectedDeleteCase === "OCN/OTN Case" ? (
              <div className="section summary-card" style={{ marginTop: "8px" }}>
                <strong>OCN/OTN Case</strong>
                <div className="filterable-table__toolbar" style={{ marginTop: "8px" }}>
                  <button type="button" className="btn btn--tiny" onClick={handleApplyOcnOtnRule} disabled={ocnOtnRuleApplying}>
                    {ocnOtnRuleApplying ? "Applying..." : "Apply OCN/OTN Rule"}
                  </button>
                  <button type="button" className="btn btn--tiny" onClick={() => setOcnOtnEditMode((p) => !p)}>
                    {ocnOtnEditMode ? "Finish edit mode" : "Edit rows"}
                  </button>
                  <button type="button" className="btn btn--tiny" onClick={handleSaveDeleteCaseSelection} disabled={!ocnOtnDirty || snapshotSaving}>
                    Save edits
                  </button>
                  <button
                    type="button"
                    className="btn btn--tiny"
                    onClick={() => {
                      setOcnOtnRows(ocnOtnSavedRows.map((r) => ({ ...r })));
                      setOcnOtnDirty(false);
                      setOcnOtnMessage("OCN/OTN edits reverted to last saved state.");
                    }}
                    disabled={!ocnOtnDirty}
                  >
                    Reset edits
                  </button>
                </div>
                {ocnOtnMessage ? <p style={{ color: "#0a8f3d", marginTop: "10px" }}>{ocnOtnMessage}</p> : null}
                {ocnOtnError ? <p style={{ color: "#d62828", marginTop: "10px" }}>{ocnOtnError}</p> : null}
                <FilterableTable
                  columns={ocnOtnColumns}
                  rows={ocnOtnRows}
                  maxHeight="520px"
                  compact
                  editable={ocnOtnEditMode}
                  onRowsChange={(nextRows) => {
                    setOcnOtnRows((nextRows as OcnOtnCaseRow[]).map((row) => ({ ...row, source_flag: "OTH" })));
                    setOcnOtnDirty(true);
                  }}
                  onDeleteRow={ocnOtnEditMode ? (rowIndex) => {
                    setOcnOtnRows((prev) => prev.filter((_, idx) => idx !== rowIndex));
                    setOcnOtnDirty(true);
                  } : undefined}
                  nonEditableColumns={["original_fid", "source_flag", "ocn_otn_decision"]}
                  emptyMessage="No OCN/OTN rows found."
                />
              </div>
            ) : null}

            {selectedDeleteCase === "CNX Case" ? (
              <div className="section summary-card" style={{ marginTop: "8px" }}>
                <strong>CNX Case</strong>
                <div className="filterable-table__toolbar" style={{ marginTop: "8px" }}>
                  <button type="button" className="btn btn--tiny" onClick={handleApplyCnxRule} disabled={cnxRuleApplying}>
                    {cnxRuleApplying ? "Applying..." : "Apply CNX Rule"}
                  </button>
                  <button type="button" className="btn btn--tiny" onClick={() => setCnxEditMode((p) => !p)}>
                    {cnxEditMode ? "Finish edit mode" : "Edit rows"}
                  </button>
                  <button type="button" className="btn btn--tiny" onClick={handleSaveDeleteCaseSelection} disabled={!cnxDirty || snapshotSaving}>
                    Save edits
                  </button>
                  <button
                    type="button"
                    className="btn btn--tiny"
                    onClick={() => {
                      setCnxRows(cnxSavedRows.map((r) => ({ ...r })));
                      setCnxDirty(false);
                      setCnxMessage("CNX edits reverted to last saved state.");
                    }}
                    disabled={!cnxDirty}
                  >
                    Reset edits
                  </button>
                </div>
                {cnxMessage ? <p style={{ color: "#0a8f3d", marginTop: "10px" }}>{cnxMessage}</p> : null}
                {cnxError ? <p style={{ color: "#d62828", marginTop: "10px" }}>{cnxError}</p> : null}
                <FilterableTable
                  columns={cnxColumns}
                  rows={cnxRows}
                  maxHeight="520px"
                  compact
                  editable={cnxEditMode}
                  onRowsChange={(nextRows) => {
                    setCnxRows((nextRows as CnxCaseRow[]).map((row) => ({ ...row, source_flag: "OTH" })));
                    setCnxDirty(true);
                  }}
                  onDeleteRow={cnxEditMode ? (rowIndex) => {
                    setCnxRows((prev) => prev.filter((_, idx) => idx !== rowIndex));
                    setCnxDirty(true);
                  } : undefined}
                  nonEditableColumns={["original_fid", "source_flag", "cnx_decision"]}
                  emptyMessage="No CNX rows found."
                />
              </div>
            ) : null}

            {selectedDeleteCase === "OHR/PIN Case" ? (
              <div className="section summary-card" style={{ marginTop: "8px" }}>
                <strong>OHR/PIN Case</strong>
                <div className="filterable-table__toolbar" style={{ marginTop: "8px" }}>
                  <button type="button" className="btn btn--tiny" onClick={handleApplyOhrPinRule} disabled={ohrPinRuleApplying}>
                    {ohrPinRuleApplying ? "Applying..." : "Apply OHR/PIN Rule"}
                  </button>
                  <button type="button" className="btn btn--tiny" onClick={() => setOhrPinEditMode((p) => !p)}>
                    {ohrPinEditMode ? "Finish edit mode" : "Edit rows"}
                  </button>
                  <button type="button" className="btn btn--tiny" onClick={handleSaveDeleteCaseSelection} disabled={!ohrPinDirty || snapshotSaving}>
                    Save edits
                  </button>
                  <button
                    type="button"
                    className="btn btn--tiny"
                    onClick={() => {
                      setOhrPinRows(ohrPinSavedRows.map((r) => ({ ...r })));
                      setOhrPinDirty(false);
                      setOhrPinMessage("OHR/PIN edits reverted to last saved state.");
                    }}
                    disabled={!ohrPinDirty}
                  >
                    Reset edits
                  </button>
                </div>
                {ohrPinMessage ? <p style={{ color: "#0a8f3d", marginTop: "10px" }}>{ohrPinMessage}</p> : null}
                {ohrPinError ? <p style={{ color: "#d62828", marginTop: "10px" }}>{ohrPinError}</p> : null}
                <FilterableTable
                  columns={ohrPinColumns}
                  rows={ohrPinRows}
                  maxHeight="520px"
                  compact
                  editable={ohrPinEditMode}
                  onRowsChange={(nextRows) => {
                    setOhrPinRows((nextRows as OhrPinCaseRow[]).map((row) => ({ ...row, source_flag: "OTH" })));
                    setOhrPinDirty(true);
                  }}
                  onDeleteRow={ohrPinEditMode ? (rowIndex) => {
                    setOhrPinRows((prev) => prev.filter((_, idx) => idx !== rowIndex));
                    setOhrPinDirty(true);
                  } : undefined}
                  nonEditableColumns={["original_fid", "source_flag", "ohr_pin_decision"]}
                  emptyMessage="No OHR/PIN rows found."
                />
              </div>
            ) : null}
          </div>
        ) : null}

        {activePanel === "restatement" ? (
          <div className="section summary-card" style={{ marginTop: "16px" }}>
            {doubleBrandMessage ? <p style={{ color: "#0a8f3d", marginBottom: "12px" }}>{doubleBrandMessage}</p> : null}
            <div style={{ display: "inline-flex", alignItems: "center", gap: "10px", marginBottom: "8px", flexWrap: "wrap" }}>
              <strong>Restatement Final OTH Result (After Delete Double Brand)</strong>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={handleApplyRestatementRule}
                disabled={restatementApplying || rows.length === 0}
              >
                {restatementApplying ? "Applying..." : "Apply Restatement Rule"}
              </button>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={handleShowFinalRestatementResult}
                disabled={!restatementApplied || restatementApplying}
              >
                Final Restatement Result
              </button>
              {showFinalRestatementResult ? (
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={() => setShowFinalRestatementResult(false)}
                >
                  Back To Restatement Data
                </button>
              ) : null}
            </div>
            {restatementMessage ? <p style={{ color: "#0a8f3d", marginBottom: "12px" }}>{restatementMessage}</p> : null}
            {!showFinalRestatementResult ? (
              <>
                <div className="card-grid card-grid--three" style={{ marginTop: "12px", marginBottom: "12px" }}>
                  <article className="card">
                    <h4 className="card__title">Final OTH Rows</h4>
                    <p className="card__text">{restatementDisplayRows.length.toLocaleString()}</p>
                  </article>
                  <article className="card">
                    <h4 className="card__title">Source Rows</h4>
                    <p className="card__text">{sourceRowCount.toLocaleString()}</p>
                  </article>
                  <article className="card">
                    <h4 className="card__title">Final FID Sum</h4>
                    <p className="card__text">{restatementDisplayTotal.toLocaleString(undefined, { maximumFractionDigits: 2 })}</p>
                  </article>
                </div>
                <FilterableTable
                  columns={restatementColumns}
                  rows={restatementDisplayRows}
                  maxHeight="560px"
                  compact
                  emptyMessage='No final OTH rows loaded yet. Run "OTH Non VCE Total Market Data", click "Show latest", then apply/save Delete Double Brand rules.'
                />
              </>
            ) : (
              <div id="final-restatement-result" className="section summary-card" style={{ marginTop: "14px" }}>
                <strong>Final Restatement Result (TMA / SAL / Restated Non-VCE)</strong>
                <FilterableTable
                  columns={finalRestatementColumns}
                  rows={finalRestatementRows}
                  maxHeight="420px"
                  compact
                  emptyMessage='No final restatement result rows. Click "Apply Restatement Rule" first, then "Final Restatement Result".'
                />
              </div>
            )}
          </div>
        ) : null}
          </>
        ) : null}
      </section>
    </div>
  );
}

export default RestatementPage;
