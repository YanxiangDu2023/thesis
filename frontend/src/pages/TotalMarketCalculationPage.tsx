import { useMemo, useState } from "react";
import FilterableTable from "../components/table/FilterableTable";
import {
  getTotalMarketCalculationEligibleOthRun,
  getLatestTotalMarketCalculationEligibleOthRows,
  getTotalMarketCalculationDoubleBrandCheckRows,
  runTotalMarketCalculationEligibleOthReport,
  saveTotalMarketCalculationEligibleOthSnapshot,
} from "../api/uploads";
import type {
  OthDeletionFlagRow,
  TotalMarketCalculationDoubleBrandCheckRow,
} from "../types/upload";

type CompactionDoubleBrandRow = OthDeletionFlagRow & {
  db_indicator_by_country: number;
};

type YbrPinCaseRow = OthDeletionFlagRow & {
  source_flag: string;
  original_fid: string | number | null;
  ybr_pin_decision: string;
};

type OcnOtnCaseRow = OthDeletionFlagRow & {
  source_flag: string;
  original_fid: string | number | null;
  ocn_otn_decision: string;
};

type CmaOhrCaseRow = OthDeletionFlagRow & {
  source_flag: string;
  original_fid: string | number | null;
  cma_ohr_decision: string;
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

type RimPinCaseRow = OthDeletionFlagRow & {
  source_flag: string;
  original_fid: string | number | null;
  rim_pin_decision: string;
};

type ErgPinCaseRow = OthDeletionFlagRow & {
  source_flag: string;
  original_fid: string | number | null;
  erg_pin_decision: string;
};

function TotalMarketCalculationPage() {
  const [activeView, setActiveView] = useState<"raw" | "doubleBrand" | "deleteDoubleBrand">("raw");
  const [doubleBrandMode, setDoubleBrandMode] = useState<"all" | "namZar">("all");
  const [showDeleteCaseButtons, setShowDeleteCaseButtons] = useState(false);
  const [selectedDeleteCase, setSelectedDeleteCase] = useState("YBR/PIN Case");
  const [compactionLoading, setCompactionLoading] = useState(false);
  const [compactionRequested, setCompactionRequested] = useState(false);
  const [compactionRows, setCompactionRows] = useState<CompactionDoubleBrandRow[]>([]);
  const [compactionSavedRows, setCompactionSavedRows] = useState<CompactionDoubleBrandRow[]>([]);
  const [compactionSourceRowCount, setCompactionSourceRowCount] = useState(0);
  const [compactionMessage, setCompactionMessage] = useState("");
  const [compactionError, setCompactionError] = useState("");
  const [compactionEditMode, setCompactionEditMode] = useState(false);
  const [compactionDirty, setCompactionDirty] = useState(false);
  const [ybrPinRequested, setYbrPinRequested] = useState(false);
  const [ybrPinLoading, setYbrPinLoading] = useState(false);
  const [ybrPinRuleApplying, setYbrPinRuleApplying] = useState(false);
  const [ybrPinRows, setYbrPinRows] = useState<YbrPinCaseRow[]>([]);
  const [ybrPinSavedRows, setYbrPinSavedRows] = useState<YbrPinCaseRow[]>([]);
  const [ybrPinMessage, setYbrPinMessage] = useState("");
  const [ybrPinError, setYbrPinError] = useState("");
  const [ybrPinEditMode, setYbrPinEditMode] = useState(false);
  const [ybrPinDirty, setYbrPinDirty] = useState(false);
  const [ybrPinRuleApplied, setYbrPinRuleApplied] = useState(false);
  const [ocnOtnRequested, setOcnOtnRequested] = useState(false);
  const [ocnOtnLoading, setOcnOtnLoading] = useState(false);
  const [ocnOtnRuleApplying, setOcnOtnRuleApplying] = useState(false);
  const [ocnOtnRows, setOcnOtnRows] = useState<OcnOtnCaseRow[]>([]);
  const [ocnOtnSavedRows, setOcnOtnSavedRows] = useState<OcnOtnCaseRow[]>([]);
  const [ocnOtnMessage, setOcnOtnMessage] = useState("");
  const [ocnOtnError, setOcnOtnError] = useState("");
  const [ocnOtnEditMode, setOcnOtnEditMode] = useState(false);
  const [ocnOtnDirty, setOcnOtnDirty] = useState(false);
  const [ocnOtnRuleApplied, setOcnOtnRuleApplied] = useState(false);
  const [cmaOhrRequested, setCmaOhrRequested] = useState(false);
  const [cmaOhrLoading, setCmaOhrLoading] = useState(false);
  const [cmaOhrRuleApplying, setCmaOhrRuleApplying] = useState(false);
  const [cmaOhrRows, setCmaOhrRows] = useState<CmaOhrCaseRow[]>([]);
  const [cmaOhrSavedRows, setCmaOhrSavedRows] = useState<CmaOhrCaseRow[]>([]);
  const [cmaOhrMessage, setCmaOhrMessage] = useState("");
  const [cmaOhrError, setCmaOhrError] = useState("");
  const [cmaOhrEditMode, setCmaOhrEditMode] = useState(false);
  const [cmaOhrDirty, setCmaOhrDirty] = useState(false);
  const [cmaOhrRuleApplied, setCmaOhrRuleApplied] = useState(false);
  const [cnxRequested, setCnxRequested] = useState(false);
  const [cnxLoading, setCnxLoading] = useState(false);
  const [cnxRuleApplying, setCnxRuleApplying] = useState(false);
  const [cnxRows, setCnxRows] = useState<CnxCaseRow[]>([]);
  const [cnxSavedRows, setCnxSavedRows] = useState<CnxCaseRow[]>([]);
  const [cnxMessage, setCnxMessage] = useState("");
  const [cnxError, setCnxError] = useState("");
  const [cnxEditMode, setCnxEditMode] = useState(false);
  const [cnxDirty, setCnxDirty] = useState(false);
  const [cnxRuleApplied, setCnxRuleApplied] = useState(false);
  const [cnxHasOtherSourceByGroup, setCnxHasOtherSourceByGroup] = useState<Record<string, boolean>>({});
  const [ohrPinRequested, setOhrPinRequested] = useState(false);
  const [ohrPinLoading, setOhrPinLoading] = useState(false);
  const [ohrPinRuleApplying, setOhrPinRuleApplying] = useState(false);
  const [ohrPinRows, setOhrPinRows] = useState<OhrPinCaseRow[]>([]);
  const [ohrPinSavedRows, setOhrPinSavedRows] = useState<OhrPinCaseRow[]>([]);
  const [ohrPinMessage, setOhrPinMessage] = useState("");
  const [ohrPinError, setOhrPinError] = useState("");
  const [ohrPinEditMode, setOhrPinEditMode] = useState(false);
  const [ohrPinDirty, setOhrPinDirty] = useState(false);
  const [ohrPinRuleApplied, setOhrPinRuleApplied] = useState(false);
  const [rimPinRequested, setRimPinRequested] = useState(false);
  const [rimPinLoading, setRimPinLoading] = useState(false);
  const [rimPinRuleApplying, setRimPinRuleApplying] = useState(false);
  const [rimPinRows, setRimPinRows] = useState<RimPinCaseRow[]>([]);
  const [rimPinSavedRows, setRimPinSavedRows] = useState<RimPinCaseRow[]>([]);
  const [rimPinMessage, setRimPinMessage] = useState("");
  const [rimPinError, setRimPinError] = useState("");
  const [rimPinEditMode, setRimPinEditMode] = useState(false);
  const [rimPinDirty, setRimPinDirty] = useState(false);
  const [rimPinRuleApplied, setRimPinRuleApplied] = useState(false);
  const [ergPinRequested, setErgPinRequested] = useState(false);
  const [ergPinLoading, setErgPinLoading] = useState(false);
  const [ergPinRuleApplying, setErgPinRuleApplying] = useState(false);
  const [ergPinRows, setErgPinRows] = useState<ErgPinCaseRow[]>([]);
  const [ergPinSavedRows, setErgPinSavedRows] = useState<ErgPinCaseRow[]>([]);
  const [ergPinMessage, setErgPinMessage] = useState("");
  const [ergPinError, setErgPinError] = useState("");
  const [ergPinEditMode, setErgPinEditMode] = useState(false);
  const [ergPinDirty, setErgPinDirty] = useState(false);
  const [ergPinRuleApplied, setErgPinRuleApplied] = useState(false);
  const [snapshotSaving, setSnapshotSaving] = useState(false);
  const [loading, setLoading] = useState(false);
  const [latestLoading, setLatestLoading] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [rows, setRows] = useState<OthDeletionFlagRow[]>([]);
  const [workflowRows, setWorkflowRows] = useState<OthDeletionFlagRow[]>([]);
  const [sourceRowCount, setSourceRowCount] = useState(0);
  const [splitMachineLines, setSplitMachineLines] = useState<string[]>([]);
  const [splitInputRows, setSplitInputRows] = useState<number | undefined>(undefined);
  const [splitOutputRows, setSplitOutputRows] = useState<number | undefined>(undefined);
  const [sourceReportRunId, setSourceReportRunId] = useState<number | undefined>(undefined);
  const [sourceReportCreatedAt, setSourceReportCreatedAt] = useState<string | undefined>(undefined);
  const [threeCheckReportRunId, setThreeCheckReportRunId] = useState<number | undefined>(undefined);
  const [threeCheckReportCreatedAt, setThreeCheckReportCreatedAt] = useState<string | undefined>(undefined);
  const [doubleBrandLoading, setDoubleBrandLoading] = useState(false);
  const [doubleBrandError, setDoubleBrandError] = useState("");
  const [doubleBrandMessage, setDoubleBrandMessage] = useState("");
  const [doubleBrandRows, setDoubleBrandRows] = useState<TotalMarketCalculationDoubleBrandCheckRow[]>([]);
  const [doubleBrandGroupCount, setDoubleBrandGroupCount] = useState(0);
  const [doubleBrandSourceRowCount, setDoubleBrandSourceRowCount] = useState(0);
  const deleteCaseButtons = useMemo(
    () => [
      "YBR/PIN Case",
      "OCN/OTN Case",
      "CMA/OHR Case",
      "CNX Case",
      "OHR/PIN Case",
      "RIM/PIN Case",
      "ERG/PIN Case",
    ],
    []
  );

  const columns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "source", label: "Source" },
      { key: "country_code", label: "Country Code" },
      { key: "country", label: "Country" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "region", label: "Region" },
      { key: "market_area", label: "Market Area" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "artificial_machine_line", label: "Artificial Machine Line" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "fid", label: "FID" },
      { key: "deletion_flag", label: "Deletion Flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "source_flag", label: "Source Flag" },
    ],
    []
  );

  const doubleBrandColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "source", label: "Source" },
      { key: "country_code", label: "Country Code" },
      { key: "country", label: "Country" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "region", label: "Region" },
      { key: "market_area", label: "Market Area" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "artificial_machine_line", label: "Artificial Machine Line" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class_flag", label: "Size Class" },
      { key: "fid", label: "FID" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "source_flag", label: "Source Flag" },
      { key: "distinct_source_count", label: "Distinct Sources" },
      { key: "distinct_sources", label: "Source Set" },
    ],
    []
  );

  const compactionColumns = useMemo(
    () => [...columns, { key: "db_indicator_by_country", label: "DB indicator by country" }],
    [columns]
  );

  const ybrPinColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "source", label: "Source" },
      { key: "country_code", label: "Country Code" },
      { key: "country", label: "Country" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "region", label: "Region" },
      { key: "market_area", label: "Market Area" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "artificial_machine_line", label: "Artificial Machine Line" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "original_fid", label: "Original FID" },
      { key: "fid", label: "FID (After Rule)" },
      { key: "ybr_pin_decision", label: "YBR/PIN Decision" },
      { key: "deletion_flag", label: "Deletion Flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "source_flag", label: "Source Flag" },
    ],
    []
  );

  const ocnOtnColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "source", label: "Source" },
      { key: "country_code", label: "Country Code" },
      { key: "country", label: "Country" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "region", label: "Region" },
      { key: "market_area", label: "Market Area" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "artificial_machine_line", label: "Artificial Machine Line" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "original_fid", label: "Original FID" },
      { key: "fid", label: "FID (After Rule)" },
      { key: "ocn_otn_decision", label: "OCN/OTN Decision" },
      { key: "deletion_flag", label: "Deletion Flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "source_flag", label: "Source Flag" },
    ],
    []
  );

  const cmaOhrColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "source", label: "Source" },
      { key: "country_code", label: "Country Code" },
      { key: "country", label: "Country" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "region", label: "Region" },
      { key: "market_area", label: "Market Area" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "artificial_machine_line", label: "Artificial Machine Line" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "original_fid", label: "Original FID" },
      { key: "fid", label: "FID (After Rule)" },
      { key: "cma_ohr_decision", label: "CMA/OHR Decision" },
      { key: "deletion_flag", label: "Deletion Flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "source_flag", label: "Source Flag" },
    ],
    []
  );

  const cnxColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "source", label: "Source" },
      { key: "country_code", label: "Country Code" },
      { key: "country", label: "Country" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "region", label: "Region" },
      { key: "market_area", label: "Market Area" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "artificial_machine_line", label: "Artificial Machine Line" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "original_fid", label: "Original FID" },
      { key: "fid", label: "FID (After Rule)" },
      { key: "cnx_decision", label: "CNX Decision" },
      { key: "deletion_flag", label: "Deletion Flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "source_flag", label: "Source Flag" },
    ],
    []
  );

  const ohrPinColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "source", label: "Source" },
      { key: "country_code", label: "Country Code" },
      { key: "country", label: "Country" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "region", label: "Region" },
      { key: "market_area", label: "Market Area" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "artificial_machine_line", label: "Artificial Machine Line" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "original_fid", label: "Original FID" },
      { key: "fid", label: "FID (After Rule)" },
      { key: "ohr_pin_decision", label: "OHR/PIN Decision" },
      { key: "deletion_flag", label: "Deletion Flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "source_flag", label: "Source Flag" },
    ],
    []
  );

  const rimPinColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "source", label: "Source" },
      { key: "country_code", label: "Country Code" },
      { key: "country", label: "Country" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "region", label: "Region" },
      { key: "market_area", label: "Market Area" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "artificial_machine_line", label: "Artificial Machine Line" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "original_fid", label: "Original FID" },
      { key: "fid", label: "FID (After Rule)" },
      { key: "rim_pin_decision", label: "RIM/PIN Decision" },
      { key: "deletion_flag", label: "Deletion Flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "source_flag", label: "Source Flag" },
    ],
    []
  );

  const ergPinColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "source", label: "Source" },
      { key: "country_code", label: "Country Code" },
      { key: "country", label: "Country" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "region", label: "Region" },
      { key: "market_area", label: "Market Area" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "artificial_machine_line", label: "Artificial Machine Line" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "original_fid", label: "Original FID" },
      { key: "fid", label: "FID (After Rule)" },
      { key: "erg_pin_decision", label: "ERG/PIN Decision" },
      { key: "deletion_flag", label: "Deletion Flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "source_flag", label: "Source Flag" },
    ],
    []
  );

  function getDuplicateGroupKey(row: TotalMarketCalculationDoubleBrandCheckRow): string {
    return [
      String(row.country ?? "").trim().toUpperCase(),
      String(row.machine_line_code ?? "").trim().toUpperCase(),
      String(row.artificial_machine_line ?? "").trim().toUpperCase(),
      String(row.size_class_flag ?? "").trim().toUpperCase(),
      String(row.brand_code ?? "").trim().toUpperCase(),
    ].join("||");
  }

  function isNamZarDuplicate(row: TotalMarketCalculationDoubleBrandCheckRow): boolean {
    const tokens = String(row.distinct_sources ?? "")
      .split(",")
      .map((item) => item.trim().toUpperCase())
      .filter(Boolean);
    return tokens.includes("NAM") && tokens.includes("ZAR");
  }

  function toKey(value: string | number | null | undefined): string {
    return String(value ?? "").trim().toUpperCase();
  }

  function isCompactionMachineRow(row: OthDeletionFlagRow): boolean {
    const machineLineName = toKey(row.machine_line_name);
    const artificialMachineLine = toKey(row.artificial_machine_line);
    const source = toKey(row.source);
    if (source === "TMA" || source === "SAL") {
      return false;
    }
    return (
      machineLineName.includes("COMPACTION MACHINE")
      || artificialMachineLine.includes("COMPACTION MACHINE")
    );
  }

  function getCompactionDuplicateGroupKey(row: OthDeletionFlagRow): string {
    return [
      toKey(row.country),
      toKey(row.machine_line_name),
      toKey(row.size_class_flag),
      toKey(row.brand_code || row.brand_name),
    ].join("||");
  }

  function getYbrPinGroupKey(row: OthDeletionFlagRow): string {
    return [
      toKey(row.country),
      toKey(row.machine_line_code || row.machine_line_name),
      toKey(row.size_class_flag),
      toKey(row.brand_code || row.brand_name),
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
      const key = getYbrPinGroupKey(row);
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

    return candidateRows.filter((row) => qualifiedGroupKeys.has(getYbrPinGroupKey(row)));
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
        continue;
      }
      const edited = queue.shift();
      if (!edited) {
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

  function applyLoadedRawRows(result: Awaited<ReturnType<typeof getLatestTotalMarketCalculationEligibleOthRows>>) {
    const normalizedRows = result.rows.map((row) => ({ ...row, source_flag: "OTH" }));
    setRows(normalizedRows);
    setSourceRowCount(result.source_row_count);
    setSplitMachineLines(result.split_machine_lines);
  }

  function applyLoadedWorkflowRows(
    result: Awaited<ReturnType<typeof getLatestTotalMarketCalculationEligibleOthRows>>
  ) {
    const normalizedRows = result.rows.map((row) => ({ ...row, source_flag: "OTH" }));
    setWorkflowRows(normalizedRows);
    setSourceRowCount(result.source_row_count);
    setSplitMachineLines(result.split_machine_lines);
    setSplitInputRows(result.split_input_rows);
    setSplitOutputRows(result.split_output_rows);
    setSourceReportRunId(result.source_report_run_id);
    setSourceReportCreatedAt(result.source_report_created_at);
    setThreeCheckReportRunId(result.three_check_report_run_id);
    setThreeCheckReportCreatedAt(result.three_check_report_created_at);
  }

  async function ensureWorkflowRowsLoaded(): Promise<OthDeletionFlagRow[]> {
    if (workflowRows.length > 0) {
      return workflowRows;
    }
    const latest = await getLatestTotalMarketCalculationEligibleOthRows();
    applyLoadedWorkflowRows(latest);
    return latest.rows.map((row) => ({ ...row, source_flag: "OTH" }));
  }

  async function saveCaseRowsToBackend(
    baseRows: OthDeletionFlagRow[],
    editedRows: OthDeletionFlagRow[],
    isCaseRow: (row: OthDeletionFlagRow) => boolean,
    message: string
  ): Promise<{ runId: number; mergedRows: OthDeletionFlagRow[] }> {
    const mergedRows = mergeCaseRowsIntoBaseRows(baseRows, editedRows, isCaseRow);
    const snapshot = await saveTotalMarketCalculationEligibleOthSnapshot({
      rows: mergedRows,
      message,
      source_row_count: sourceRowCount > 0 ? sourceRowCount : baseRows.length,
      split_machine_lines: splitMachineLines,
      split_input_rows: splitInputRows,
      split_output_rows: splitOutputRows,
      source_report_run_id: sourceReportRunId,
      source_report_created_at: sourceReportCreatedAt,
      three_check_report_run_id: threeCheckReportRunId,
      three_check_report_created_at: threeCheckReportCreatedAt,
    });
    return {
      runId: snapshot.run_id,
      mergedRows,
    };
  }

  function buildCompactionRowsWithIndicator(
    inputRows: Array<OthDeletionFlagRow | CompactionDoubleBrandRow>
  ): CompactionDoubleBrandRow[] {
    const groupCountMap = new Map<string, number>();

    for (const row of inputRows) {
      const key = getCompactionDuplicateGroupKey(row);
      groupCountMap.set(key, (groupCountMap.get(key) ?? 0) + 1);
    }

    return inputRows.map((row) => {
      const key = getCompactionDuplicateGroupKey(row);
      return {
        ...row,
        source_flag: "OTH",
        db_indicator_by_country: groupCountMap.get(key) ?? 1,
      };
    });
  }

  function cloneCompactionRows(rowsToClone: CompactionDoubleBrandRow[]): CompactionDoubleBrandRow[] {
    return rowsToClone.map((row) => ({ ...row }));
  }

  function cloneYbrPinRows(rowsToClone: YbrPinCaseRow[]): YbrPinCaseRow[] {
    return rowsToClone.map((row) => ({ ...row }));
  }

  function cloneOcnOtnRows(rowsToClone: OcnOtnCaseRow[]): OcnOtnCaseRow[] {
    return rowsToClone.map((row) => ({ ...row }));
  }

  function cloneCmaOhrRows(rowsToClone: CmaOhrCaseRow[]): CmaOhrCaseRow[] {
    return rowsToClone.map((row) => ({ ...row }));
  }

  function cloneCnxRows(rowsToClone: CnxCaseRow[]): CnxCaseRow[] {
    return rowsToClone.map((row) => ({ ...row }));
  }

  function cloneOhrPinRows(rowsToClone: OhrPinCaseRow[]): OhrPinCaseRow[] {
    return rowsToClone.map((row) => ({ ...row }));
  }

  function cloneRimPinRows(rowsToClone: RimPinCaseRow[]): RimPinCaseRow[] {
    return rowsToClone.map((row) => ({ ...row }));
  }

  function cloneErgPinRows(rowsToClone: ErgPinCaseRow[]): ErgPinCaseRow[] {
    return rowsToClone.map((row) => ({ ...row }));
  }

  function resetRimPinCaseState() {
    setRimPinRequested(false);
    setRimPinRows([]);
    setRimPinSavedRows([]);
    setRimPinMessage("");
    setRimPinError("");
    setRimPinEditMode(false);
    setRimPinDirty(false);
    setRimPinRuleApplied(false);
  }

  function resetErgPinCaseState() {
    setErgPinRequested(false);
    setErgPinRows([]);
    setErgPinSavedRows([]);
    setErgPinMessage("");
    setErgPinError("");
    setErgPinEditMode(false);
    setErgPinDirty(false);
    setErgPinRuleApplied(false);
  }

  async function handleCalculateTotalMarket() {
    setActiveView("raw");
    setShowDeleteCaseButtons(false);
    setLoading(true);
    setError("");
    setMessage("Starting Total Market Calculation run...");

    try {
      const started = await runTotalMarketCalculationEligibleOthReport();
      setMessage(`Run #${started.run_id} started. Waiting for completion...`);

      const maxAttempts = 300;
      let finished = false;

      for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
        const run = await getTotalMarketCalculationEligibleOthRun(started.run_id);
        if (run.status === "success") {
          setMessage(
            `Run successful. Row Count: ${run.row_count ?? 0} (Run #${run.run_id}). Click Show latest to load.`
          );
          finished = true;
          break;
        }

        if (run.status === "failed") {
          throw new Error(run.message || `Run #${run.run_id} failed.`);
        }

        setMessage(`Run #${run.run_id} is running... (${attempt}/${maxAttempts})`);
        await new Promise((resolve) => window.setTimeout(resolve, 2000));
      }

      if (!finished) {
        throw new Error(
          `Run did not finish in time. Please click Show latest after a while to check results.`
        );
      }
    } catch (fetchError) {
      setError(
        fetchError instanceof Error
          ? fetchError.message
          : "Failed to run Total Market Calculation."
      );
      setMessage("");
    } finally {
      setLoading(false);
    }
  }

  async function handleShowLatestRaw() {
    setActiveView("raw");
    setShowDeleteCaseButtons(false);
    setLatestLoading(true);
    setError("");
    setMessage("Loading latest saved Total Market Calculation rows...");

    try {
      const result = await getLatestTotalMarketCalculationEligibleOthRows();
      applyLoadedRawRows(result);
      setMessage(
        `Latest loaded. Row Count: ${result.row_count}${result.run_id ? ` (Run #${result.run_id})` : ""}.`
      );
    } catch (fetchError) {
      setError(
        fetchError instanceof Error
          ? fetchError.message
          : "Failed to load latest Total Market Calculation rows."
      );
      setMessage("");
    } finally {
      setLatestLoading(false);
    }
  }

  async function handleReportCheckDoubleBrand() {
    setActiveView("doubleBrand");
    setDoubleBrandMode("all");
    setShowDeleteCaseButtons(false);
    setDoubleBrandLoading(true);
    setDoubleBrandError("");
    setDoubleBrandMessage("Checking OTH duplicate groups across different sources...");

    try {
      const result = await getTotalMarketCalculationDoubleBrandCheckRows();
      setDoubleBrandRows(result.rows.map((row) => ({ ...row, source_flag: "OTH" })));
      setDoubleBrandGroupCount(result.duplicate_group_count);
      setDoubleBrandSourceRowCount(result.source_row_count);
      setDoubleBrandMessage(
        `Found ${result.row_count} rows across ${result.duplicate_group_count} duplicate groups (same country + machine line code + artificial machine line + size class + brand code, but different source).`
      );
    } catch (fetchError) {
      setDoubleBrandError(
        fetchError instanceof Error
          ? fetchError.message
          : "Failed to run Report check double brand."
      );
      setDoubleBrandMessage("");
    } finally {
      setDoubleBrandLoading(false);
    }
  }

  async function handleReportCheckDoubleBrandNamZar() {
    setActiveView("doubleBrand");
    setDoubleBrandMode("namZar");
    setShowDeleteCaseButtons(false);
    setDoubleBrandLoading(true);
    setDoubleBrandError("");
    setDoubleBrandMessage("Checking NAM/ZAR duplicate groups...");

    try {
      const result = await getTotalMarketCalculationDoubleBrandCheckRows();
      const filteredRows = result.rows
        .filter(isNamZarDuplicate)
        .map((row) => ({ ...row, source_flag: "OTH" }));
      const filteredGroupCount = new Set(filteredRows.map(getDuplicateGroupKey)).size;

      setDoubleBrandRows(filteredRows);
      setDoubleBrandGroupCount(filteredGroupCount);
      setDoubleBrandSourceRowCount(result.source_row_count);
      setDoubleBrandMessage(
        `Found ${filteredRows.length} rows across ${filteredGroupCount} duplicate groups for source pair NAM/ZAR.`
      );
    } catch (fetchError) {
      setDoubleBrandError(
        fetchError instanceof Error
          ? fetchError.message
          : "Failed to run Check Double Brand (NAM, ZAR)."
      );
      setDoubleBrandMessage("");
    } finally {
      setDoubleBrandLoading(false);
    }
  }

  function handleDeleteDoubleBrand() {
    setActiveView("deleteDoubleBrand");
    setShowDeleteCaseButtons(true);
    setCompactionRequested(false);
    setCompactionRows([]);
    setCompactionSavedRows([]);
    setCompactionSourceRowCount(0);
    setCompactionMessage("");
    setCompactionError("");
    setCompactionEditMode(false);
    setCompactionDirty(false);
    setYbrPinRequested(false);
    setYbrPinRows([]);
    setYbrPinSavedRows([]);
    setYbrPinMessage("");
    setYbrPinError("");
    setYbrPinEditMode(false);
    setYbrPinDirty(false);
    setYbrPinRuleApplied(false);
    setOcnOtnRequested(false);
    setOcnOtnRows([]);
    setOcnOtnSavedRows([]);
    setOcnOtnMessage("");
    setOcnOtnError("");
    setOcnOtnEditMode(false);
    setOcnOtnDirty(false);
    setOcnOtnRuleApplied(false);
    setCmaOhrRequested(false);
    setCmaOhrRows([]);
    setCmaOhrSavedRows([]);
    setCmaOhrMessage("");
    setCmaOhrError("");
    setCmaOhrEditMode(false);
    setCmaOhrDirty(false);
    setCmaOhrRuleApplied(false);
    setCnxRequested(false);
    setCnxRows([]);
    setCnxSavedRows([]);
    setCnxMessage("");
    setCnxError("");
    setCnxEditMode(false);
    setCnxDirty(false);
    setCnxRuleApplied(false);
    setCnxHasOtherSourceByGroup({});
    setOhrPinRequested(false);
    setOhrPinRows([]);
    setOhrPinSavedRows([]);
    setOhrPinMessage("");
    setOhrPinError("");
    setOhrPinEditMode(false);
    setOhrPinDirty(false);
    setOhrPinRuleApplied(false);
    resetRimPinCaseState();
    resetErgPinCaseState();
    setDoubleBrandError("");
    setDoubleBrandMessage("Delete Double Brand action is ready. Select case buttons below.");
  }

  async function handleLoadDeleteDoubleBrandCompactionMachine() {
    setYbrPinRequested(false);
    setYbrPinRows([]);
    setYbrPinSavedRows([]);
    setYbrPinMessage("");
    setYbrPinError("");
    setYbrPinEditMode(false);
    setYbrPinDirty(false);
    setYbrPinRuleApplied(false);
    setOcnOtnRequested(false);
    setOcnOtnRows([]);
    setOcnOtnSavedRows([]);
    setOcnOtnMessage("");
    setOcnOtnError("");
    setOcnOtnEditMode(false);
    setOcnOtnDirty(false);
    setOcnOtnRuleApplied(false);
    setCmaOhrRequested(false);
    setCmaOhrRows([]);
    setCmaOhrSavedRows([]);
    setCmaOhrMessage("");
    setCmaOhrError("");
    setCmaOhrEditMode(false);
    setCmaOhrDirty(false);
    setCmaOhrRuleApplied(false);
    setCnxRequested(false);
    setCnxRows([]);
    setCnxSavedRows([]);
    setCnxMessage("");
    setCnxError("");
    setCnxEditMode(false);
    setCnxDirty(false);
    setCnxRuleApplied(false);
    setCnxHasOtherSourceByGroup({});
    setOhrPinRequested(false);
    setOhrPinRows([]);
    setOhrPinSavedRows([]);
    setOhrPinMessage("");
    setOhrPinError("");
    setOhrPinEditMode(false);
    setOhrPinDirty(false);
    setOhrPinRuleApplied(false);
    resetRimPinCaseState();
    resetErgPinCaseState();
    setCompactionRequested(true);
    setCompactionLoading(true);
    setCompactionError("");
    setCompactionMessage("Loading Compaction Machine OTH rows...");

    try {
      const baseRows = await ensureWorkflowRowsLoaded();
      const compactionOnlyRows = baseRows.filter(isCompactionMachineRow);
      const withIndicator = buildCompactionRowsWithIndicator(compactionOnlyRows);

      withIndicator.sort((a, b) => {
        const keyA = [
          toKey(a.country),
          toKey(a.machine_line_name),
          toKey(a.size_class_flag),
          toKey(a.brand_code || a.brand_name),
          toKey(a.source),
        ].join("|");
        const keyB = [
          toKey(b.country),
          toKey(b.machine_line_name),
          toKey(b.size_class_flag),
          toKey(b.brand_code || b.brand_name),
          toKey(b.source),
        ].join("|");
        return keyA.localeCompare(keyB);
      });

      setCompactionRows(withIndicator);
      setCompactionSavedRows(cloneCompactionRows(withIndicator));
      setCompactionSourceRowCount(compactionOnlyRows.length);
      setCompactionEditMode(false);
      setCompactionDirty(false);
      setCompactionMessage(
        `Loaded ${withIndicator.length} Compaction Machine OTH rows. DB indicator by country is computed per country + machine line + size class + brand.`
      );
    } catch (err) {
      setCompactionRows([]);
      setCompactionSourceRowCount(0);
      setCompactionError(
        err instanceof Error
          ? err.message
          : "Failed to load Delete Double Brand (Compaction Machine) rows."
      );
      setCompactionMessage("");
    } finally {
      setCompactionLoading(false);
    }
  }

  function handleSelectDeleteCase(caseName: string) {
    setSelectedDeleteCase(caseName);
    if (caseName === "YBR/PIN Case") {
      void handleLoadYbrPinCase();
      return;
    }
    if (caseName === "OCN/OTN Case") {
      void handleLoadOcnOtnCase();
      return;
    }
    if (caseName === "CMA/OHR Case") {
      void handleLoadCmaOhrCase();
      return;
    }
    if (caseName === "CNX Case") {
      void handleLoadCnxCase();
      return;
    }
    if (caseName === "OHR/PIN Case") {
      void handleLoadOhrPinCase();
      return;
    }
    if (caseName === "RIM/PIN Case") {
      void handleLoadRimPinCase();
      return;
    }
    if (caseName === "ERG/PIN Case") {
      void handleLoadErgPinCase();
      return;
    }

    setYbrPinRequested(false);
    setYbrPinRows([]);
    setYbrPinSavedRows([]);
    setYbrPinMessage("");
    setYbrPinError("");
    setYbrPinEditMode(false);
    setYbrPinDirty(false);
    setYbrPinRuleApplied(false);
    setOcnOtnRequested(false);
    setOcnOtnRows([]);
    setOcnOtnSavedRows([]);
    setOcnOtnMessage("");
    setOcnOtnError("");
    setOcnOtnEditMode(false);
    setOcnOtnDirty(false);
    setOcnOtnRuleApplied(false);
    setCmaOhrRequested(false);
    setCmaOhrRows([]);
    setCmaOhrSavedRows([]);
    setCmaOhrMessage("");
    setCmaOhrError("");
    setCmaOhrEditMode(false);
    setCmaOhrDirty(false);
    setCmaOhrRuleApplied(false);
    setCnxRequested(false);
    setCnxRows([]);
    setCnxSavedRows([]);
    setCnxMessage("");
    setCnxError("");
    setCnxEditMode(false);
    setCnxDirty(false);
    setCnxRuleApplied(false);
    setCnxHasOtherSourceByGroup({});
    setOhrPinRequested(false);
    setOhrPinRows([]);
    setOhrPinSavedRows([]);
    setOhrPinMessage("");
    setOhrPinError("");
    setOhrPinEditMode(false);
    setOhrPinDirty(false);
    setOhrPinRuleApplied(false);
    resetRimPinCaseState();
    resetErgPinCaseState();
    setDoubleBrandMessage(`Selected ${caseName}.`);
  }

  async function handleSaveDeleteCaseSelection() {
    type SaveTarget = {
      rows: OthDeletionFlagRow[];
      dirty: boolean;
      label: string;
      isCaseRow: (row: OthDeletionFlagRow) => boolean;
      markSaved: () => void;
      setInfo: (message: string) => void;
      setError: (message: string) => void;
      clearError: () => void;
    };

    let target: SaveTarget | null = null;

    if (compactionRequested) {
      target = {
        rows: compactionRows,
        dirty: compactionDirty,
        label: "Delete Double Brand (Compaction Machine)",
        isCaseRow: (row) => isCompactionMachineRow(row),
        markSaved: () => {
          setCompactionSavedRows(cloneCompactionRows(compactionRows));
          setCompactionDirty(false);
          setCompactionMessage("Compaction edits saved to backend raw data.");
        },
        setInfo: setCompactionMessage,
        setError: setCompactionError,
        clearError: () => setCompactionError(""),
      };
    } else if (ybrPinRequested) {
      target = {
        rows: ybrPinRows,
        dirty: ybrPinDirty,
        label: "YBR/PIN Case",
        isCaseRow: (row) => {
          const source = toKey(row.source);
          return source === "YBR" || source === "PIN";
        },
        markSaved: () => {
          setYbrPinSavedRows(cloneYbrPinRows(ybrPinRows));
          setYbrPinDirty(false);
          setYbrPinMessage("YBR/PIN edits saved to backend raw data.");
        },
        setInfo: setYbrPinMessage,
        setError: setYbrPinError,
        clearError: () => setYbrPinError(""),
      };
    } else if (ocnOtnRequested) {
      target = {
        rows: ocnOtnRows,
        dirty: ocnOtnDirty,
        label: "OCN/OTN Case",
        isCaseRow: (row) => {
          const source = toKey(row.source);
          return source === "OCN" || source === "OTN";
        },
        markSaved: () => {
          setOcnOtnSavedRows(cloneOcnOtnRows(ocnOtnRows));
          setOcnOtnDirty(false);
          setOcnOtnMessage("OCN/OTN edits saved to backend raw data.");
        },
        setInfo: setOcnOtnMessage,
        setError: setOcnOtnError,
        clearError: () => setOcnOtnError(""),
      };
    } else if (cmaOhrRequested) {
      target = {
        rows: cmaOhrRows,
        dirty: cmaOhrDirty,
        label: "CMA/OHR Case",
        isCaseRow: (row) => {
          const source = toKey(row.source);
          return source === "CMA" || source === "CMM" || source === "OHR";
        },
        markSaved: () => {
          setCmaOhrSavedRows(cloneCmaOhrRows(cmaOhrRows));
          setCmaOhrDirty(false);
          setCmaOhrMessage("CMA/OHR edits saved to backend raw data.");
        },
        setInfo: setCmaOhrMessage,
        setError: setCmaOhrError,
        clearError: () => setCmaOhrError(""),
      };
    } else if (cnxRequested) {
      target = {
        rows: cnxRows,
        dirty: cnxDirty,
        label: "CNX Case",
        isCaseRow: (row) => toKey(row.source) === "CNX",
        markSaved: () => {
          setCnxSavedRows(cloneCnxRows(cnxRows));
          setCnxDirty(false);
          setCnxMessage("CNX edits saved to backend raw data.");
        },
        setInfo: setCnxMessage,
        setError: setCnxError,
        clearError: () => setCnxError(""),
      };
    } else if (ohrPinRequested) {
      target = {
        rows: ohrPinRows,
        dirty: ohrPinDirty,
        label: "OHR/PIN Case",
        isCaseRow: (row) => {
          const source = toKey(row.source);
          return source === "OHR" || source === "PIN";
        },
        markSaved: () => {
          setOhrPinSavedRows(cloneOhrPinRows(ohrPinRows));
          setOhrPinDirty(false);
          setOhrPinMessage("OHR/PIN edits saved to backend raw data.");
        },
        setInfo: setOhrPinMessage,
        setError: setOhrPinError,
        clearError: () => setOhrPinError(""),
      };
    } else if (rimPinRequested) {
      target = {
        rows: rimPinRows,
        dirty: rimPinDirty,
        label: "RIM/PIN Case",
        isCaseRow: (row) => {
          const source = toKey(row.source);
          return source === "RIM" || source === "PIN";
        },
        markSaved: () => {
          setRimPinSavedRows(cloneRimPinRows(rimPinRows));
          setRimPinDirty(false);
          setRimPinMessage("RIM/PIN edits saved to backend raw data.");
        },
        setInfo: setRimPinMessage,
        setError: setRimPinError,
        clearError: () => setRimPinError(""),
      };
    } else if (ergPinRequested) {
      target = {
        rows: ergPinRows,
        dirty: ergPinDirty,
        label: "ERG/PIN Case",
        isCaseRow: (row) => {
          const source = toKey(row.source);
          return source === "ERG" || source === "PIN";
        },
        markSaved: () => {
          setErgPinSavedRows(cloneErgPinRows(ergPinRows));
          setErgPinDirty(false);
          setErgPinMessage("ERG/PIN edits saved to backend raw data.");
        },
        setInfo: setErgPinMessage,
        setError: setErgPinError,
        clearError: () => setErgPinError(""),
      };
    }

    if (!target) {
      setDoubleBrandMessage(`No loaded delete-case data to save for ${selectedDeleteCase}.`);
      return;
    }

    if (!target.dirty) {
      target.setInfo(`${target.label} has no pending changes to save.`);
      setDoubleBrandMessage(`${target.label} has no pending changes to save.`);
      return;
    }

    setSnapshotSaving(true);
    setDoubleBrandError("");
    target.clearError();
    target.setInfo(`Saving ${target.label}...`);
    setDoubleBrandMessage(`Saving ${target.label} changes to backend raw data...`);

    try {
      const baseRows = await ensureWorkflowRowsLoaded();
      const { runId, mergedRows } = await saveCaseRowsToBackend(
        baseRows,
        target.rows,
        target.isCaseRow,
        `${target.label} snapshot saved from Delete Double Brand case bar`
      );
      target.markSaved();
      setWorkflowRows(mergedRows);
      setDoubleBrandMessage(
        `${target.label} saved to backend raw data (Run #${runId}). Next case now uses this updated working table.`
      );
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : "Failed to save delete-case changes.";
      setDoubleBrandError(errorMessage);
      target.setError(errorMessage);
      target.setInfo(`Save failed for ${target.label}.`);
    } finally {
      setSnapshotSaving(false);
    }
  }

  function handleCompactionRowsChange(nextRows: Array<Record<string, string | number | null>>) {
    const recalculated = buildCompactionRowsWithIndicator(nextRows as OthDeletionFlagRow[]);
    setCompactionRows(recalculated);
    setCompactionDirty(true);
  }

  function handleCompactionDeleteRow(rowIndex: number) {
    setCompactionRows((previousRows) => {
      const nextRows = previousRows.filter((_, index) => index !== rowIndex);
      const recalculated = buildCompactionRowsWithIndicator(nextRows);
      return recalculated;
    });
    setCompactionDirty(true);
  }

  function handleToggleCompactionEditMode() {
    setCompactionEditMode((previous) => !previous);
  }

  async function handleSaveCompactionEdits() {
    await handleSaveDeleteCaseSelection();
  }

  function handleResetCompactionEdits() {
    setCompactionRows(cloneCompactionRows(compactionSavedRows));
    setCompactionDirty(false);
    setCompactionMessage("Compaction edits reverted to last saved state.");
  }

  async function handleLoadYbrPinCase() {
    setCompactionRequested(false);
    setCompactionRows([]);
    setCompactionSavedRows([]);
    setCompactionMessage("");
    setCompactionError("");
    setCompactionEditMode(false);
    setCompactionDirty(false);
    setOcnOtnRequested(false);
    setOcnOtnRows([]);
    setOcnOtnSavedRows([]);
    setOcnOtnMessage("");
    setOcnOtnError("");
    setOcnOtnEditMode(false);
    setOcnOtnDirty(false);
    setOcnOtnRuleApplied(false);
    setCmaOhrRequested(false);
    setCmaOhrRows([]);
    setCmaOhrSavedRows([]);
    setCmaOhrMessage("");
    setCmaOhrError("");
    setCmaOhrEditMode(false);
    setCmaOhrDirty(false);
    setCmaOhrRuleApplied(false);
    setCnxRequested(false);
    setCnxRows([]);
    setCnxSavedRows([]);
    setCnxMessage("");
    setCnxError("");
    setCnxEditMode(false);
    setCnxDirty(false);
    setCnxRuleApplied(false);
    setCnxHasOtherSourceByGroup({});
    setOhrPinRequested(false);
    setOhrPinRows([]);
    setOhrPinSavedRows([]);
    setOhrPinMessage("");
    setOhrPinError("");
    setOhrPinEditMode(false);
    setOhrPinDirty(false);
    setOhrPinRuleApplied(false);
    resetRimPinCaseState();
    resetErgPinCaseState();

    setYbrPinRequested(true);
    setYbrPinLoading(true);
    setYbrPinError("");
    setYbrPinEditMode(false);
    setYbrPinDirty(false);
    setYbrPinRuleApplied(false);
    setYbrPinMessage("Loading YBR/PIN rows...");

    try {
      const baseRows = await ensureWorkflowRowsLoaded();
      const filtered = filterRowsByRequiredSourceGroups(baseRows, [["YBR"], ["PIN"]])
        .map((row) => ({
          ...row,
          source_flag: "OTH",
          original_fid: row.fid,
          ybr_pin_decision: "PENDING",
        }));

      filtered.sort((a, b) => {
        const keyA = [
          toKey(a.country),
          toKey(a.machine_line_name),
          toKey(a.size_class_flag),
          toKey(a.brand_code || a.brand_name),
          toKey(a.source),
        ].join("|");
        const keyB = [
          toKey(b.country),
          toKey(b.machine_line_name),
          toKey(b.size_class_flag),
          toKey(b.brand_code || b.brand_name),
          toKey(b.source),
        ].join("|");
        return keyA.localeCompare(keyB);
      });

      setYbrPinRows(filtered);
      setYbrPinSavedRows(cloneYbrPinRows(filtered));
      setYbrPinMessage(
        `Loaded ${filtered.length} rows where YBR and PIN both exist in the same group. Click "Apply YBR/PIN Rule" to mark keep/drop and set dropped FID to 0.`
      );
      setDoubleBrandMessage("Selected YBR/PIN Case.");
    } catch (err) {
      setYbrPinRows([]);
      setYbrPinSavedRows([]);
      setYbrPinError(
        err instanceof Error ? err.message : "Failed to load YBR/PIN rows."
      );
      setYbrPinMessage("");
    } finally {
      setYbrPinLoading(false);
    }
  }

  async function handleApplyYbrPinRule() {
    setYbrPinRuleApplying(true);
    setYbrPinError("");

    try {
      const excludedBrands = new Set(["LIU", "SNY", "XCM"]);
      const groupedRows = new Map<string, YbrPinCaseRow[]>();

      for (const row of ybrPinRows) {
        const key = getYbrPinGroupKey(row);
        if (!groupedRows.has(key)) {
          groupedRows.set(key, []);
        }
        groupedRows.get(key)?.push(row);
      }

      const nextRows = ybrPinRows.map((row) => ({ ...row }));
      const droppedRowIndices = new Set<number>();
      let keepCount = 0;
      let dropCount = 0;
      let manualCount = 0;
      let pendingCount = 0;

      nextRows.forEach((row, index) => {
        const groupRows = groupedRows.get(getYbrPinGroupKey(row)) ?? [];
        const brandCode = toKey(row.brand_code);
        const source = toKey(row.source);
        const hasYbr = groupRows.some((item) => toKey(item.source) === "YBR");
        const hasPin = groupRows.some((item) => toKey(item.source) === "PIN");

        row.original_fid = row.original_fid ?? row.fid;

        if (!(hasYbr && hasPin)) {
          row.fid = row.original_fid;
          row.ybr_pin_decision = "KEEP_SINGLE_SOURCE";
          pendingCount += 1;
          return;
        }

        if (excludedBrands.has(brandCode)) {
          row.fid = row.original_fid;
          row.ybr_pin_decision = "MANUAL_REVIEW_EXCLUDED_BRAND";
          manualCount += 1;
          return;
        }

        if (source === "YBR") {
          row.fid = row.original_fid;
          row.ybr_pin_decision = "KEEP_YBR_TRUSTED";
          keepCount += 1;
        } else if (source === "PIN") {
          row.fid = 0;
          row.ybr_pin_decision = "DROP_PIN_FID_0_YBR_TRUSTED";
          droppedRowIndices.add(index);
          dropCount += 1;
        }
      });

      setYbrPinRows(nextRows);
      setYbrPinDirty(true);
      setYbrPinRuleApplied(true);
      setYbrPinMessage(
        `YBR/PIN rule applied. Keep: ${keepCount}, Drop(FID=0): ${dropCount}, Manual excluded: ${manualCount}, Single-source kept: ${pendingCount}.`
      );
    } catch (err) {
      setYbrPinError(
        err instanceof Error ? err.message : "Failed to apply YBR/PIN rule."
      );
    } finally {
      setYbrPinRuleApplying(false);
    }
  }

  function handleYbrPinRowsChange(nextRows: Array<Record<string, string | number | null>>) {
    const normalizedRows = (nextRows as YbrPinCaseRow[]).map((row) => ({
      ...row,
      source_flag: "OTH",
    }));
    setYbrPinRows(normalizedRows);
    setYbrPinDirty(true);
  }

  function handleToggleYbrPinEditMode() {
    setYbrPinEditMode((previous) => !previous);
  }

  async function handleSaveYbrPinEdits() {
    await handleSaveDeleteCaseSelection();
  }

  function handleResetYbrPinEdits() {
    setYbrPinRows(cloneYbrPinRows(ybrPinSavedRows));
    setYbrPinDirty(false);
    setYbrPinMessage("YBR/PIN edits reverted to last saved state.");
  }

  async function handleLoadOcnOtnCase() {
    setCompactionRequested(false);
    setCompactionRows([]);
    setCompactionSavedRows([]);
    setCompactionMessage("");
    setCompactionError("");
    setCompactionEditMode(false);
    setCompactionDirty(false);

    setYbrPinRequested(false);
    setYbrPinRows([]);
    setYbrPinSavedRows([]);
    setYbrPinMessage("");
    setYbrPinError("");
    setYbrPinEditMode(false);
    setYbrPinDirty(false);
    setYbrPinRuleApplied(false);
    setCmaOhrRequested(false);
    setCmaOhrRows([]);
    setCmaOhrSavedRows([]);
    setCmaOhrMessage("");
    setCmaOhrError("");
    setCmaOhrEditMode(false);
    setCmaOhrDirty(false);
    setCmaOhrRuleApplied(false);
    setCnxRequested(false);
    setCnxRows([]);
    setCnxSavedRows([]);
    setCnxMessage("");
    setCnxError("");
    setCnxEditMode(false);
    setCnxDirty(false);
    setCnxRuleApplied(false);
    setCnxHasOtherSourceByGroup({});
    setOhrPinRequested(false);
    setOhrPinRows([]);
    setOhrPinSavedRows([]);
    setOhrPinMessage("");
    setOhrPinError("");
    setOhrPinEditMode(false);
    setOhrPinDirty(false);
    setOhrPinRuleApplied(false);
    resetRimPinCaseState();
    resetErgPinCaseState();

    setOcnOtnRequested(true);
    setOcnOtnLoading(true);
    setOcnOtnError("");
    setOcnOtnEditMode(false);
    setOcnOtnDirty(false);
    setOcnOtnRuleApplied(false);
    setOcnOtnMessage("Loading OCN/OTN rows...");

    try {
      const baseRows = await ensureWorkflowRowsLoaded();
      const filtered = filterRowsByRequiredSourceGroups(baseRows, [["OCN"], ["OTN"]])
        .map((row) => ({
          ...row,
          source_flag: "OTH",
          original_fid: row.fid,
          ocn_otn_decision: "PENDING",
        }));

      filtered.sort((a, b) => {
        const keyA = [
          toKey(a.country),
          toKey(a.machine_line_name),
          toKey(a.size_class_flag),
          toKey(a.brand_code || a.brand_name),
          toKey(a.source),
        ].join("|");
        const keyB = [
          toKey(b.country),
          toKey(b.machine_line_name),
          toKey(b.size_class_flag),
          toKey(b.brand_code || b.brand_name),
          toKey(b.source),
        ].join("|");
        return keyA.localeCompare(keyB);
      });

      setOcnOtnRows(filtered);
      setOcnOtnSavedRows(cloneOcnOtnRows(filtered));
      setOcnOtnMessage(
        `Loaded ${filtered.length} rows where OCN and OTN both exist in the same group. Click "Apply OCN/OTN Rule" to keep OCN and adjust OTN by max(OTN-OCN, 0).`
      );
      setDoubleBrandMessage("Selected OCN/OTN Case.");
    } catch (err) {
      setOcnOtnRows([]);
      setOcnOtnSavedRows([]);
      setOcnOtnError(
        err instanceof Error ? err.message : "Failed to load OCN/OTN rows."
      );
      setOcnOtnMessage("");
    } finally {
      setOcnOtnLoading(false);
    }
  }

  async function handleApplyOcnOtnRule() {
    setOcnOtnRuleApplying(true);
    setOcnOtnError("");

    try {
      const toNumber = (value: string | number | null | undefined): number => {
        if (value === null || value === undefined || value === "") {
          return 0;
        }
        if (typeof value === "number") {
          return Number.isFinite(value) ? value : 0;
        }
        const parsed = Number(String(value).replace(/,/g, "").trim());
        return Number.isFinite(parsed) ? parsed : 0;
      };

      const groupedRows = new Map<string, OcnOtnCaseRow[]>();
      const sourceTotals = new Map<string, { ocnTotal: number; otnTotal: number; otnAssigned: boolean }>();

      for (const row of ocnOtnRows) {
        const key = getYbrPinGroupKey(row);
        if (!groupedRows.has(key)) {
          groupedRows.set(key, []);
        }
        groupedRows.get(key)?.push(row);
      }

      for (const [groupKey, groupRows] of groupedRows.entries()) {
        const ocnTotal = groupRows
          .filter((item) => toKey(item.source) === "OCN")
          .reduce((total, item) => total + toNumber(item.original_fid ?? item.fid), 0);
        const otnTotal = groupRows
          .filter((item) => toKey(item.source) === "OTN")
          .reduce((total, item) => total + toNumber(item.original_fid ?? item.fid), 0);

        sourceTotals.set(groupKey, { ocnTotal, otnTotal, otnAssigned: false });
      }

      const nextRows = ocnOtnRows.map((row) => ({ ...row }));
      let keepCount = 0;
      let adjustCount = 0;
      let dropCount = 0;
      let singleSourceCount = 0;

      nextRows.forEach((row) => {
        const groupKey = getYbrPinGroupKey(row);
        const groupRows = groupedRows.get(groupKey) ?? [];
        const totals = sourceTotals.get(groupKey);
        const source = toKey(row.source);
        const hasOcn = groupRows.some((item) => toKey(item.source) === "OCN");
        const hasOtn = groupRows.some((item) => toKey(item.source) === "OTN");

        row.original_fid = row.original_fid ?? row.fid;

        if (!(hasOcn && hasOtn)) {
          row.fid = row.original_fid;
          row.ocn_otn_decision = "KEEP_SINGLE_SOURCE";
          singleSourceCount += 1;
          return;
        }

        if (!totals) {
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
      setOcnOtnRuleApplied(true);
      setOcnOtnMessage(
        `OCN/OTN rule applied. Keep OCN: ${keepCount}, Adjusted OTN: ${adjustCount}, Dropped OTN(FID=0): ${dropCount}, Single-source kept: ${singleSourceCount}.`
      );
    } catch (err) {
      setOcnOtnError(
        err instanceof Error ? err.message : "Failed to apply OCN/OTN rule."
      );
    } finally {
      setOcnOtnRuleApplying(false);
    }
  }

  function handleOcnOtnRowsChange(nextRows: Array<Record<string, string | number | null>>) {
    const normalizedRows = (nextRows as OcnOtnCaseRow[]).map((row) => ({
      ...row,
      source_flag: "OTH",
    }));
    setOcnOtnRows(normalizedRows);
    setOcnOtnDirty(true);
  }

  function handleToggleOcnOtnEditMode() {
    setOcnOtnEditMode((previous) => !previous);
  }

  async function handleSaveOcnOtnEdits() {
    await handleSaveDeleteCaseSelection();
  }

  function handleResetOcnOtnEdits() {
    setOcnOtnRows(cloneOcnOtnRows(ocnOtnSavedRows));
    setOcnOtnDirty(false);
    setOcnOtnMessage("OCN/OTN edits reverted to last saved state.");
  }

  async function handleLoadCmaOhrCase() {
    setCompactionRequested(false);
    setCompactionRows([]);
    setCompactionSavedRows([]);
    setCompactionMessage("");
    setCompactionError("");
    setCompactionEditMode(false);
    setCompactionDirty(false);

    setYbrPinRequested(false);
    setYbrPinRows([]);
    setYbrPinSavedRows([]);
    setYbrPinMessage("");
    setYbrPinError("");
    setYbrPinEditMode(false);
    setYbrPinDirty(false);
    setYbrPinRuleApplied(false);

    setOcnOtnRequested(false);
    setOcnOtnRows([]);
    setOcnOtnSavedRows([]);
    setOcnOtnMessage("");
    setOcnOtnError("");
    setOcnOtnEditMode(false);
    setOcnOtnDirty(false);
    setOcnOtnRuleApplied(false);
    setCnxRequested(false);
    setCnxRows([]);
    setCnxSavedRows([]);
    setCnxMessage("");
    setCnxError("");
    setCnxEditMode(false);
    setCnxDirty(false);
    setCnxRuleApplied(false);
    setCnxHasOtherSourceByGroup({});
    setOhrPinRequested(false);
    setOhrPinRows([]);
    setOhrPinSavedRows([]);
    setOhrPinMessage("");
    setOhrPinError("");
    setOhrPinEditMode(false);
    setOhrPinDirty(false);
    setOhrPinRuleApplied(false);
    resetRimPinCaseState();
    resetErgPinCaseState();

    setCmaOhrRequested(true);
    setCmaOhrLoading(true);
    setCmaOhrError("");
    setCmaOhrEditMode(false);
    setCmaOhrDirty(false);
    setCmaOhrRuleApplied(false);
    setCmaOhrMessage("Loading CMA/CMM and OHR rows...");

    try {
      const baseRows = await ensureWorkflowRowsLoaded();
      const filtered = filterRowsByRequiredSourceGroups(baseRows, [["CMA", "CMM"], ["OHR"]])
        .map((row) => ({
          ...row,
          source_flag: "OTH",
          original_fid: row.fid,
          cma_ohr_decision: "PENDING",
        }));

      filtered.sort((a, b) => {
        const keyA = [
          toKey(a.country),
          toKey(a.machine_line_name),
          toKey(a.size_class_flag),
          toKey(a.brand_code || a.brand_name),
          toKey(a.source),
        ].join("|");
        const keyB = [
          toKey(b.country),
          toKey(b.machine_line_name),
          toKey(b.size_class_flag),
          toKey(b.brand_code || b.brand_name),
          toKey(b.source),
        ].join("|");
        return keyA.localeCompare(keyB);
      });

      setCmaOhrRows(filtered);
      setCmaOhrSavedRows(cloneCmaOhrRows(filtered));
      setCmaOhrMessage(
        `Loaded ${filtered.length} rows where CMA/CMM and OHR both exist in the same group. Click "Apply CMA/OHR Rule" to decide which source to keep.`
      );
      setDoubleBrandMessage("Selected CMA/OHR Case.");
    } catch (err) {
      setCmaOhrRows([]);
      setCmaOhrSavedRows([]);
      setCmaOhrError(
        err instanceof Error ? err.message : "Failed to load CMA/OHR rows."
      );
      setCmaOhrMessage("");
    } finally {
      setCmaOhrLoading(false);
    }
  }

  async function handleApplyCmaOhrRule() {
    setCmaOhrRuleApplying(true);
    setCmaOhrError("");

    try {
      const groupedRows = new Map<string, CmaOhrCaseRow[]>();
      for (const row of cmaOhrRows) {
        const key = getYbrPinGroupKey(row);
        if (!groupedRows.has(key)) {
          groupedRows.set(key, []);
        }
        groupedRows.get(key)?.push(row);
      }

      const nextRows = cmaOhrRows.map((row) => ({ ...row }));
      let keepCount = 0;
      let dropCount = 0;
      let singleSourceCount = 0;

      nextRows.forEach((row) => {
        const groupRows = groupedRows.get(getYbrPinGroupKey(row)) ?? [];
        const source = toKey(row.source);
        const hasCmaLike = groupRows.some((item) => {
          const itemSource = toKey(item.source);
          return itemSource === "CMA" || itemSource === "CMM";
        });
        const hasOhr = groupRows.some((item) => toKey(item.source) === "OHR");
        const machineLineCode = toKey(row.machine_line_code || row.machine_line_name);
        const isMl390or400 = machineLineCode === "390" || machineLineCode === "400";
        const brandCountry = toKey(row.country_code || row.country);
        const isCnBrandCountry = brandCountry === "CN" || brandCountry === "CHINA";
        const isCnSpecialCase = isMl390or400 && isCnBrandCountry;

        row.original_fid = row.original_fid ?? row.fid;

        if (!(hasCmaLike && hasOhr)) {
          row.fid = row.original_fid;
          row.cma_ohr_decision = "KEEP_SINGLE_SOURCE";
          singleSourceCount += 1;
          return;
        }

        if (source === "CMA" || source === "CMM") {
          if (isCnSpecialCase) {
            row.fid = row.original_fid;
            row.cma_ohr_decision = "KEEP_CN_390_400_CMA_CMM";
            keepCount += 1;
          } else {
            row.fid = 0;
            row.cma_ohr_decision = "DROP_NON_CN_OR_OTHER_ML_CMA_CMM_FID_0";
            dropCount += 1;
          }
          return;
        }

        if (source === "OHR") {
          if (isCnSpecialCase) {
            row.fid = 0;
            row.cma_ohr_decision = "DROP_CN_390_400_OHR_FID_0";
            dropCount += 1;
          } else {
            row.fid = row.original_fid;
            row.cma_ohr_decision = "KEEP_NON_CN_OR_OTHER_ML_OHR";
            keepCount += 1;
          }
          return;
        }

        row.fid = row.original_fid;
        row.cma_ohr_decision = "KEEP_SINGLE_SOURCE";
        singleSourceCount += 1;
      });

      setCmaOhrRows(nextRows);
      setCmaOhrDirty(true);
      setCmaOhrRuleApplied(true);
      setCmaOhrMessage(
        `CMA/OHR rule applied. Keep: ${keepCount}, Drop(FID=0): ${dropCount}, Single-source kept: ${singleSourceCount}.`
      );
    } catch (err) {
      setCmaOhrError(
        err instanceof Error ? err.message : "Failed to apply CMA/OHR rule."
      );
    } finally {
      setCmaOhrRuleApplying(false);
    }
  }

  function handleCmaOhrRowsChange(nextRows: Array<Record<string, string | number | null>>) {
    const normalizedRows = (nextRows as CmaOhrCaseRow[]).map((row) => ({
      ...row,
      source_flag: "OTH",
    }));
    setCmaOhrRows(normalizedRows);
    setCmaOhrDirty(true);
  }

  function handleToggleCmaOhrEditMode() {
    setCmaOhrEditMode((previous) => !previous);
  }

  async function handleSaveCmaOhrEdits() {
    await handleSaveDeleteCaseSelection();
  }

  function handleResetCmaOhrEdits() {
    setCmaOhrRows(cloneCmaOhrRows(cmaOhrSavedRows));
    setCmaOhrDirty(false);
    setCmaOhrMessage("CMA/OHR edits reverted to last saved state.");
  }

  async function handleLoadCnxCase() {
    setCompactionRequested(false);
    setCompactionRows([]);
    setCompactionSavedRows([]);
    setCompactionMessage("");
    setCompactionError("");
    setCompactionEditMode(false);
    setCompactionDirty(false);

    setYbrPinRequested(false);
    setYbrPinRows([]);
    setYbrPinSavedRows([]);
    setYbrPinMessage("");
    setYbrPinError("");
    setYbrPinEditMode(false);
    setYbrPinDirty(false);
    setYbrPinRuleApplied(false);

    setOcnOtnRequested(false);
    setOcnOtnRows([]);
    setOcnOtnSavedRows([]);
    setOcnOtnMessage("");
    setOcnOtnError("");
    setOcnOtnEditMode(false);
    setOcnOtnDirty(false);
    setOcnOtnRuleApplied(false);

    setCmaOhrRequested(false);
    setCmaOhrRows([]);
    setCmaOhrSavedRows([]);
    setCmaOhrMessage("");
    setCmaOhrError("");
    setCmaOhrEditMode(false);
    setCmaOhrDirty(false);
    setCmaOhrRuleApplied(false);

    setCnxRequested(true);
    setCnxLoading(true);
    setCnxError("");
    setCnxEditMode(false);
    setCnxDirty(false);
    setCnxRuleApplied(false);
    setOhrPinRequested(false);
    setOhrPinRows([]);
    setOhrPinSavedRows([]);
    setOhrPinMessage("");
    setOhrPinError("");
    setOhrPinEditMode(false);
    setOhrPinDirty(false);
    setOhrPinRuleApplied(false);
    resetRimPinCaseState();
    resetErgPinCaseState();
    setCnxMessage("Loading CNX rows...");

    try {
      const baseRows = await ensureWorkflowRowsLoaded();
      const sourcesByGroup = new Map<string, Set<string>>();
      for (const row of baseRows) {
        const key = getYbrPinGroupKey(row);
        if (!sourcesByGroup.has(key)) {
          sourcesByGroup.set(key, new Set());
        }
        sourcesByGroup.get(key)?.add(toKey(row.source));
      }

      const hasOtherSourceByGroup: Record<string, boolean> = {};
      for (const [key, sources] of sourcesByGroup.entries()) {
        hasOtherSourceByGroup[key] = Array.from(sources).some((source) => source !== "CNX");
      }

      const filtered = baseRows
        .filter((row) => {
          if (toKey(row.source) !== "CNX") {
            return false;
          }
          const key = getYbrPinGroupKey(row);
          return hasOtherSourceByGroup[key] ?? false;
        })
        .map((row) => ({
          ...row,
          source_flag: "OTH",
          original_fid: row.fid,
          cnx_decision: "PENDING",
        }));

      filtered.sort((a, b) => {
        const keyA = [
          toKey(a.country),
          toKey(a.machine_line_name),
          toKey(a.size_class_flag),
          toKey(a.brand_code || a.brand_name),
          toKey(a.source),
        ].join("|");
        const keyB = [
          toKey(b.country),
          toKey(b.machine_line_name),
          toKey(b.size_class_flag),
          toKey(b.brand_code || b.brand_name),
          toKey(b.source),
        ].join("|");
        return keyA.localeCompare(keyB);
      });

      setCnxHasOtherSourceByGroup(hasOtherSourceByGroup);
      setCnxRows(filtered);
      setCnxSavedRows(cloneCnxRows(filtered));
      setCnxMessage(
        `Loaded ${filtered.length} CNX rows where the same group also has non-CNX source(s). Click "Apply CNX Rule" to set CNX FID to 0.`
      );
      setDoubleBrandMessage("Selected CNX Case.");
    } catch (err) {
      setCnxHasOtherSourceByGroup({});
      setCnxRows([]);
      setCnxSavedRows([]);
      setCnxError(
        err instanceof Error ? err.message : "Failed to load CNX rows."
      );
      setCnxMessage("");
    } finally {
      setCnxLoading(false);
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
        const key = getYbrPinGroupKey(row);
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
      setCnxRuleApplied(true);
      setCnxMessage(
        `CNX rule applied. Dropped CNX(FID=0): ${dropCount}, Kept single-source CNX: ${keepCount}.`
      );
    } catch (err) {
      setCnxError(
        err instanceof Error ? err.message : "Failed to apply CNX rule."
      );
    } finally {
      setCnxRuleApplying(false);
    }
  }

  function handleCnxRowsChange(nextRows: Array<Record<string, string | number | null>>) {
    const normalizedRows = (nextRows as CnxCaseRow[]).map((row) => ({
      ...row,
      source_flag: "OTH",
    }));
    setCnxRows(normalizedRows);
    setCnxDirty(true);
  }

  function handleToggleCnxEditMode() {
    setCnxEditMode((previous) => !previous);
  }

  async function handleSaveCnxEdits() {
    await handleSaveDeleteCaseSelection();
  }

  function handleResetCnxEdits() {
    setCnxRows(cloneCnxRows(cnxSavedRows));
    setCnxDirty(false);
    setCnxMessage("CNX edits reverted to last saved state.");
  }

  async function handleLoadOhrPinCase() {
    setCompactionRequested(false);
    setCompactionRows([]);
    setCompactionSavedRows([]);
    setCompactionMessage("");
    setCompactionError("");
    setCompactionEditMode(false);
    setCompactionDirty(false);

    setYbrPinRequested(false);
    setYbrPinRows([]);
    setYbrPinSavedRows([]);
    setYbrPinMessage("");
    setYbrPinError("");
    setYbrPinEditMode(false);
    setYbrPinDirty(false);
    setYbrPinRuleApplied(false);

    setOcnOtnRequested(false);
    setOcnOtnRows([]);
    setOcnOtnSavedRows([]);
    setOcnOtnMessage("");
    setOcnOtnError("");
    setOcnOtnEditMode(false);
    setOcnOtnDirty(false);
    setOcnOtnRuleApplied(false);

    setCmaOhrRequested(false);
    setCmaOhrRows([]);
    setCmaOhrSavedRows([]);
    setCmaOhrMessage("");
    setCmaOhrError("");
    setCmaOhrEditMode(false);
    setCmaOhrDirty(false);
    setCmaOhrRuleApplied(false);

    setCnxRequested(false);
    setCnxRows([]);
    setCnxSavedRows([]);
    setCnxMessage("");
    setCnxError("");
    setCnxEditMode(false);
    setCnxDirty(false);
    setCnxRuleApplied(false);
    setCnxHasOtherSourceByGroup({});
    resetRimPinCaseState();
    resetErgPinCaseState();

    setOhrPinRequested(true);
    setOhrPinLoading(true);
    setOhrPinError("");
    setOhrPinEditMode(false);
    setOhrPinDirty(false);
    setOhrPinRuleApplied(false);
    setOhrPinMessage("Loading OHR/PIN rows...");

    try {
      const baseRows = await ensureWorkflowRowsLoaded();
      const filtered = filterRowsByRequiredSourceGroups(baseRows, [["OHR"], ["PIN"]])
        .map((row) => ({
          ...row,
          source_flag: "OTH",
          original_fid: row.fid,
          ohr_pin_decision: "PENDING",
        }));

      filtered.sort((a, b) => {
        const keyA = [
          toKey(a.country),
          toKey(a.machine_line_name),
          toKey(a.size_class_flag),
          toKey(a.brand_code || a.brand_name),
          toKey(a.source),
        ].join("|");
        const keyB = [
          toKey(b.country),
          toKey(b.machine_line_name),
          toKey(b.size_class_flag),
          toKey(b.brand_code || b.brand_name),
          toKey(b.source),
        ].join("|");
        return keyA.localeCompare(keyB);
      });

      setOhrPinRows(filtered);
      setOhrPinSavedRows(cloneOhrPinRows(filtered));
      setOhrPinMessage(
        `Loaded ${filtered.length} rows where OHR and PIN both exist in the same group. Click "Apply OHR/PIN Rule" to keep OHR and set duplicated PIN rows to FID 0.`
      );
      setDoubleBrandMessage("Selected OHR/PIN Case.");
    } catch (err) {
      setOhrPinRows([]);
      setOhrPinSavedRows([]);
      setOhrPinError(
        err instanceof Error ? err.message : "Failed to load OHR/PIN rows."
      );
      setOhrPinMessage("");
    } finally {
      setOhrPinLoading(false);
    }
  }

  async function handleApplyOhrPinRule() {
    setOhrPinRuleApplying(true);
    setOhrPinError("");

    try {
      const groupedRows = new Map<string, OhrPinCaseRow[]>();
      for (const row of ohrPinRows) {
        const key = getYbrPinGroupKey(row);
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
        const groupRows = groupedRows.get(getYbrPinGroupKey(row)) ?? [];
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
      setOhrPinRuleApplied(true);
      setOhrPinMessage(
        `OHR/PIN rule applied. Kept OHR: ${keepCount}, Dropped PIN(FID=0): ${dropCount}, Single-source kept: ${singleSourceCount}.`
      );
    } catch (err) {
      setOhrPinError(
        err instanceof Error ? err.message : "Failed to apply OHR/PIN rule."
      );
    } finally {
      setOhrPinRuleApplying(false);
    }
  }

  function handleOhrPinRowsChange(nextRows: Array<Record<string, string | number | null>>) {
    const normalizedRows = (nextRows as OhrPinCaseRow[]).map((row) => ({
      ...row,
      source_flag: "OTH",
    }));
    setOhrPinRows(normalizedRows);
    setOhrPinDirty(true);
  }

  function handleToggleOhrPinEditMode() {
    setOhrPinEditMode((previous) => !previous);
  }

  async function handleSaveOhrPinEdits() {
    await handleSaveDeleteCaseSelection();
  }

  function handleResetOhrPinEdits() {
    setOhrPinRows(cloneOhrPinRows(ohrPinSavedRows));
    setOhrPinDirty(false);
    setOhrPinMessage("OHR/PIN edits reverted to last saved state.");
  }

  async function handleLoadRimPinCase() {
    setCompactionRequested(false);
    setCompactionRows([]);
    setCompactionSavedRows([]);
    setCompactionMessage("");
    setCompactionError("");
    setCompactionEditMode(false);
    setCompactionDirty(false);

    setYbrPinRequested(false);
    setYbrPinRows([]);
    setYbrPinSavedRows([]);
    setYbrPinMessage("");
    setYbrPinError("");
    setYbrPinEditMode(false);
    setYbrPinDirty(false);
    setYbrPinRuleApplied(false);

    setOcnOtnRequested(false);
    setOcnOtnRows([]);
    setOcnOtnSavedRows([]);
    setOcnOtnMessage("");
    setOcnOtnError("");
    setOcnOtnEditMode(false);
    setOcnOtnDirty(false);
    setOcnOtnRuleApplied(false);

    setCmaOhrRequested(false);
    setCmaOhrRows([]);
    setCmaOhrSavedRows([]);
    setCmaOhrMessage("");
    setCmaOhrError("");
    setCmaOhrEditMode(false);
    setCmaOhrDirty(false);
    setCmaOhrRuleApplied(false);

    setCnxRequested(false);
    setCnxRows([]);
    setCnxSavedRows([]);
    setCnxMessage("");
    setCnxError("");
    setCnxEditMode(false);
    setCnxDirty(false);
    setCnxRuleApplied(false);
    setCnxHasOtherSourceByGroup({});

    setOhrPinRequested(false);
    setOhrPinRows([]);
    setOhrPinSavedRows([]);
    setOhrPinMessage("");
    setOhrPinError("");
    setOhrPinEditMode(false);
    setOhrPinDirty(false);
    setOhrPinRuleApplied(false);
    resetErgPinCaseState();

    setRimPinRequested(true);
    setRimPinLoading(true);
    setRimPinError("");
    setRimPinEditMode(false);
    setRimPinDirty(false);
    setRimPinRuleApplied(false);
    setRimPinMessage("Loading RIM/PIN rows...");

    try {
      const baseRows = await ensureWorkflowRowsLoaded();
      const filtered = filterRowsByRequiredSourceGroups(baseRows, [["RIM"], ["PIN"]])
        .map((row) => ({
          ...row,
          source_flag: "OTH",
          original_fid: row.fid,
          rim_pin_decision: "PENDING",
        }));

      filtered.sort((a, b) => {
        const keyA = [
          toKey(a.country),
          toKey(a.machine_line_name),
          toKey(a.size_class_flag),
          toKey(a.brand_code || a.brand_name),
          toKey(a.source),
        ].join("|");
        const keyB = [
          toKey(b.country),
          toKey(b.machine_line_name),
          toKey(b.size_class_flag),
          toKey(b.brand_code || b.brand_name),
          toKey(b.source),
        ].join("|");
        return keyA.localeCompare(keyB);
      });

      setRimPinRows(filtered);
      setRimPinSavedRows(cloneRimPinRows(filtered));
      setRimPinMessage(
        `Loaded ${filtered.length} rows where RIM and PIN both exist in the same group. Click "Apply RIM/PIN Rule" to keep RIM and set duplicated PIN rows to FID 0.`
      );
      setDoubleBrandMessage("Selected RIM/PIN Case.");
    } catch (err) {
      setRimPinRows([]);
      setRimPinSavedRows([]);
      setRimPinError(
        err instanceof Error ? err.message : "Failed to load RIM/PIN rows."
      );
      setRimPinMessage("");
    } finally {
      setRimPinLoading(false);
    }
  }

  async function handleApplyRimPinRule() {
    setRimPinRuleApplying(true);
    setRimPinError("");

    try {
      const groupedRows = new Map<string, RimPinCaseRow[]>();
      for (const row of rimPinRows) {
        const key = getYbrPinGroupKey(row);
        if (!groupedRows.has(key)) {
          groupedRows.set(key, []);
        }
        groupedRows.get(key)?.push(row);
      }

      const nextRows = rimPinRows.map((row) => ({ ...row }));
      let keepCount = 0;
      let dropCount = 0;
      let singleSourceCount = 0;

      nextRows.forEach((row) => {
        const groupRows = groupedRows.get(getYbrPinGroupKey(row)) ?? [];
        const source = toKey(row.source);
        const hasRim = groupRows.some((item) => toKey(item.source) === "RIM");
        const hasPin = groupRows.some((item) => toKey(item.source) === "PIN");

        row.original_fid = row.original_fid ?? row.fid;

        if (!(hasRim && hasPin)) {
          row.fid = row.original_fid;
          row.rim_pin_decision = "KEEP_SINGLE_SOURCE";
          singleSourceCount += 1;
          return;
        }

        if (source === "RIM") {
          row.fid = row.original_fid;
          row.rim_pin_decision = "KEEP_RIM_TRUSTED";
          keepCount += 1;
          return;
        }

        if (source === "PIN") {
          row.fid = 0;
          row.rim_pin_decision = "DROP_PIN_FID_0_RIM_TRUSTED";
          dropCount += 1;
          return;
        }

        row.fid = row.original_fid;
        row.rim_pin_decision = "KEEP_SINGLE_SOURCE";
        singleSourceCount += 1;
      });

      setRimPinRows(nextRows);
      setRimPinDirty(true);
      setRimPinRuleApplied(true);
      setRimPinMessage(
        `RIM/PIN rule applied. Kept RIM: ${keepCount}, Dropped PIN(FID=0): ${dropCount}, Single-source kept: ${singleSourceCount}.`
      );
    } catch (err) {
      setRimPinError(
        err instanceof Error ? err.message : "Failed to apply RIM/PIN rule."
      );
    } finally {
      setRimPinRuleApplying(false);
    }
  }

  function handleRimPinRowsChange(nextRows: Array<Record<string, string | number | null>>) {
    const normalizedRows = (nextRows as RimPinCaseRow[]).map((row) => ({
      ...row,
      source_flag: "OTH",
    }));
    setRimPinRows(normalizedRows);
    setRimPinDirty(true);
  }

  function handleToggleRimPinEditMode() {
    setRimPinEditMode((previous) => !previous);
  }

  async function handleSaveRimPinEdits() {
    await handleSaveDeleteCaseSelection();
  }

  function handleResetRimPinEdits() {
    setRimPinRows(cloneRimPinRows(rimPinSavedRows));
    setRimPinDirty(false);
    setRimPinMessage("RIM/PIN edits reverted to last saved state.");
  }

  async function handleLoadErgPinCase() {
    setCompactionRequested(false);
    setCompactionRows([]);
    setCompactionSavedRows([]);
    setCompactionMessage("");
    setCompactionError("");
    setCompactionEditMode(false);
    setCompactionDirty(false);

    setYbrPinRequested(false);
    setYbrPinRows([]);
    setYbrPinSavedRows([]);
    setYbrPinMessage("");
    setYbrPinError("");
    setYbrPinEditMode(false);
    setYbrPinDirty(false);
    setYbrPinRuleApplied(false);

    setOcnOtnRequested(false);
    setOcnOtnRows([]);
    setOcnOtnSavedRows([]);
    setOcnOtnMessage("");
    setOcnOtnError("");
    setOcnOtnEditMode(false);
    setOcnOtnDirty(false);
    setOcnOtnRuleApplied(false);

    setCmaOhrRequested(false);
    setCmaOhrRows([]);
    setCmaOhrSavedRows([]);
    setCmaOhrMessage("");
    setCmaOhrError("");
    setCmaOhrEditMode(false);
    setCmaOhrDirty(false);
    setCmaOhrRuleApplied(false);

    setCnxRequested(false);
    setCnxRows([]);
    setCnxSavedRows([]);
    setCnxMessage("");
    setCnxError("");
    setCnxEditMode(false);
    setCnxDirty(false);
    setCnxRuleApplied(false);
    setCnxHasOtherSourceByGroup({});

    setOhrPinRequested(false);
    setOhrPinRows([]);
    setOhrPinSavedRows([]);
    setOhrPinMessage("");
    setOhrPinError("");
    setOhrPinEditMode(false);
    setOhrPinDirty(false);
    setOhrPinRuleApplied(false);

    setRimPinRequested(false);
    setRimPinRows([]);
    setRimPinSavedRows([]);
    setRimPinMessage("");
    setRimPinError("");
    setRimPinEditMode(false);
    setRimPinDirty(false);
    setRimPinRuleApplied(false);

    setErgPinRequested(true);
    setErgPinLoading(true);
    setErgPinError("");
    setErgPinEditMode(false);
    setErgPinDirty(false);
    setErgPinRuleApplied(false);
    setErgPinMessage("Loading ERG/PIN rows...");

    try {
      const baseRows = await ensureWorkflowRowsLoaded();
      const filtered = filterRowsByRequiredSourceGroups(baseRows, [["ERG"], ["PIN"]])
        .map((row) => ({
          ...row,
          source_flag: "OTH",
          original_fid: row.fid,
          erg_pin_decision: "PENDING",
        }));

      filtered.sort((a, b) => {
        const keyA = [
          toKey(a.country),
          toKey(a.machine_line_name),
          toKey(a.size_class_flag),
          toKey(a.brand_code || a.brand_name),
          toKey(a.source),
        ].join("|");
        const keyB = [
          toKey(b.country),
          toKey(b.machine_line_name),
          toKey(b.size_class_flag),
          toKey(b.brand_code || b.brand_name),
          toKey(b.source),
        ].join("|");
        return keyA.localeCompare(keyB);
      });

      setErgPinRows(filtered);
      setErgPinSavedRows(cloneErgPinRows(filtered));
      setErgPinMessage(
        `Loaded ${filtered.length} rows where ERG and PIN both exist in the same group. Click "Apply ERG/PIN Rule" to keep ERG and set duplicated PIN rows to FID 0.`
      );
      setDoubleBrandMessage("Selected ERG/PIN Case.");
    } catch (err) {
      setErgPinRows([]);
      setErgPinSavedRows([]);
      setErgPinError(
        err instanceof Error ? err.message : "Failed to load ERG/PIN rows."
      );
      setErgPinMessage("");
    } finally {
      setErgPinLoading(false);
    }
  }

  async function handleApplyErgPinRule() {
    setErgPinRuleApplying(true);
    setErgPinError("");

    try {
      const groupedRows = new Map<string, ErgPinCaseRow[]>();
      for (const row of ergPinRows) {
        const key = getYbrPinGroupKey(row);
        if (!groupedRows.has(key)) {
          groupedRows.set(key, []);
        }
        groupedRows.get(key)?.push(row);
      }

      const nextRows = ergPinRows.map((row) => ({ ...row }));
      let keepCount = 0;
      let dropCount = 0;
      let singleSourceCount = 0;

      nextRows.forEach((row) => {
        const groupRows = groupedRows.get(getYbrPinGroupKey(row)) ?? [];
        const source = toKey(row.source);
        const hasErg = groupRows.some((item) => toKey(item.source) === "ERG");
        const hasPin = groupRows.some((item) => toKey(item.source) === "PIN");

        row.original_fid = row.original_fid ?? row.fid;

        if (!(hasErg && hasPin)) {
          row.fid = row.original_fid;
          row.erg_pin_decision = "KEEP_SINGLE_SOURCE";
          singleSourceCount += 1;
          return;
        }

        if (source === "ERG") {
          row.fid = row.original_fid;
          row.erg_pin_decision = "KEEP_ERG_TRUSTED";
          keepCount += 1;
          return;
        }

        if (source === "PIN") {
          row.fid = 0;
          row.erg_pin_decision = "DROP_PIN_FID_0_ERG_TRUSTED";
          dropCount += 1;
          return;
        }

        row.fid = row.original_fid;
        row.erg_pin_decision = "KEEP_SINGLE_SOURCE";
        singleSourceCount += 1;
      });

      setErgPinRows(nextRows);
      setErgPinDirty(true);
      setErgPinRuleApplied(true);
      setErgPinMessage(
        `ERG/PIN rule applied. Kept ERG: ${keepCount}, Dropped PIN(FID=0): ${dropCount}, Single-source kept: ${singleSourceCount}.`
      );
    } catch (err) {
      setErgPinError(
        err instanceof Error ? err.message : "Failed to apply ERG/PIN rule."
      );
    } finally {
      setErgPinRuleApplying(false);
    }
  }

  function handleErgPinRowsChange(nextRows: Array<Record<string, string | number | null>>) {
    const normalizedRows = (nextRows as ErgPinCaseRow[]).map((row) => ({
      ...row,
      source_flag: "OTH",
    }));
    setErgPinRows(normalizedRows);
    setErgPinDirty(true);
  }

  function handleToggleErgPinEditMode() {
    setErgPinEditMode((previous) => !previous);
  }

  async function handleSaveErgPinEdits() {
    await handleSaveDeleteCaseSelection();
  }

  function handleResetErgPinEdits() {
    setErgPinRows(cloneErgPinRows(ergPinSavedRows));
    setErgPinDirty(false);
    setErgPinMessage("ERG/PIN edits reverted to last saved state.");
  }

  return (
    <div className="page">
      <section className="section section--layer-detail-wide">
        <div className="section-header">
          <p className="section-tag">Total Market Calculation</p>
          <h1 className="section-title">Total Market Calculation</h1>
          <p className="section-description">
            Review the full OTH Deletion Flag dataset (all reporter/deletion states). For rows
            that are split-applicable, this view shows split output rows. Volvo rows are excluded
            from this input view.
          </p>
        </div>

        <div className="overview-actions" style={{ marginBottom: "20px" }}>
          <div className="overview-actions__buttons tmc-actions-grid">
            <button
              type="button"
              className="btn btn--primary tmc-action-main tmc-action-raw"
              onClick={handleShowLatestRaw}
              disabled={latestLoading}
            >
              {latestLoading ? "Loading raw..." : "Show Raw Total Market Calculation Rows"}
            </button>
            <button
              type="button"
              className="btn btn--overview tmc-action-main tmc-action-report"
              onClick={handleReportCheckDoubleBrand}
              disabled={doubleBrandLoading}
            >
              {doubleBrandLoading && doubleBrandMode === "all"
                ? "Checking..."
                : "Report check double brand"}
            </button>
            <button
              type="button"
              className="btn btn--overview tmc-action-main tmc-action-nam-zar"
              onClick={handleReportCheckDoubleBrandNamZar}
              disabled={doubleBrandLoading}
            >
              {doubleBrandLoading && doubleBrandMode === "namZar"
                ? "Checking..."
                : "Check Double Brand (NAM, ZAR)"}
            </button>
            <button
              type="button"
              className="btn btn--overview tmc-action-main tmc-action-delete"
              onClick={handleDeleteDoubleBrand}
              disabled={doubleBrandLoading}
            >
              Delete Double Brand 
            </button>
            <button
              type="button"
              className="btn btn--overview tmc-action-main tmc-action-calculate"
              onClick={handleCalculateTotalMarket}
              disabled={loading}
            >
              {loading ? "Calculating..." : "Calculate Total Market"}
            </button>
            <button
              type="button"
              className="btn btn--tiny tmc-action-latest"
              onClick={handleShowLatestRaw}
              disabled={loading || latestLoading}
            >
              {latestLoading ? "Loading latest..." : "Show latest"}
            </button>
          </div>
          {showDeleteCaseButtons ? (
            <div className="tmc-delete-case-bar" style={{ marginTop: "12px" }}>
              <button
                type="button"
                className="tmc-delete-case-btn tmc-delete-case-btn--label"
                onClick={handleLoadDeleteDoubleBrandCompactionMachine}
                disabled={compactionLoading}
              >
                Delete Double Brand (Compaction Machine)
              </button>
              {deleteCaseButtons.map((caseName) => (
                <button
                  key={caseName}
                  type="button"
                  className={`tmc-delete-case-btn${
                    selectedDeleteCase === caseName ? " tmc-delete-case-btn--active" : ""
                  }`}
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
          ) : null}
        </div>

        {activeView === "raw" ? (
          <>
            {message ? <p style={{ color: "#0a8f3d", marginBottom: "12px" }}>{message}</p> : null}
            {error ? <p style={{ color: "#d62828", marginBottom: "12px" }}>{error}</p> : null}

            <div className="card-grid card-grid--three" style={{ marginBottom: "16px" }}>
              <article className="card">
                <h4 className="card__title">Loaded Rows</h4>
                <p className="card__text">{rows.length.toLocaleString()}</p>
              </article>
              <article className="card">
                <h4 className="card__title">Source OTH Rows</h4>
                <p className="card__text">{sourceRowCount.toLocaleString()}</p>
              </article>
              <article className="card">
                <h4 className="card__title">Split Machine Lines</h4>
                <p className="card__text">
                  {splitMachineLines.length > 0 ? splitMachineLines.join(", ") : "-"}
                </p>
              </article>
            </div>

            <div className="section summary-card" style={{ marginTop: "8px" }}>
              <strong>Total Market Calculation Input</strong>
              <FilterableTable
                columns={columns}
                rows={rows}
                maxHeight="520px"
                compact
                emptyMessage="No eligible OTH reporter rows found yet."
              />
            </div>
          </>
        ) : null}

        {activeView === "doubleBrand" ? (
          <>
            {doubleBrandMessage ? (
              <p style={{ color: "#0a8f3d", marginTop: "16px", marginBottom: "12px" }}>
                {doubleBrandMessage}
              </p>
            ) : null}
            {doubleBrandError ? (
              <p style={{ color: "#d62828", marginTop: "16px", marginBottom: "12px" }}>
                {doubleBrandError}
              </p>
            ) : null}

            <div className="card-grid card-grid--three" style={{ marginTop: "12px", marginBottom: "16px" }}>
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

            <div className="section summary-card" style={{ marginTop: "8px" }}>
              <strong>
                {doubleBrandMode === "namZar"
                  ? "Report Check Double Brand (NAM, ZAR)"
                  : "Report Check Double Brand"}
              </strong>
              <FilterableTable
                columns={doubleBrandColumns}
                rows={doubleBrandRows}
                maxHeight="520px"
                compact
                emptyMessage={
                  doubleBrandMode === "namZar"
                    ? "No NAM/ZAR duplicate OTH rows found for the same country + machine line code + artificial machine line + size class + brand code."
                    : "No cross-source duplicate OTH rows found for the same country + machine line code + artificial machine line + size class + brand code."
                }
              />
            </div>
          </>
        ) : null}

        {activeView === "deleteDoubleBrand" && compactionRequested ? (
          <>
            {compactionMessage ? (
              <p style={{ color: "#0a8f3d", marginTop: "12px", marginBottom: "12px" }}>
                {compactionMessage}
              </p>
            ) : null}
            {compactionError ? (
              <p style={{ color: "#d62828", marginTop: "12px", marginBottom: "12px" }}>
                {compactionError}
              </p>
            ) : null}

            <div className="card-grid card-grid--three" style={{ marginTop: "12px", marginBottom: "16px" }}>
              <article className="card">
                <h4 className="card__title">Compaction OTH Rows</h4>
                <p className="card__text">{compactionRows.length.toLocaleString()}</p>
              </article>
              <article className="card">
                <h4 className="card__title">Scanned Compaction Rows</h4>
                <p className="card__text">{compactionSourceRowCount.toLocaleString()}</p>
              </article>
              <article className="card">
                <h4 className="card__title">Duplicate Indicator Rule</h4>
                <p className="card__text">Same country + machine line + size class + brand</p>
              </article>
            </div>

            <div className="section summary-card" style={{ marginTop: "8px" }}>
              <strong>Delete Double Brand (Compaction Machine)</strong>
              <div style={{ display: "flex", gap: "8px", marginTop: "10px", marginBottom: "10px", flexWrap: "wrap" }}>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleToggleCompactionEditMode}
                  disabled={compactionRows.length === 0}
                >
                  {compactionEditMode ? "Finish edit mode" : "Edit rows"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleSaveCompactionEdits}
                  disabled={compactionRows.length === 0 || !compactionDirty || snapshotSaving}
                >
                  {snapshotSaving ? "Saving..." : "Save edits"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleResetCompactionEdits}
                  disabled={compactionRows.length === 0 || compactionSavedRows.length === 0}
                >
                  Reset edits
                </button>
              </div>
              <FilterableTable
                columns={compactionColumns}
                rows={compactionRows}
                maxHeight="520px"
                compact
                editable={compactionEditMode}
                onRowsChange={handleCompactionRowsChange}
                onDeleteRow={handleCompactionDeleteRow}
                nonEditableColumns={["db_indicator_by_country", "source_flag"]}
                emptyMessage="No Compaction Machine OTH rows found."
              />
            </div>
          </>
        ) : null}

        {activeView === "deleteDoubleBrand" && ybrPinRequested ? (
          <>
            {ybrPinMessage ? (
              <p style={{ color: "#0a8f3d", marginTop: "12px", marginBottom: "12px" }}>
                {ybrPinMessage}
              </p>
            ) : null}
            {ybrPinError ? (
              <p style={{ color: "#d62828", marginTop: "12px", marginBottom: "12px" }}>
                {ybrPinError}
              </p>
            ) : null}

            <div className="section summary-card" style={{ marginTop: "8px" }}>
              <strong>YBR/PIN Case</strong>
              <div style={{ display: "flex", gap: "8px", marginTop: "10px", marginBottom: "10px", flexWrap: "wrap" }}>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleApplyYbrPinRule}
                  disabled={ybrPinLoading || ybrPinRuleApplying || ybrPinRows.length === 0}
                >
                  {ybrPinRuleApplying ? "Applying..." : "Apply YBR/PIN Rule"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleToggleYbrPinEditMode}
                  disabled={ybrPinLoading || ybrPinRows.length === 0 || !ybrPinRuleApplied}
                >
                  {ybrPinEditMode ? "Finish edit mode" : "Edit rows"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleSaveYbrPinEdits}
                  disabled={ybrPinRows.length === 0 || !ybrPinDirty || !ybrPinRuleApplied || snapshotSaving}
                >
                  {snapshotSaving ? "Saving..." : "Save edits"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleResetYbrPinEdits}
                  disabled={ybrPinRows.length === 0 || ybrPinSavedRows.length === 0 || !ybrPinRuleApplied}
                >
                  Reset edits
                </button>
              </div>
              <FilterableTable
                columns={ybrPinColumns}
                rows={ybrPinRows}
                maxHeight="520px"
                compact
                editable={ybrPinEditMode}
                onRowsChange={handleYbrPinRowsChange}
                nonEditableColumns={[
                  "year",
                  "source",
                  "country_code",
                  "country",
                  "country_grouping",
                  "region",
                  "market_area",
                  "machine_line_name",
                  "machine_line_code",
                  "artificial_machine_line",
                  "brand_name",
                  "brand_code",
                  "size_class_flag",
                  "original_fid",
                  "source_flag",
                ]}
                emptyMessage="No rows found for Source in (YBR, PIN)."
              />
            </div>
          </>
        ) : null}

        {activeView === "deleteDoubleBrand" && ocnOtnRequested ? (
          <>
            {ocnOtnMessage ? (
              <p style={{ color: "#0a8f3d", marginTop: "12px", marginBottom: "12px" }}>
                {ocnOtnMessage}
              </p>
            ) : null}
            {ocnOtnError ? (
              <p style={{ color: "#d62828", marginTop: "12px", marginBottom: "12px" }}>
                {ocnOtnError}
              </p>
            ) : null}

            <div className="section summary-card" style={{ marginTop: "8px" }}>
              <strong>OCN/OTN Case</strong>
              <div style={{ display: "flex", gap: "8px", marginTop: "10px", marginBottom: "10px", flexWrap: "wrap" }}>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleApplyOcnOtnRule}
                  disabled={ocnOtnLoading || ocnOtnRuleApplying || ocnOtnRows.length === 0}
                >
                  {ocnOtnRuleApplying ? "Applying..." : "Apply OCN/OTN Rule"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleToggleOcnOtnEditMode}
                  disabled={ocnOtnLoading || ocnOtnRows.length === 0 || !ocnOtnRuleApplied}
                >
                  {ocnOtnEditMode ? "Finish edit mode" : "Edit rows"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleSaveOcnOtnEdits}
                  disabled={ocnOtnRows.length === 0 || !ocnOtnDirty || !ocnOtnRuleApplied || snapshotSaving}
                >
                  {snapshotSaving ? "Saving..." : "Save edits"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleResetOcnOtnEdits}
                  disabled={ocnOtnRows.length === 0 || ocnOtnSavedRows.length === 0 || !ocnOtnRuleApplied}
                >
                  Reset edits
                </button>
              </div>
              <FilterableTable
                columns={ocnOtnColumns}
                rows={ocnOtnRows}
                maxHeight="520px"
                compact
                editable={ocnOtnEditMode}
                onRowsChange={handleOcnOtnRowsChange}
                nonEditableColumns={[
                  "year",
                  "source",
                  "country_code",
                  "country",
                  "country_grouping",
                  "region",
                  "market_area",
                  "machine_line_name",
                  "machine_line_code",
                  "artificial_machine_line",
                  "brand_name",
                  "brand_code",
                  "size_class_flag",
                  "original_fid",
                  "source_flag",
                ]}
                emptyMessage="No rows found for Source in (OCN, OTN)."
              />
            </div>
          </>
        ) : null}

        {activeView === "deleteDoubleBrand" && ohrPinRequested ? (
          <>
            {ohrPinMessage ? (
              <p style={{ color: "#0a8f3d", marginTop: "12px", marginBottom: "12px" }}>
                {ohrPinMessage}
              </p>
            ) : null}
            {ohrPinError ? (
              <p style={{ color: "#d62828", marginTop: "12px", marginBottom: "12px" }}>
                {ohrPinError}
              </p>
            ) : null}

            <div className="section summary-card" style={{ marginTop: "8px" }}>
              <strong>OHR/PIN Case</strong>
              <div style={{ display: "flex", gap: "8px", marginTop: "10px", marginBottom: "10px", flexWrap: "wrap" }}>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleApplyOhrPinRule}
                  disabled={ohrPinLoading || ohrPinRuleApplying || ohrPinRows.length === 0}
                >
                  {ohrPinRuleApplying ? "Applying..." : "Apply OHR/PIN Rule"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleToggleOhrPinEditMode}
                  disabled={ohrPinLoading || ohrPinRows.length === 0 || !ohrPinRuleApplied}
                >
                  {ohrPinEditMode ? "Finish edit mode" : "Edit rows"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleSaveOhrPinEdits}
                  disabled={ohrPinRows.length === 0 || !ohrPinDirty || !ohrPinRuleApplied || snapshotSaving}
                >
                  {snapshotSaving ? "Saving..." : "Save edits"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleResetOhrPinEdits}
                  disabled={ohrPinRows.length === 0 || ohrPinSavedRows.length === 0 || !ohrPinRuleApplied}
                >
                  Reset edits
                </button>
              </div>
              <FilterableTable
                columns={ohrPinColumns}
                rows={ohrPinRows}
                maxHeight="520px"
                compact
                editable={ohrPinEditMode}
                onRowsChange={handleOhrPinRowsChange}
                nonEditableColumns={[
                  "year",
                  "source",
                  "country_code",
                  "country",
                  "country_grouping",
                  "region",
                  "market_area",
                  "machine_line_name",
                  "machine_line_code",
                  "artificial_machine_line",
                  "brand_name",
                  "brand_code",
                  "size_class_flag",
                  "original_fid",
                  "source_flag",
                ]}
                emptyMessage="No rows found for Source in (OHR, PIN)."
              />
            </div>
          </>
        ) : null}

        {activeView === "deleteDoubleBrand" && rimPinRequested ? (
          <>
            {rimPinMessage ? (
              <p style={{ color: "#0a8f3d", marginTop: "12px", marginBottom: "12px" }}>
                {rimPinMessage}
              </p>
            ) : null}
            {rimPinError ? (
              <p style={{ color: "#d62828", marginTop: "12px", marginBottom: "12px" }}>
                {rimPinError}
              </p>
            ) : null}

            <div className="section summary-card" style={{ marginTop: "8px" }}>
              <strong>RIM/PIN Case</strong>
              <div style={{ display: "flex", gap: "8px", marginTop: "10px", marginBottom: "10px", flexWrap: "wrap" }}>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleApplyRimPinRule}
                  disabled={rimPinLoading || rimPinRuleApplying || rimPinRows.length === 0}
                >
                  {rimPinRuleApplying ? "Applying..." : "Apply RIM/PIN Rule"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleToggleRimPinEditMode}
                  disabled={rimPinLoading || rimPinRows.length === 0 || !rimPinRuleApplied}
                >
                  {rimPinEditMode ? "Finish edit mode" : "Edit rows"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleSaveRimPinEdits}
                  disabled={rimPinRows.length === 0 || !rimPinDirty || !rimPinRuleApplied || snapshotSaving}
                >
                  {snapshotSaving ? "Saving..." : "Save edits"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleResetRimPinEdits}
                  disabled={rimPinRows.length === 0 || rimPinSavedRows.length === 0 || !rimPinRuleApplied}
                >
                  Reset edits
                </button>
              </div>
              <FilterableTable
                columns={rimPinColumns}
                rows={rimPinRows}
                maxHeight="520px"
                compact
                editable={rimPinEditMode}
                onRowsChange={handleRimPinRowsChange}
                nonEditableColumns={[
                  "year",
                  "source",
                  "country_code",
                  "country",
                  "country_grouping",
                  "region",
                  "market_area",
                  "machine_line_name",
                  "machine_line_code",
                  "artificial_machine_line",
                  "brand_name",
                  "brand_code",
                  "size_class_flag",
                  "original_fid",
                  "source_flag",
                ]}
                emptyMessage="No rows found for Source in (RIM, PIN)."
              />
            </div>
          </>
        ) : null}

        {activeView === "deleteDoubleBrand" && ergPinRequested ? (
          <>
            {ergPinMessage ? (
              <p style={{ color: "#0a8f3d", marginTop: "12px", marginBottom: "12px" }}>
                {ergPinMessage}
              </p>
            ) : null}
            {ergPinError ? (
              <p style={{ color: "#d62828", marginTop: "12px", marginBottom: "12px" }}>
                {ergPinError}
              </p>
            ) : null}

            <div className="section summary-card" style={{ marginTop: "8px" }}>
              <strong>ERG/PIN Case</strong>
              <div style={{ display: "flex", gap: "8px", marginTop: "10px", marginBottom: "10px", flexWrap: "wrap" }}>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleApplyErgPinRule}
                  disabled={ergPinLoading || ergPinRuleApplying || ergPinRows.length === 0}
                >
                  {ergPinRuleApplying ? "Applying..." : "Apply ERG/PIN Rule"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleToggleErgPinEditMode}
                  disabled={ergPinLoading || ergPinRows.length === 0 || !ergPinRuleApplied}
                >
                  {ergPinEditMode ? "Finish edit mode" : "Edit rows"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleSaveErgPinEdits}
                  disabled={ergPinRows.length === 0 || !ergPinDirty || !ergPinRuleApplied || snapshotSaving}
                >
                  {snapshotSaving ? "Saving..." : "Save edits"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleResetErgPinEdits}
                  disabled={ergPinRows.length === 0 || ergPinSavedRows.length === 0 || !ergPinRuleApplied}
                >
                  Reset edits
                </button>
              </div>
              <FilterableTable
                columns={ergPinColumns}
                rows={ergPinRows}
                maxHeight="520px"
                compact
                editable={ergPinEditMode}
                onRowsChange={handleErgPinRowsChange}
                nonEditableColumns={[
                  "year",
                  "source",
                  "country_code",
                  "country",
                  "country_grouping",
                  "region",
                  "market_area",
                  "machine_line_name",
                  "machine_line_code",
                  "artificial_machine_line",
                  "brand_name",
                  "brand_code",
                  "size_class_flag",
                  "original_fid",
                  "source_flag",
                ]}
                emptyMessage="No rows found for Source in (ERG, PIN)."
              />
            </div>
          </>
        ) : null}

        {activeView === "deleteDoubleBrand" && cmaOhrRequested ? (
          <>
            {cmaOhrMessage ? (
              <p style={{ color: "#0a8f3d", marginTop: "12px", marginBottom: "12px" }}>
                {cmaOhrMessage}
              </p>
            ) : null}
            {cmaOhrError ? (
              <p style={{ color: "#d62828", marginTop: "12px", marginBottom: "12px" }}>
                {cmaOhrError}
              </p>
            ) : null}

            <div className="section summary-card" style={{ marginTop: "8px" }}>
              <strong>CMA/OHR Case</strong>
              <div style={{ display: "flex", gap: "8px", marginTop: "10px", marginBottom: "10px", flexWrap: "wrap" }}>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleApplyCmaOhrRule}
                  disabled={cmaOhrLoading || cmaOhrRuleApplying || cmaOhrRows.length === 0}
                >
                  {cmaOhrRuleApplying ? "Applying..." : "Apply CMA/OHR Rule"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleToggleCmaOhrEditMode}
                  disabled={cmaOhrLoading || cmaOhrRows.length === 0 || !cmaOhrRuleApplied}
                >
                  {cmaOhrEditMode ? "Finish edit mode" : "Edit rows"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleSaveCmaOhrEdits}
                  disabled={cmaOhrRows.length === 0 || !cmaOhrDirty || !cmaOhrRuleApplied || snapshotSaving}
                >
                  {snapshotSaving ? "Saving..." : "Save edits"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleResetCmaOhrEdits}
                  disabled={cmaOhrRows.length === 0 || cmaOhrSavedRows.length === 0 || !cmaOhrRuleApplied}
                >
                  Reset edits
                </button>
              </div>
              <FilterableTable
                columns={cmaOhrColumns}
                rows={cmaOhrRows}
                maxHeight="520px"
                compact
                editable={cmaOhrEditMode}
                onRowsChange={handleCmaOhrRowsChange}
                nonEditableColumns={[
                  "year",
                  "source",
                  "country_code",
                  "country",
                  "country_grouping",
                  "region",
                  "market_area",
                  "machine_line_name",
                  "machine_line_code",
                  "artificial_machine_line",
                  "brand_name",
                  "brand_code",
                  "size_class_flag",
                  "original_fid",
                  "source_flag",
                ]}
                emptyMessage="No rows found for Source in (CMA, CMM, OHR)."
              />
            </div>
          </>
        ) : null}

        {activeView === "deleteDoubleBrand" && cnxRequested ? (
          <>
            {cnxMessage ? (
              <p style={{ color: "#0a8f3d", marginTop: "12px", marginBottom: "12px" }}>
                {cnxMessage}
              </p>
            ) : null}
            {cnxError ? (
              <p style={{ color: "#d62828", marginTop: "12px", marginBottom: "12px" }}>
                {cnxError}
              </p>
            ) : null}

            <div className="section summary-card" style={{ marginTop: "8px" }}>
              <strong>CNX Case</strong>
              <div style={{ display: "flex", gap: "8px", marginTop: "10px", marginBottom: "10px", flexWrap: "wrap" }}>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleApplyCnxRule}
                  disabled={cnxLoading || cnxRuleApplying || cnxRows.length === 0}
                >
                  {cnxRuleApplying ? "Applying..." : "Apply CNX Rule"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleToggleCnxEditMode}
                  disabled={cnxLoading || cnxRows.length === 0 || !cnxRuleApplied}
                >
                  {cnxEditMode ? "Finish edit mode" : "Edit rows"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleSaveCnxEdits}
                  disabled={cnxRows.length === 0 || !cnxDirty || !cnxRuleApplied || snapshotSaving}
                >
                  {snapshotSaving ? "Saving..." : "Save edits"}
                </button>
                <button
                  type="button"
                  className="btn btn--tiny"
                  onClick={handleResetCnxEdits}
                  disabled={cnxRows.length === 0 || cnxSavedRows.length === 0 || !cnxRuleApplied}
                >
                  Reset edits
                </button>
              </div>
              <FilterableTable
                columns={cnxColumns}
                rows={cnxRows}
                maxHeight="520px"
                compact
                editable={cnxEditMode}
                onRowsChange={handleCnxRowsChange}
                nonEditableColumns={[
                  "year",
                  "source",
                  "country_code",
                  "country",
                  "country_grouping",
                  "region",
                  "market_area",
                  "machine_line_name",
                  "machine_line_code",
                  "artificial_machine_line",
                  "brand_name",
                  "brand_code",
                  "size_class_flag",
                  "original_fid",
                  "source_flag",
                ]}
                emptyMessage="No CNX rows found."
              />
            </div>
          </>
        ) : null}
      </section>
    </div>
  );
}

export default TotalMarketCalculationPage;


