import { useEffect, useMemo, useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";
import FilterableTable from "../components/table/FilterableTable";
import {
  getA10AdjustmentReport,
  getCrpD1CombinedReport,
  getExcavatorsSplitCaseRun,
  getLatestUploadByMatrixType,
  getLatestCrpD1CombinedReport,
  getLatestOthDeletionFlagReport,
  getLatestExcavatorsSplitCaseReport,
  getLatestP00ThreeCheckReport,
  getOthDeletionFlagReport,
  getP00ThreeCheckReport,
  getP10VceNonVceReport,
  runExcavatorsSplitCaseReport,
  saveExcavatorsSplitCaseSnapshot,
  saveEditedUpload,
} from "../api/uploads";
import type {
  A10AdjustmentRow,
  CrpD1CombinedReportRow,
  OthDeletionFlagRow,
  P00ThreeCheckRow,
  P10VceNonVceRow,
  UploadRow,
} from "../types/upload";

type LayerDetail = {
  code: string;
  title: string;
  description: string;
  highlights: string[];
};

type ExcavatorsSplitCaseRow = {
  year: string;
  machine_line_name: string;
  machine_line_code: string;
  source: string;
  size_class_flag: string;
  matched_rows: number;
  gross_fid: number;
  volvo_deduction: number;
  net_fid: number;
};

type ExcavatorsSplitDetailRow = {
  row_type: string;
  year: string;
  country_grouping: string;
  country: string;
  region: string;
  machine_line: string;
  artificial_machine_line: string;
  brand_code: string;
  reporter_flag: string;
  source: string;
  pri_sec: string;
  size_class: string;
  before_split_fid_lt_10t: number | "";
  copy_fid_lt_10t: number | "";
  after_split_fid_lt_6t: number | "";
  after_split_fid_6_10t: number | "";
  after_split_fid_target_three?: number | "";
  tm_non_vce_lt_6t: number | "";
  tm_non_vce_6_10t: number | "";
  tm_non_vce_target_three?: number | "";
  resplit?: string;
  after_resplit_fid_lt_6t?: number | "";
  after_resplit_fid_6_10t?: number | "";
  after_resplit_fid_target_three?: number | "";
  before_after_difference: number | "";
  reference_level?: string;
  split_ratio?: string;
};

type ExcavatorsSplitCaseType = "ALL" | "CEX" | "GEC" | "GEW";
type WheelLoadersSplitCaseType = "ALL" | "WLO_GT10" | "WLO_LT10" | "WLO_LT12";
type SplitDetailCaseType = ExcavatorsSplitCaseType | "WLO_GT10" | "WLO_LT10" | "WLO_LT12";
type ExcavatorsSplitDetailConfig = {
  inputSizeKeys: string[];
  inputSizeLabel: string;
  firstTargetKeys: string[];
  firstTargetLabel: string;
  secondTargetKeys: string[];
  secondTargetLabel: string;
  thirdTargetKeys?: string[];
  thirdTargetLabel?: string;
  panelTitle: string;
  description: string;
};

function toMatchKey(value: string | number | null | undefined): string {
  return String(value ?? "")
    .trim()
    .replace(/＜/g, "<")
    .replace(/\s+/g, "")
    .toUpperCase();
}

function toNumberValue(value: string | number | null | undefined): number {
  if (typeof value === "number") {
    return Number.isFinite(value) ? value : 0;
  }

  const raw = String(value ?? "").trim();
  if (!raw) {
    return 0;
  }

  const compact = raw.replace(/\s+/g, "");
  let normalized = compact;

  if (compact.includes(",") && compact.includes(".")) {
    normalized =
      compact.lastIndexOf(",") > compact.lastIndexOf(".")
        ? compact.replace(/\./g, "").replace(",", ".")
        : compact.replace(/,/g, "");
  } else if (compact.includes(",")) {
    const parts = compact.split(",");
    normalized =
      parts.length === 2 && parts[1].length <= 4
        ? `${parts[0].replace(/\./g, "")}.${parts[1]}`
        : compact.replace(/,/g, "");
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : 0;
}

function formatElapsedTime(totalSeconds: number): string {
  const safeSeconds = Math.max(0, Math.floor(totalSeconds));
  const minutes = Math.floor(safeSeconds / 60);
  const seconds = safeSeconds % 60;
  if (minutes <= 0) {
    return `${seconds}s`;
  }
  return `${minutes}m ${String(seconds).padStart(2, "0")}s`;
}

function roundTo4(value: number): number {
  return Number(value.toFixed(4));
}

function formatNumberDisplay(value: number, fractionDigits = 2): string {
  return value.toLocaleString(undefined, {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  });
}

function normalizeSizeClassForResplit(value: string | number | null | undefined): string {
  const key = toMatchKey(value);
  if (key === "MINI") {
    return "<6T";
  }
  if (key === "MIDI") {
    return "6<10T";
  }
  return key;
}

function initializeResplitColumns(rows: ExcavatorsSplitDetailRow[]): ExcavatorsSplitDetailRow[] {
  return rows.map((row) => ({
    ...row,
    resplit: "",
    after_resplit_fid_lt_6t: "",
    after_resplit_fid_6_10t: "",
    after_resplit_fid_target_three: "",
  }));
}

function applyExcavatorsResplitByCrpSource(
  rows: ExcavatorsSplitDetailRow[],
  sourceMatrixRows: UploadRow[],
  sizeClassRows: UploadRow[],
  detailConfig: ExcavatorsSplitDetailConfig
): ExcavatorsSplitDetailRow[] {
  const countryArtificialToCrpSource = new Map<string, string>();
  sourceMatrixRows.forEach((row) => {
    const countryKey = toMatchKey(row.country_name ?? row.country);
    const artificialKey = toMatchKey(row.artificial_machine_line);
    const crpSourceKey = toMatchKey(row.crp_source ?? row.source_code);
    if (!countryKey || !artificialKey || !crpSourceKey) {
      return;
    }
    const key = `${countryKey}|${artificialKey}`;
    if (!countryArtificialToCrpSource.has(key)) {
      countryArtificialToCrpSource.set(key, crpSourceKey);
    }
  });

  const unsoldSizeClassBySourceBrandMachine = new Map<string, Set<string>>();
  sizeClassRows.forEach((row) => {
    const sourceCodeKey = toMatchKey(row.source_code ?? row.source);
    const brandCodeKey = toMatchKey(row.brand_code);
    const machineCodeKey = toMatchKey(row.machine_code);
    const sizeClassKey = normalizeSizeClassForResplit(row.size_class);
    if (!sourceCodeKey || !brandCodeKey || !machineCodeKey || !sizeClassKey) {
      return;
    }
    const key = `${sourceCodeKey}|${brandCodeKey}|${machineCodeKey}`;
    if (!unsoldSizeClassBySourceBrandMachine.has(key)) {
      unsoldSizeClassBySourceBrandMachine.set(key, new Set<string>());
    }
    unsoldSizeClassBySourceBrandMachine.get(key)?.add(sizeClassKey);
  });

  return rows.map((row) => {
    if (toMatchKey(row.row_type) !== "OTH") {
      return {
        ...row,
        resplit: "",
        after_resplit_fid_lt_6t: "",
        after_resplit_fid_6_10t: "",
        after_resplit_fid_target_three: "",
      };
    }

    const countryKey = toMatchKey(row.country);
    const artificialKey = toMatchKey(row.artificial_machine_line);
    const brandCodeKey = toMatchKey(row.brand_code);
    const crpSourceKey = countryArtificialToCrpSource.get(`${countryKey}|${artificialKey}`) ?? "";

    const firstSizeClass = normalizeSizeClassForResplit(detailConfig.firstTargetLabel);
    const secondSizeClass = normalizeSizeClassForResplit(detailConfig.secondTargetLabel);
    const thirdSizeClass = detailConfig.thirdTargetLabel
      ? normalizeSizeClassForResplit(detailConfig.thirdTargetLabel)
      : "";

    const beforeFirst = roundTo4(toNumberValue(row.after_split_fid_lt_6t));
    const beforeSecond = roundTo4(toNumberValue(row.after_split_fid_6_10t));
    const beforeThird = detailConfig.thirdTargetLabel
      ? roundTo4(toNumberValue(row.after_split_fid_target_three))
      : 0;
    const beforeTotal = roundTo4(beforeFirst + beforeSecond + beforeThird);

    let afterFirst = beforeFirst;
    let afterSecond = beforeSecond;
    let afterThird = beforeThird;

    const unsoldSet =
      unsoldSizeClassBySourceBrandMachine.get(`${crpSourceKey}|${brandCodeKey}|${artificialKey}`) ??
      new Set<string>();
    const unsoldFirst = unsoldSet.has(firstSizeClass);
    const unsoldSecond = unsoldSet.has(secondSizeClass);
    const unsoldThird = thirdSizeClass ? unsoldSet.has(thirdSizeClass) : false;
    const triggeredLabels: string[] = [];

    if (unsoldFirst && beforeFirst > 0) {
      triggeredLabels.push(detailConfig.firstTargetLabel);
    }
    if (unsoldSecond && beforeSecond > 0) {
      triggeredLabels.push(detailConfig.secondTargetLabel);
    }
    if (detailConfig.thirdTargetLabel && unsoldThird && beforeThird > 0) {
      triggeredLabels.push(detailConfig.thirdTargetLabel);
    }

    if (detailConfig.thirdTargetLabel) {
      const targetSpecs = [
        {
          label: detailConfig.firstTargetLabel,
          blocked: unsoldFirst,
          value: beforeFirst,
        },
        {
          label: detailConfig.secondTargetLabel,
          blocked: unsoldSecond,
          value: beforeSecond,
        },
        {
          label: detailConfig.thirdTargetLabel,
          blocked: unsoldThird,
          value: beforeThird,
        },
      ];
      const allowedTargets = targetSpecs.filter((spec) => !spec.blocked);
      const allowedWeight = allowedTargets.reduce((sum, spec) => sum + spec.value, 0);

      if (triggeredLabels.length > 0) {
        if (allowedTargets.length === 0) {
          afterFirst = 0;
          afterSecond = 0;
          afterThird = 0;
        } else if (allowedTargets.length === 1) {
          afterFirst = targetSpecs[0].label === allowedTargets[0].label ? beforeTotal : 0;
          afterSecond = targetSpecs[1].label === allowedTargets[0].label ? beforeTotal : 0;
          afterThird = targetSpecs[2].label === allowedTargets[0].label ? beforeTotal : 0;
        } else {
          let remaining = beforeTotal;
          const assignedByLabel = new Map<string, number>();

          allowedTargets.forEach((spec, index) => {
            if (index === allowedTargets.length - 1) {
              assignedByLabel.set(spec.label, roundTo4(remaining));
              return;
            }

            const rawShare =
              allowedWeight > 0
                ? (beforeTotal * spec.value) / allowedWeight
                : beforeTotal / allowedTargets.length;
            const share = roundTo4(rawShare);
            assignedByLabel.set(spec.label, share);
            remaining = roundTo4(remaining - share);
          });

          afterFirst = assignedByLabel.get(targetSpecs[0].label) ?? 0;
          afterSecond = assignedByLabel.get(targetSpecs[1].label) ?? 0;
          afterThird = assignedByLabel.get(targetSpecs[2].label) ?? 0;
        }
      }
    } else if (unsoldFirst && !unsoldSecond) {
      afterSecond = roundTo4(beforeSecond + beforeFirst);
      afterFirst = 0;
    } else if (!unsoldFirst && unsoldSecond) {
      afterFirst = roundTo4(beforeFirst + beforeSecond);
      afterSecond = 0;
    }

    return {
      ...row,
      resplit: triggeredLabels.length > 0 ? `Y(${triggeredLabels.join(", ")})` : "",
      after_resplit_fid_lt_6t: afterFirst,
      after_resplit_fid_6_10t: afterSecond,
      after_resplit_fid_target_three: detailConfig.thirdTargetLabel ? afterThird : "",
    };
  });
}

function getExcavatorsSplitDetailConfig(
  caseType: SplitDetailCaseType
): ExcavatorsSplitDetailConfig | null {
  if (caseType === "CEX") {
    return {
      inputSizeKeys: ["<10T"],
      inputSizeLabel: "<10T",
      firstTargetKeys: ["<6T"],
      firstTargetLabel: "<6T",
      secondTargetKeys: ["6<10T"],
      secondTargetLabel: "6<10T",
      panelTitle: "CEX Split Detail",
      description:
        "Lists the CEX OTH rows to split for <10T together with the related CEX TMA rows.",
    };
  }

  if (caseType === "GEC") {
    return {
      inputSizeKeys: [">6T"],
      inputSizeLabel: ">6T",
      firstTargetKeys: [">10T"],
      firstTargetLabel: ">10T",
      secondTargetKeys: ["6<10T"],
      secondTargetLabel: "6<10T",
      panelTitle: "GEC Split Detail",
      description:
        "Lists the GEC OTH rows to split for >6T together with the related GEC TMA rows, then splits into >10T and 6<10T using Country, then Region, then Country Grouping fallback.",
    };
  }

  if (caseType === "GEW") {
    return {
      inputSizeKeys: [">6T"],
      inputSizeLabel: ">6T",
      firstTargetKeys: ["6<11T"],
      firstTargetLabel: "6<11T",
      secondTargetKeys: [">11T"],
      secondTargetLabel: ">11T",
      panelTitle: "GEW Split Detail",
      description:
        "Lists the GEW OTH rows to split for >6T together with the related GEW TMA rows, then splits into 6<11T and >11T using Country, then Region, then Country Grouping fallback.",
    };
  }

  if (caseType === "WLO_GT10") {
    return {
      inputSizeKeys: [">10"],
      inputSizeLabel: ">10",
      firstTargetKeys: ["10<12"],
      firstTargetLabel: "10<12",
      secondTargetKeys: [">12"],
      secondTargetLabel: ">12",
      panelTitle: "WLO Split Detail (>10)",
      description:
        "Lists the WLO OTH rows to split for >10 together with the related WLO TMA rows, then splits into 10<12 and >12 using Country, then Region, then Country Grouping fallback.",
    };
  }

  if (caseType === "WLO_LT10") {
    return {
      inputSizeKeys: ["<10"],
      inputSizeLabel: "<10",
      firstTargetKeys: ["7<10"],
      firstTargetLabel: "7<10",
      secondTargetKeys: ["<7"],
      secondTargetLabel: "<7",
      panelTitle: "WLO Split Detail (<10)",
      description:
        "Lists the WLO OTH rows to split for <10 together with the related WLO TMA rows, then splits into 7<10 and <7 using Country, then Region, then Country Grouping fallback.",
    };
  }

  if (caseType === "WLO_LT12") {
    return {
      inputSizeKeys: ["<12"],
      inputSizeLabel: "<12",
      firstTargetKeys: ["10<12"],
      firstTargetLabel: "10<12",
      secondTargetKeys: ["7<10"],
      secondTargetLabel: "7<10",
      thirdTargetKeys: ["<7"],
      thirdTargetLabel: "<7",
      panelTitle: "WLO Split Detail (<12)",
      description:
        "Lists the WLO OTH rows to split for <12 together with the related WLO TMA rows, then splits into 10<12, 7<10, and <7 using Country, then Region, then Country Grouping fallback.",
    };
  }

  return null;
}

const EXCAVATORS_SPLIT_CASE_DETAILS: Record<
  ExcavatorsSplitCaseType,
  { heading: string; buttonLabel: string; panelTitle: string; description: string }
> = {
  ALL: {
    heading: "Excavators Split",
    buttonLabel: "Excavators Split Case",
    panelTitle: "Excavators Split Case",
    description:
      "Filtered from OTH rows where Reporter Flag = Y, with either Artificial machine line = CEX and Size Class Flag = <10T, Artificial machine line = GEC and Size Class Flag = >6T, or Artificial machine line = GEW and Size Class Flag = >6T. Rows with Brand Name = VOLVO are subtracted from FID.",
  },
  CEX: {
    heading: "1. Split CEX <10T",
    buttonLabel: "Split CEX Case <10T",
    panelTitle: "Split CEX Case",
    description:
      "Lists CEX OTH rows with Reporter Flag = Y and Size Class Flag = <10T, then splits non-Volvo FID by TMA Non-VCE structure using Country, then Region, then Country Grouping fallback.",
  },
  GEC: {
    heading: "2. Split GEC >6T",
    buttonLabel: "Split GEC >6T",
    panelTitle: "Split GEC Case",
    description:
      "Filtered from OTH rows where Reporter Flag = Y, Artificial machine line = GEC, and Size Class Flag = >6T. Rows with Brand Name = VOLVO are subtracted from FID.",
  },
  GEW: {
    heading: "3. Split GEW >6T",
    buttonLabel: "Split GEW >6T",
    panelTitle: "Split GEW Case",
    description:
      "Filtered from OTH rows where Reporter Flag = Y, Artificial machine line = GEW, and Size Class Flag = >6T. Rows with Brand Name = VOLVO are subtracted from FID.",
  },
};

const WHEEL_LOADERS_SPLIT_CASE_DETAILS: Record<
  WheelLoadersSplitCaseType,
  { heading: string; buttonLabel: string; panelTitle: string; description: string }
> = {
  ALL: {
    heading: "Wheel Loaders Split",
    buttonLabel: "Wheel Loaders Split Case",
    panelTitle: "Wheel Loaders Split Case",
    description:
      "Filtered from OTH rows where Reporter Flag = Y, Artificial machine line = WLO, and Size Class Flag is >10, <10, or <12. Rows with Brand Name = VOLVO are subtracted from FID.",
  },
  WLO_GT10: {
    heading: "1. Split WLO (>10)",
    buttonLabel: "Split WLO (>10) Case",
    panelTitle: "Split WLO (>10) Case",
    description:
      "Filtered from OTH rows where Reporter Flag = Y, Artificial machine line = WLO, and Size Class Flag = >10. Rows with Brand Name = VOLVO are subtracted from FID.",
  },
  WLO_LT10: {
    heading: "2. Split WLO (<10)",
    buttonLabel: "Split WLO (<10) Case",
    panelTitle: "Split WLO (<10) Case",
    description:
      "Filtered from OTH rows where Reporter Flag = Y, Artificial machine line = WLO, and Size Class Flag = <10. Rows with Brand Name = VOLVO are subtracted from FID.",
  },
  WLO_LT12: {
    heading: "3. Split WLO (<12)",
    buttonLabel: "Split WLO (<12) Case",
    panelTitle: "Split WLO (<12) Case",
    description:
      "Filtered from OTH rows where Reporter Flag = Y, Artificial machine line = WLO, and Size Class Flag = <12. Rows with Brand Name = VOLVO are subtracted from FID.",
  },
};

const LAYER_DETAILS: Record<string, LayerDetail> = {
  P00: {
    code: "P00",
    title: "Preparation Raw Layer",
    description: "",
    highlights: [
      "1. For each CRP record, determine whether it should be deleted and whether it is classified as a reporter.",
      "2. For each OTH record, prepare mapped control report fields.",
      "2.1 Mark Deletion flag: Y if Machine Line Code = 390; otherwise Y when Country + Artificial machine line is not found in Source Matrix.",
      "2.2 Assign Pri/Sec: match Source + Country + Artificial machine line to Source Matrix. If Source equals primary_source then P; if Source equals secondary_source then S; otherwise blank.",
      "2.3 Assign Reporter flag: first read CRP Source from Source Matrix by Country + Artificial machine line, then match Reporter List by source_code + Artificial machine line + brand_code. If matched, set Y.",
      "3. Build one combined Check Report that puts TMA, SAL, and OTH into one table and shows TM, VCE FID, and TM Non VCE for the matched group.",
    ],
  },
  P10: {
    code: "P10",
    title: "Prepared Layer",
    description: "Compute and display TMA, Volvo CE (VCE), and Non-Volvo CE values.",
    highlights: [
      "TMA (Total Market) comes from TMA source records.",
      "Rows with Volvo Deletion Flag = Y are excluded from the P10 report output.",
      "VCE includes Volvo/SAL rows where CRP Source + Artificial machine line + brand_code matches Reporter List, excluding Motor Graders.",
      "Non-VCE = max(TMA - VCE, 0).",
    ],
  },
  A10: {
    code: "A10",
    title: "Adjustment Layer",
    description: "Current A10 output summarizes SAL and TMA rows into one reviewable result structure.",
    highlights: [
      "Shows original SAL rows, original TMA rows, and one derived Result row for each matched group.",
      "Uses the same country and machine-line matching basis as the current P00/P10 preparation logic.",
      "Supports review of VCE, TM FID, and TM Non VCE before later split logic is introduced.",
    ],
  },
  A20: {
    code: "A20",
    title: "Final Adjustment Layer",
    description: "Finalized adjusted results for downstream review and analysis.",
    highlights: [
      "Finalizes adjustment outputs for reporting readiness.",
      "Provides stable result set for downstream consumers.",
      "Serves as the final stage in current prototype scope.",
    ],
  },
  MLS: {
    code: "MLS",
    title: "Machine Line Split Layer",
    description: "Planned machine line split stage that follows the current adjustment flow.",
    highlights: ["Excavators Split"],
  },
};

const CRP_D1_COMBINED_SQL = `WITH latest_tma AS (
  SELECT id FROM upload_runs
  WHERE matrix_type = 'tma_data' AND status = 'success'
  ORDER BY uploaded_at DESC, id DESC
  LIMIT 1
),
latest_volvo AS (
  SELECT id FROM upload_runs
  WHERE matrix_type = 'volvo_sale_data' AND status = 'success'
  ORDER BY uploaded_at DESC, id DESC
  LIMIT 1
),
latest_group_country AS (
  SELECT id FROM upload_runs
  WHERE matrix_type = 'group_country' AND status = 'success'
  ORDER BY uploaded_at DESC, id DESC
  LIMIT 1
),
latest_source_matrix AS (
  SELECT id FROM upload_runs
  WHERE matrix_type = 'source_matrix' AND status = 'success'
  ORDER BY uploaded_at DESC, id DESC
  LIMIT 1
),
latest_machine_line_mapping AS (
  SELECT id FROM upload_runs
  WHERE matrix_type = 'machine_line_mapping' AND status = 'success'
  ORDER BY uploaded_at DESC, id DESC
  LIMIT 1
),
gc_by_code AS (
  SELECT
    UPPER(TRIM(country_code)) AS country_code_key,
    UPPER(TRIM(year)) AS year_key,
    MIN(group_code) AS group_code,
    MIN(country_grouping) AS country_grouping,
    MIN(country_name) AS country_name,
    MIN(region) AS region
  FROM group_country_rows
  WHERE upload_run_id = (SELECT id FROM latest_group_country)
  GROUP BY UPPER(TRIM(country_code)), UPPER(TRIM(year))
),
gc_by_name AS (
  SELECT
    UPPER(TRIM(country_name)) AS country_name_key,
    UPPER(TRIM(year)) AS year_key,
    MIN(group_code) AS group_code,
    MIN(country_grouping) AS country_grouping,
    MIN(country_name) AS country_name,
    MIN(region) AS region
  FROM group_country_rows
  WHERE upload_run_id = (SELECT id FROM latest_group_country)
  GROUP BY UPPER(TRIM(country_name)), UPPER(TRIM(year))
),
tma_agg AS (
  SELECT
    TRIM(t.year) AS year,
    TRIM(t.end_country_code) AS end_country_code,
    TRIM(t.end_country) AS country_raw,
    TRIM(t.geographical_region) AS region_raw,
    TRIM(t.machine_line_code) AS machine_line_code,
    TRIM(t.machine_line) AS machine_line_name,
    TRIM(t.size_class_mapping) AS size_class,
    SUM(CAST(REPLACE(NULLIF(TRIM(t.total_market_fid_sales), ''), ',', '') AS REAL)) AS fid,
    'TMA' AS source
  FROM tma_data_rows t
  WHERE t.upload_run_id = (SELECT id FROM latest_tma)
  GROUP BY
    TRIM(t.year),
    TRIM(t.end_country_code),
    TRIM(t.end_country),
    TRIM(t.geographical_region),
    TRIM(t.machine_line_code),
    TRIM(t.machine_line),
    TRIM(t.size_class_mapping)
),
volvo_agg AS (
  SELECT
    TRIM(v.calendar) AS year,
    TRIM(v.country) AS end_country_code,
    TRIM(v.country) AS country_raw,
    TRIM(v.region) AS region_raw,
    TRIM(v.machine) AS machine_line_code,
    TRIM(v.machine_line) AS machine_line_name,
    TRIM(v.size_class) AS size_class,
    SUM(CAST(REPLACE(NULLIF(TRIM(v.fid), ''), ',', '') AS REAL)) AS fid,
    COALESCE(NULLIF(TRIM(v.source), ''), 'SAL') AS source
  FROM volvo_sale_data_rows v
  WHERE v.upload_run_id = (SELECT id FROM latest_volvo)
  GROUP BY
    TRIM(v.calendar),
    TRIM(v.country),
    TRIM(v.region),
    TRIM(v.machine),
    TRIM(v.machine_line),
    TRIM(v.size_class),
    COALESCE(NULLIF(TRIM(v.source), ''), 'SAL')
),
all_agg AS (
  SELECT * FROM tma_agg
  UNION ALL
  SELECT * FROM volvo_agg
),
source_matrix_country_artificial_lines AS (
  SELECT
    UPPER(TRIM(country_name)) AS country_name_key,
    UPPER(TRIM(artificial_machine_line)) AS artificial_machine_line_key,
    MAX(
      CASE
        WHEN TRIM(COALESCE(crp_source, '')) <> '' THEN TRIM(crp_source)
        ELSE NULL
      END
    ) AS crp_source
  FROM source_matrix_rows
  WHERE upload_run_id = (SELECT id FROM latest_source_matrix)
    AND TRIM(COALESCE(country_name, '')) <> ''
    AND TRIM(COALESCE(artificial_machine_line, '')) <> ''
  GROUP BY
    UPPER(TRIM(country_name)),
    UPPER(TRIM(artificial_machine_line))
),
reporter_list_artificial_brand AS (
  SELECT
    UPPER(TRIM(source_code)) AS source_code_key,
    UPPER(TRIM(artificial_machine_line)) AS artificial_machine_line_key,
    UPPER(TRIM(brand_code)) AS brand_code_key
  FROM reporter_list_rows
  WHERE upload_run_id = (SELECT id FROM upload_runs WHERE matrix_type = 'reporter_list' AND status = 'success' ORDER BY uploaded_at DESC, id DESC LIMIT 1)
    AND TRIM(COALESCE(source_code, '')) <> ''
    AND TRIM(COALESCE(artificial_machine_line, '')) <> ''
    AND TRIM(COALESCE(brand_code, '')) <> ''
  GROUP BY
    UPPER(TRIM(source_code)),
    UPPER(TRIM(artificial_machine_line)),
    UPPER(TRIM(brand_code))
),
final_rows_base AS (
  SELECT
    ROW_NUMBER() OVER (ORDER BY a.year, a.machine_line_code, a.machine_line_name, a.size_class, a.source) AS base_row_id,
    a.year,
    COALESCE(g_code.group_code, g_name.group_code, '') AS country_group_code,
    COALESCE(g_code.country_grouping, g_name.country_grouping, '') AS country_grouping,
    COALESCE(g_code.country_name, g_name.country_name, a.country_raw) AS country,
    COALESCE(g_code.region, g_name.region, a.region_raw) AS region,
    a.machine_line_code,
    a.machine_line_name,
    a.size_class,
    CASE WHEN UPPER(TRIM(a.source)) = 'SAL' THEN 'VCE' ELSE '#' END AS brand_code,
    '#' AS pri_sec,
    a.source,
    a.fid
  FROM all_agg a
  LEFT JOIN gc_by_code g_code
    ON UPPER(TRIM(a.end_country_code)) = g_code.country_code_key
   AND UPPER(TRIM(a.year)) = g_code.year_key
  LEFT JOIN gc_by_name g_name
    ON UPPER(TRIM(a.country_raw)) = g_name.country_name_key
   AND UPPER(TRIM(a.year)) = g_name.year_key
),
machine_line_mapping_matches AS (
  SELECT
    frb.base_row_id,
    TRIM(mlm.artificial_machine_line) AS artificial_machine_line,
    ROW_NUMBER() OVER (
      PARTITION BY frb.base_row_id
      ORDER BY mlm.row_index ASC, mlm.id ASC
    ) AS match_rank
  FROM final_rows_base frb
  JOIN machine_line_mapping_rows mlm
    ON mlm.upload_run_id = (SELECT id FROM latest_machine_line_mapping)
   AND UPPER(TRIM(COALESCE(mlm.size_class, ''))) = UPPER(TRIM(COALESCE(frb.size_class, '')))
   AND (
        UPPER(TRIM(COALESCE(frb.machine_line_name, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_name, '')))
     OR UPPER(TRIM(COALESCE(frb.machine_line_code, ''))) = UPPER(TRIM(COALESCE(mlm.machine_line_code, '')))
   )
),
final_rows AS (
  SELECT
    frb.year AS "Year",
    frb.country_group_code AS "Country Group Code",
    frb.country_grouping AS "Country Grouping",
    frb.country AS "Country",
    frb.region AS "Region",
    frb.machine_line_code AS "Machine Line Code",
    frb.machine_line_name AS "Machine Line name",
    CASE
      WHEN UPPER(TRIM(COALESCE(frb.source, ''))) = 'SAL'
       AND UPPER(TRIM(COALESCE(frb.size_class, ''))) = 'MINI' THEN '<6T'
      WHEN UPPER(TRIM(COALESCE(frb.source, ''))) = 'SAL'
       AND UPPER(TRIM(COALESCE(frb.size_class, ''))) = 'MIDI' THEN '6<10T'
      ELSE frb.size_class
    END AS "Size Class",
    COALESCE(mlmm.artificial_machine_line, '') AS "Artificial machine line",
    frb.brand_code AS "Brand Code",
    CASE
      WHEN UPPER(TRIM(frb.source)) = 'TMA' THEN '#'
      WHEN UPPER(TRIM(frb.source)) = 'SAL'
           AND TRIM(COALESCE(sm_artificial.crp_source, '')) <> ''
           AND rl_artificial.source_code_key IS NOT NULL THEN 'Y'
      ELSE ''
    END AS "Reporter Flag",
    frb.pri_sec AS "Pri/Sec",
    frb.source AS "Source",
    CASE
      WHEN UPPER(TRIM(frb.source)) = 'SAL'
           AND TRIM(CAST(frb.machine_line_code AS TEXT)) = '390' THEN 'Y'
      WHEN UPPER(TRIM(frb.source)) = 'SAL'
           AND TRIM(COALESCE(mlmm.artificial_machine_line, '')) <> ''
           AND sm_artificial.country_name_key IS NULL THEN 'Y'
      ELSE ''
    END AS "Deletion flag",
    frb.fid AS "fid"
  FROM final_rows_base frb
  LEFT JOIN machine_line_mapping_matches mlmm
    ON frb.base_row_id = mlmm.base_row_id
   AND mlmm.match_rank = 1
  LEFT JOIN source_matrix_country_artificial_lines sm_artificial
    ON UPPER(TRIM(COALESCE(frb.country, ''))) = sm_artificial.country_name_key
   AND UPPER(TRIM(COALESCE(mlmm.artificial_machine_line, ''))) = sm_artificial.artificial_machine_line_key
  LEFT JOIN reporter_list_artificial_brand rl_artificial
    ON UPPER(TRIM(COALESCE(sm_artificial.crp_source, ''))) = rl_artificial.source_code_key
   AND UPPER(TRIM(COALESCE(mlmm.artificial_machine_line, ''))) = rl_artificial.artificial_machine_line_key
   AND UPPER(TRIM(COALESCE(frb.brand_code, ''))) = rl_artificial.brand_code_key
)
SELECT *
FROM final_rows
ORDER BY
  "Country Grouping",
  "Country Group Code",
  "Country",
  "Machine Line Code",
  "Machine Line name",
  "Size Class";`;

const CRP_D1_RULE_BULLETS = [
  "Deletion flag is only evaluated for SAL records.",
  "Deletion flag = Y when Source = SAL and Machine Line Code = 390.",
  "Deletion flag = Y when Source = SAL and Country + Artificial machine line is not found in latest Source Matrix.",
  "Reporter Flag = # for TMA records; for SAL records it is Y only when Source Matrix CRP Source plus Artificial machine line plus brand_code matches Reporter List, otherwise it stays blank.",
  "All SAL rows are displayed in P00; SAL rows with empty CRP Source are not filtered out from this report.",
  "Brand Code = VCE for SAL records, Brand Code = # for TMA records.",
  "Artificial machine line is matched from Machine Line Mapping by machine line + size class + position.",
  "For SAL rows, Size Class is normalized to the TMA bucket when needed; currently this is used for Compact Excavators (CEX), where Mini maps to <6T and Midi maps to 6<10T.",
  "SAL can include machine lines that do not exist in TMA, so VCE FID can be lower than the total SAL value.",
  "Country mapping first uses country code + year, then falls back to country name + year.",
];

const OTH_DELETION_FLAG_SQL = `WITH source_matrix_base AS (
  SELECT
    UPPER(TRIM(country_name)) AS country_name_key,
    UPPER(TRIM(artificial_machine_line)) AS artificial_machine_line_key,
    UPPER(TRIM(primary_source)) AS primary_source_key,
    UPPER(TRIM(secondary_source)) AS secondary_source_key
  FROM source_matrix_rows
  WHERE upload_run_id = :latest_source_matrix_upload_run_id
    AND TRIM(COALESCE(country_name, '')) <> ''
    AND TRIM(COALESCE(artificial_machine_line, '')) <> ''
),
source_matrix_keys AS (
  SELECT country_name_key, artificial_machine_line_key
  FROM source_matrix_base
  GROUP BY country_name_key, artificial_machine_line_key
),
source_matrix_source_flags AS (
  SELECT country_name_key, artificial_machine_line_key, primary_source_key AS source_key, 'P' AS pri_sec
  FROM source_matrix_base
  WHERE TRIM(COALESCE(primary_source_key, '')) <> ''
  UNION ALL
  SELECT country_name_key, artificial_machine_line_key, secondary_source_key AS source_key, 'S' AS pri_sec
  FROM source_matrix_base
  WHERE TRIM(COALESCE(secondary_source_key, '')) <> ''
),
source_matrix_source_flags_dedup AS (
  SELECT
    country_name_key,
    artificial_machine_line_key,
    source_key,
    CASE
      WHEN SUM(CASE WHEN pri_sec = 'P' THEN 1 ELSE 0 END) > 0 THEN 'P'
      WHEN SUM(CASE WHEN pri_sec = 'S' THEN 1 ELSE 0 END) > 0 THEN 'S'
      ELSE ''
    END AS pri_sec
  FROM source_matrix_source_flags
  GROUP BY country_name_key, artificial_machine_line_key, source_key
)
SELECT
  o.year AS year,
  o.source AS source,
  o.country AS country_code,
  COALESCE(g.country_name, o.country) AS country,
  COALESCE(m.machine_line_name, o.machine_line) AS machine_line_name,
  m.machine_line_code AS machine_line_code,
  COALESCE(b.brand_code, '') AS brand_code,
  CASE
    WHEN TRIM(COALESCE(m.machine_line_code, '')) = '390' THEN 'Y'
    WHEN TRIM(COALESCE(g.country_name, o.country, '')) <> ''
      AND TRIM(COALESCE(m.machine_line_name, o.machine_line, '')) <> ''
      AND smk.country_name_key IS NULL THEN 'Y'
    ELSE ''
  END AS deletion_flag,
  COALESCE(smsf.pri_sec, '') AS pri_sec,
  CASE
    WHEN EXISTS (
      SELECT 1
      FROM source_matrix_rows sm
      JOIN reporter_list_rows rl
        ON UPPER(TRIM(COALESCE(rl.source_code, ''))) = UPPER(TRIM(COALESCE(sm.crp_source, '')))
        AND UPPER(TRIM(COALESCE(rl.artificial_machine_line, ''))) = UPPER(TRIM(COALESCE(m.artificial_machine_line, '')))
        AND UPPER(TRIM(COALESCE(rl.brand_code, ''))) = UPPER(TRIM(COALESCE(b.brand_code, '')))
        AND rl.upload_run_id = :latest_reporter_list_upload_run_id
      WHERE sm.upload_run_id = :latest_source_matrix_upload_run_id
        AND UPPER(TRIM(COALESCE(sm.country_name, ''))) = UPPER(TRIM(COALESCE(g.country_name, o.country, '')))
        AND UPPER(TRIM(COALESCE(sm.artificial_machine_line, ''))) = UPPER(TRIM(COALESCE(m.artificial_machine_line, '')))
        AND TRIM(COALESCE(sm.crp_source, '')) <> ''
    ) THEN 'Y'
    ELSE ''
  END AS reporter_flag
FROM oth_data_rows o
LEFT JOIN group_country_rows g
  ON UPPER(TRIM(o.country)) = UPPER(TRIM(g.country_code))
 AND UPPER(TRIM(o.year)) = UPPER(TRIM(g.year))
LEFT JOIN machine_line_mapping_rows m
  ON (
      UPPER(TRIM(o.machine_line)) = UPPER(TRIM(m.machine_line_name))
      OR UPPER(TRIM(o.machine_line)) = UPPER(TRIM(m.machine_line_code))
  )
LEFT JOIN brand_mapping_rows b
  ON UPPER(TRIM(o.brand_name)) = UPPER(TRIM(b.brand_name))
LEFT JOIN source_matrix_keys smk
  ON UPPER(TRIM(COALESCE(g.country_name, o.country))) = smk.country_name_key
 AND UPPER(TRIM(COALESCE(m.artificial_machine_line, ''))) = smk.artificial_machine_line_key
LEFT JOIN source_matrix_source_flags_dedup smsf
  ON UPPER(TRIM(COALESCE(g.country_name, o.country))) = smsf.country_name_key
 AND UPPER(TRIM(COALESCE(m.artificial_machine_line, ''))) = smsf.artificial_machine_line_key
 AND UPPER(TRIM(COALESCE(o.source, ''))) = smsf.source_key
WHERE o.upload_run_id = :latest_oth_upload_run_id
ORDER BY o.row_index ASC;`;

const OTH_RULE_BULLETS = [
  "2.1 Deletion flag: set Y when Machine Line Code = 390, or when Country + Artificial machine line is missing in Source Matrix.",
  "2.2 Pri/Sec: match Source + Country + Artificial machine line to Source Matrix. Match primary_source -> P, match secondary_source -> S, no match -> blank.",
  "2.3 Reporter flag: get CRP Source from Source Matrix by Country + Artificial machine line, then match Reporter List by source_code + Artificial machine line + brand_code. If matched, set Y.",
];

const OTH_SQL_MAP_BULLETS = [
  "`source_matrix_keys`: checks whether Country + Artificial machine line exists in Source Matrix (for 2.1).",
  "`source_matrix_source_flags_dedup`: resolves Pri/Sec by Source + Country + Artificial machine line (for 2.2).",
  "`EXISTS` join with `source_matrix_rows` + `reporter_list_rows`: resolves Reporter flag (for 2.3).",
];

const OTH_KEY_SQL_SNIPPETS: Array<{ title: string; explain: string; sql: string }> = [
  {
    title: "2.2 Pri/Sec Rule",
    explain: "Pri/Sec is assigned by matching Source on top of Country + Artificial machine line.",
    sql: `COALESCE(smsf.pri_sec, '') AS pri_sec
...
LEFT JOIN source_matrix_source_flags_dedup smsf
  ON UPPER(TRIM(COALESCE(g.country_name, o.country))) = smsf.country_name_key
 AND UPPER(TRIM(COALESCE(m.artificial_machine_line, ''))) = smsf.artificial_machine_line_key
 AND UPPER(TRIM(COALESCE(o.source, ''))) = smsf.source_key`,
  },
  {
    title: "2.3 Reporter Flag Rule",
    explain: "Reporter flag becomes Y only when Source Matrix CRP Source links to Reporter List for the same Artificial machine line + brand.",
    sql: `CASE
  WHEN EXISTS (
    SELECT 1
    FROM source_matrix_rows sm
    JOIN reporter_list_rows rl
      ON UPPER(TRIM(COALESCE(rl.source_code, ''))) = UPPER(TRIM(COALESCE(sm.crp_source, '')))
     AND UPPER(TRIM(COALESCE(rl.artificial_machine_line, ''))) = UPPER(TRIM(COALESCE(m.artificial_machine_line, '')))
     AND UPPER(TRIM(COALESCE(rl.brand_code, ''))) = UPPER(TRIM(COALESCE(b.brand_code, '')))
    WHERE ...
  ) THEN 'Y'
  ELSE ''
END AS reporter_flag`,
  },
];

const CRP_D1_SQL_MAP_BULLETS = [
  "`gc_by_code` and `gc_by_name`: build country lookup tables from Group Country upload.",
  "`tma_agg` and `volvo_agg`: normalize + aggregate TMA and SAL rows into the same shape.",
  "`source_matrix_country_artificial_lines`: resolves CRP Source availability and validates Country + Artificial machine line against the latest Source Matrix.",
  "`reporter_list_artificial_brand`: stores Reporter List keys by source_code + Artificial machine line + brand_code.",
  "`machine_line_mapping_matches`: matches Artificial machine line from Machine Line Mapping using machine line + size class + position.",
  "`all_agg`, `final_rows_base`, `final_rows`, and `display_rows`: union TMA + SAL, apply business flags, match artificial machine line, and merge TMA Mini/Midi rows for display.",
];

const CRP_D1_KEY_SQL_SNIPPETS: Array<{ title: string; explain: string; sql: string }> = [
  {
    title: "Deletion Flag Rule",
    explain:
      "Only SAL rows can be marked for deletion: code 390, or Country + Artificial machine line missing from Source Matrix.",
    sql: `CASE
  WHEN UPPER(TRIM(frb.source)) = 'SAL'
       AND TRIM(CAST(frb.machine_line_code AS TEXT)) = '390' THEN 'Y'
  WHEN UPPER(TRIM(frb.source)) = 'SAL'
       AND TRIM(COALESCE(mlmm.artificial_machine_line, '')) <> ''
       AND sm_artificial.country_name_key IS NULL THEN 'Y'
  ELSE ''
END AS "Deletion flag"`,
  },
  {
    title: "Reporter + Brand Rules",
    explain: "Brand depends on source type. Reporter Flag is # for TMA, Y only for SAL rows whose Source Matrix CRP Source plus Artificial machine line plus brand_code matches Reporter List, and blank for the remaining SAL rows.",
    sql: `CASE
  WHEN UPPER(TRIM(frb.source)) = 'SAL' THEN 'VCE'
  ELSE '#'
END AS "Brand Code",
CASE
  WHEN UPPER(TRIM(frb.source)) = 'TMA' THEN '#'
  WHEN UPPER(TRIM(frb.source)) = 'SAL'
       AND TRIM(COALESCE(sm_artificial.crp_source, '')) <> ''
       AND rl_artificial.source_code_key IS NOT NULL THEN 'Y'
  ELSE ''
END AS "Reporter Flag"`,
  },
  {
    title: "Country Mapping Priority",
    explain: "Country info prefers country_code + year match, then falls back to country_name + year.",
    sql: `COALESCE(g_code.group_code, g_name.group_code, '') AS country_group_code,
COALESCE(g_code.country_grouping, g_name.country_grouping, '') AS country_grouping,
COALESCE(g_code.country_name, g_name.country_name, a.country_raw) AS country,
COALESCE(g_code.region, g_name.region, a.region_raw) AS region

LEFT JOIN gc_by_code g_code
  ON UPPER(TRIM(a.end_country_code)) = g_code.country_code_key
 AND UPPER(TRIM(a.year)) = g_code.year_key
LEFT JOIN gc_by_name g_name
  ON UPPER(TRIM(a.country_raw)) = g_name.country_name_key
 AND UPPER(TRIM(a.year)) = g_name.year_key`,
  },
];

const P10_RULE_BULLETS = [
  "Total Market (TMA): sum of rows where Source = TMA.",
  "Rows with Volvo Deletion Flag = Y are excluded from the P10 report output.",
  "VCE: sum of Volvo/SAL rows where Source Matrix CRP Source plus Artificial machine line plus brand_code matches Reporter List, excluding Motor Graders.",
  "Non-VCE: max(TMA - VCE, 0).",
];

const A10_RULE_BULLETS = [
  "A10 currently does not run split or re-split logic yet; it summarizes the prepared SAL and TMA rows.",
  "A10 shows one SAL detail row, one TMA detail row, and one derived Result row for each matched Year + Country Group + Country + Region + Machine Line + Size Class combination.",
  "This output includes all Volvo rows with Reporter Flag = Y, together with the matched TMA result for the same group when TMA exists.",
  "For SAL rows, Size Class is normalized when needed: Mini maps to <6T and Midi maps to 6<10T. TMA Size Class stays unchanged.",
  "Only Volvo/SAL rows whose Source Matrix CRP Source plus Artificial machine line plus brand_code matches Reporter List can contribute to VCE-related values.",
  "SAL rows with Volvo Deletion Flag = Y do not contribute to the Result FID.",
  "Result FID = sum of valid Volvo/SAL rows for the group.",
  "Result TM FID = sum of TMA rows for the same group.",
  "Result TM Non VCE = max(TM FID - FID, 0).",
];

const THREE_CHECK_RULE_BULLETS = [
  "Check Report combines prepared TMA + SAL rows from P00 with OTH rows from OTH Deletion Flag Report into one review table.",
  "Source is not merged: TMA, SAL, and OTH stay as separate rows in the final output.",
  "TM, VCE FID, and TM Non VCE are matched by Year + Country + Machine Line Name + Size Class.",
  "For TMA and SAL rows, those three values come directly from the prepared P00 combined result.",
  "For OTH rows, Size Class is first normalized with Machine Line Mapping; when OTH maps to Mini or Midi, that normalized value is used for the TM/VCE lookup.",
  "TM = matched TMA value for the group.",
  "VCE FID = matched Volvo/SAL value for the group.",
  "TM Non VCE = max(TM - VCE FID, 0).",
  "Artificial machine line is carried from the source row: P00 Combined for TMA/SAL rows and OTH mapping for OTH rows.",
];

const THREE_CHECK_SQL_MAP_BULLETS = [
  "`_get_crp_d1_combined_report_data(include_all_sal=True)`: provides prepared TMA + SAL rows with TM / VCE FID / TM Non VCE already calculated.",
  "`get_oth_deletion_flag_report()`: provides OTH rows with mapped country, machine line, artificial machine line, brand, and control flags.",
  "`machine_line_mapping_rows`: normalizes OTH Size Class to Mini/Midi when needed before matching stats.",
  "`stats_by_group`: stores TM / VCE FID / TM Non VCE by Year + Country + Machine Line Name + Size Class.",
  "`result_rows`: appends TMA, SAL, and OTH into one final table without merging different sources together.",
];

const THREE_CHECK_KEY_SQL_SNIPPETS: Array<{ title: string; explain: string; sql: string }> = [
  {
    title: "Current Implementation Note",
    explain:
      "3 Check Report is currently assembled in backend Python from the prepared report outputs. It is not a single standalone SQL statement yet.",
    sql: `combined = _get_crp_d1_combined_report_data(include_all_sal=True)
oth = get_oth_deletion_flag_report()`,
  },
  {
    title: "TM / VCE / TM Non VCE Match Key",
    explain:
      "TMA and SAL rows first build a stats dictionary keyed by Year + Country + Machine Line Name + Size Class. OTH rows then read TM / VCE FID / TM Non VCE from that same key.",
    sql: `group_key = (
    _to_text(row.get("year")),
    _to_text(row.get("country")),
    _to_text(row.get("machine_line_name")),
    _to_text(row.get("size_class")),
)
stats_by_group[group_key] = {
    "tm": to_number(row.get("tm")),
    "vce_fid": to_number(row.get("vce_fid")),
    "tm_non_vce": to_number(row.get("tm_non_vce")),
}`,
  },
  {
    title: "OTH Size-Class Normalization",
    explain:
      "Before an OTH row looks up TM / VCE stats, its Size Class can be remapped to Mini or Midi from Machine Line Mapping so it aligns with the prepared TMA/SAL groups.",
    sql: `comparison_size_class = resolve_oth_size_class(
    row.get("machine_line_name"),
    row.get("machine_line_code"),
    row.get("size_class_flag"),
)
group_key = (
    _to_text(row.get("year")),
    _to_text(row.get("country")),
    _to_text(row.get("machine_line_name")),
    comparison_size_class,
)`,
  },
];

const REPORT_TABLE_MAX_HEIGHT = "72vh";
const SPLIT_MANUAL_NON_EDITABLE_COLUMNS: string[] = [
  "row_type",
  "year",
  "country_grouping",
  "country",
  "region",
  "machine_line",
  "artificial_machine_line",
  "brand_code",
  "reporter_flag",
  "source",
  "pri_sec",
  "size_class",
  "before_split_fid_lt_10t",
  "copy_fid_lt_10t",
  "after_split_fid_lt_6t",
  "after_split_fid_6_10t",
  "after_split_fid_target_three",
  "tm_non_vce_lt_6t",
  "tm_non_vce_6_10t",
  "tm_non_vce_target_three",
  "before_after_difference",
  "reference_level",
  "split_ratio",
];

function getSplitManualMatrixType(caseType: SplitDetailCaseType): string | null {
  if (caseType === "CEX") {
    return "split_manual_cex";
  }
  if (caseType === "GEC") {
    return "split_manual_gec";
  }
  if (caseType === "GEW") {
    return "split_manual_gew";
  }
  if (caseType === "WLO_GT10") {
    return "split_manual_wlo_gt10";
  }
  if (caseType === "WLO_LT10") {
    return "split_manual_wlo_lt10";
  }
  return null;
}

function normalizeSplitManualRows(rows: UploadRow[]): ExcavatorsSplitDetailRow[] {
  return rows.map((row) => ({
    row_type: String(row.row_type ?? ""),
    year: String(row.year ?? ""),
    country_grouping: String(row.country_grouping ?? ""),
    country: String(row.country ?? ""),
    region: String(row.region ?? ""),
    machine_line: String(row.machine_line ?? ""),
    artificial_machine_line: String(row.artificial_machine_line ?? ""),
    brand_code: String(row.brand_code ?? ""),
    reporter_flag: String(row.reporter_flag ?? ""),
    source: String(row.source ?? ""),
    pri_sec: String(row.pri_sec ?? ""),
    size_class: String(row.size_class ?? ""),
    before_split_fid_lt_10t: String(row.before_split_fid_lt_10t ?? "") === "" ? "" : toNumberValue(row.before_split_fid_lt_10t),
    copy_fid_lt_10t: String(row.copy_fid_lt_10t ?? "") === "" ? "" : toNumberValue(row.copy_fid_lt_10t),
    after_split_fid_lt_6t: String(row.after_split_fid_lt_6t ?? "") === "" ? "" : toNumberValue(row.after_split_fid_lt_6t),
    after_split_fid_6_10t: String(row.after_split_fid_6_10t ?? "") === "" ? "" : toNumberValue(row.after_split_fid_6_10t),
    after_split_fid_target_three:
      String(row.after_split_fid_target_three ?? "") === "" ? "" : toNumberValue(row.after_split_fid_target_three),
    tm_non_vce_lt_6t: String(row.tm_non_vce_lt_6t ?? "") === "" ? "" : toNumberValue(row.tm_non_vce_lt_6t),
    tm_non_vce_6_10t: String(row.tm_non_vce_6_10t ?? "") === "" ? "" : toNumberValue(row.tm_non_vce_6_10t),
    tm_non_vce_target_three:
      String(row.tm_non_vce_target_three ?? "") === "" ? "" : toNumberValue(row.tm_non_vce_target_three),
    resplit: String(row.resplit ?? ""),
    after_resplit_fid_lt_6t:
      String(row.after_resplit_fid_lt_6t ?? "") === "" ? "" : toNumberValue(row.after_resplit_fid_lt_6t),
    after_resplit_fid_6_10t:
      String(row.after_resplit_fid_6_10t ?? "") === "" ? "" : toNumberValue(row.after_resplit_fid_6_10t),
    after_resplit_fid_target_three:
      String(row.after_resplit_fid_target_three ?? "") === "" ? "" : toNumberValue(row.after_resplit_fid_target_three),
    before_after_difference:
      String(row.before_after_difference ?? "") === "" ? "" : toNumberValue(row.before_after_difference),
    reference_level: String(row.reference_level ?? ""),
    split_ratio: String(row.split_ratio ?? ""),
  }));
}

function LayerDetailPage() {
  const params = useParams();
  const location = useLocation();
  const layerCode = (params.layerCode ?? "").toUpperCase();
  const layer = LAYER_DETAILS[layerCode];
  const [runningCombinedReport, setRunningCombinedReport] = useState(false);
  const [combinedReportMessage, setCombinedReportMessage] = useState("");
  const [combinedReportError, setCombinedReportError] = useState("");
  const [combinedReportRows, setCombinedReportRows] = useState<CrpD1CombinedReportRow[]>([]);
  const [combinedReportResetToken, setCombinedReportResetToken] = useState(0);
  const [runningOthDeletionFlagReport, setRunningOthDeletionFlagReport] = useState(false);
  const [othDeletionFlagStartedAt, setOthDeletionFlagStartedAt] = useState<number | null>(null);
  const [othDeletionFlagElapsedSeconds, setOthDeletionFlagElapsedSeconds] = useState(0);
  const [othDeletionFlagMessage, setOthDeletionFlagMessage] = useState("");
  const [othDeletionFlagError, setOthDeletionFlagError] = useState("");
  const [othDeletionFlagRows, setOthDeletionFlagRows] = useState<OthDeletionFlagRow[]>([]);
  const [othDeletionFlagResetToken, setOthDeletionFlagResetToken] = useState(0);
  const [runningThreeCheckReport, setRunningThreeCheckReport] = useState(false);
  const [threeCheckMessage, setThreeCheckMessage] = useState("");
  const [threeCheckError, setThreeCheckError] = useState("");
  const [threeCheckRows, setThreeCheckRows] = useState<P00ThreeCheckRow[]>([]);
  const [threeCheckResetToken, setThreeCheckResetToken] = useState(0);
  const [runningP10Report, setRunningP10Report] = useState(false);
  const [p10Message, setP10Message] = useState("");
  const [p10Error, setP10Error] = useState("");
  const [p10Rows, setP10Rows] = useState<P10VceNonVceRow[]>([]);
  const [p10FilteredRows, setP10FilteredRows] = useState<P10VceNonVceRow[] | null>(null);
  const [p10Summary, setP10Summary] = useState({
    total_market_sum: 0,
    vce_sum: 0,
    non_vce_sum: 0,
  });
  const [p10ResetToken, setP10ResetToken] = useState(0);
  const [runningA10Report, setRunningA10Report] = useState(false);
  const [a10Message, setA10Message] = useState("");
  const [a10Error, setA10Error] = useState("");
  const [a10Rows, setA10Rows] = useState<A10AdjustmentRow[]>([]);
  const [a10ResetToken, setA10ResetToken] = useState(0);
  const [showSqlGuide, setShowSqlGuide] = useState(false);
  const [showOthSqlGuide, setShowOthSqlGuide] = useState(false);
  const [showThreeCheckSqlGuide, setShowThreeCheckSqlGuide] = useState(false);
  const [runningExcavatorsSplitCase, setRunningExcavatorsSplitCase] = useState(false);
  const [excavatorsSplitCaseMessage, setExcavatorsSplitCaseMessage] = useState("");
  const [excavatorsSplitCaseError, setExcavatorsSplitCaseError] = useState("");
  const [excavatorsSplitCaseRows, setExcavatorsSplitCaseRows] = useState<ExcavatorsSplitCaseRow[]>([]);
  const [excavatorsSplitDetailRows, setExcavatorsSplitDetailRows] = useState<ExcavatorsSplitDetailRow[]>([]);
  const [excavatorsSplitCaseResetToken, setExcavatorsSplitCaseResetToken] = useState(0);
  const [showExcavatorsSplitCasePanel, setShowExcavatorsSplitCasePanel] = useState(false);
  const [activeExcavatorsSplitCase, setActiveExcavatorsSplitCase] =
    useState<ExcavatorsSplitCaseType>("ALL");
  const [excavatorsResplitReadyByCase, setExcavatorsResplitReadyByCase] = useState<
    Record<ExcavatorsSplitCaseType, boolean>
  >({
    ALL: false,
    CEX: false,
    GEC: false,
    GEW: false,
  });
  const [editingExcavatorsManual, setEditingExcavatorsManual] = useState(false);
  const [excavatorsManualRows, setExcavatorsManualRows] = useState<ExcavatorsSplitDetailRow[]>([]);
  const [savingExcavatorsManual, setSavingExcavatorsManual] = useState(false);
  const [excavatorsManualMessage, setExcavatorsManualMessage] = useState("");
  const [excavatorsManualError, setExcavatorsManualError] = useState("");
  const [runningWheelLoadersSplitCase, setRunningWheelLoadersSplitCase] = useState(false);
  const [wheelLoadersSplitMessage, setWheelLoadersSplitMessage] = useState("");
  const [wheelLoadersSplitError, setWheelLoadersSplitError] = useState("");
  const [wheelLoadersSplitCaseRows, setWheelLoadersSplitCaseRows] = useState<ExcavatorsSplitCaseRow[]>([]);
  const [wheelLoadersSplitDetailRows, setWheelLoadersSplitDetailRows] = useState<ExcavatorsSplitDetailRow[]>([]);
  const [wheelLoadersSplitCaseResetToken, setWheelLoadersSplitCaseResetToken] = useState(0);
  const [showWheelLoadersSplitCasePanel, setShowWheelLoadersSplitCasePanel] = useState(false);
  const [activeWheelLoadersSplitCase, setActiveWheelLoadersSplitCase] =
    useState<WheelLoadersSplitCaseType>("ALL");
  const [wheelResplitReadyByCase, setWheelResplitReadyByCase] = useState<
    Record<WheelLoadersSplitCaseType, boolean>
  >({
    ALL: false,
    WLO_GT10: false,
    WLO_LT10: false,
    WLO_LT12: false,
  });
  const [editingWheelManual, setEditingWheelManual] = useState(false);
  const [wheelManualRows, setWheelManualRows] = useState<ExcavatorsSplitDetailRow[]>([]);
  const [savingWheelManual, setSavingWheelManual] = useState(false);
  const [wheelManualMessage, setWheelManualMessage] = useState("");
  const [wheelManualError, setWheelManualError] = useState("");

  useEffect(() => {
    if (!runningOthDeletionFlagReport || othDeletionFlagStartedAt === null) {
      setOthDeletionFlagElapsedSeconds(0);
      return;
    }

    const tick = () => {
      setOthDeletionFlagElapsedSeconds(
        Math.max(0, Math.floor((Date.now() - othDeletionFlagStartedAt) / 1000))
      );
    };

    tick();
    const intervalId = window.setInterval(tick, 1000);
    return () => window.clearInterval(intervalId);
  }, [runningOthDeletionFlagReport, othDeletionFlagStartedAt]);
  const [autoRunHandled, setAutoRunHandled] = useState(false);

  const combinedReportColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "country_group_code", label: "Country Group Code" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "country", label: "Country" },
      { key: "region", label: "Region" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "machine_line_name", label: "Machine Line name" },
      { key: "size_class", label: "Size Class" },
      { key: "artificial_machine_line", label: "Artificial machine line" },
      { key: "brand_code", label: "Brand Code" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "source", label: "Source" },
      { key: "deletion_flag", label: "Deletion flag" },
      { key: "fid", label: "fid" },
      { key: "tm", label: "TM" },
      { key: "vce_fid", label: "VCE FID" },
      { key: "tm_non_vce", label: "TM Non VCE" },
    ],
    []
  );

  const p10Columns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "country_group_code", label: "Country Group Code" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "country", label: "Country" },
      { key: "region", label: "Region" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "machine_line_name", label: "Machine Line name" },
      { key: "artificial_machine_line", label: "Artificial machine line" },
      { key: "size_class", label: "Size Class" },
      { key: "total_market", label: "TMA (Total Market)" },
      { key: "vce", label: "Volvo CE (VCE)" },
      { key: "non_vce", label: "Non-Volvo CE" },
      { key: "vce_share_pct", label: "VCE / TMA (%)" },
    ],
    []
  );

  const a10Columns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "country_group_code", label: "Country Group Code" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "country", label: "Country" },
      { key: "region", label: "Region" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "machine_line_name", label: "Machine Line name" },
      { key: "artificial_machine_line", label: "Artificial machine line" },
      { key: "size_class", label: "Size Class" },
      { key: "brand_code", label: "Brand Code" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "vce_flag", label: "VCE Flag" },
      { key: "source", label: "Source" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "calculation_step", label: "Calculation Step" },
      { key: "fid", label: "FID" },
      { key: "tm_fid", label: "TM FID" },
      { key: "tm_non_vce", label: "TM Non VCE" },
    ],
    []
  );

  const othDeletionFlagColumns = useMemo(
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
      { key: "artificial_machine_line", label: "Artificial machine line" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "ms_percent", label: "MS (%)" },
      { key: "deletion_flag", label: "Deletion flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "reporter_flag", label: "Reporter flag" },
      { key: "fid", label: "FID" },
    ],
    []
  );

  const threeCheckColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "country", label: "Country" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "region", label: "Region" },
      { key: "machine_line_name", label: "Machine Line Name" },
      { key: "machine_line_code", label: "Machine Line Code" },
      { key: "artificial_machine_line", label: "Artificial machine line" },
      { key: "brand_name", label: "Brand Name" },
      { key: "brand_code", label: "Brand Code" },
      { key: "size_class", label: "Size Class" },
      { key: "source", label: "Source" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "deletion_flag", label: "Deletion Flag" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "fid", label: "FID" },
      { key: "tm", label: "TM" },
      { key: "vce_fid", label: "VCE FID" },
      { key: "tm_non_vce", label: "TM Non VCE" },
    ],
    []
  );

  const excavatorsSplitCaseColumns = useMemo(
    () => [
      { key: "year", label: "Year" },
      { key: "machine_line_name", label: "Machine Line" },
      { key: "machine_line_code", label: "Artificial machine line" },
      { key: "source", label: "Source" },
      { key: "size_class_flag", label: "Size Class Flag" },
      { key: "matched_rows", label: "Matched Rows", summarizable: true },
      { key: "gross_fid", label: "Gross FID", summarizable: true },
      { key: "volvo_deduction", label: "Volvo Deduction", summarizable: true },
      { key: "net_fid", label: "Net FID", summarizable: true },
    ],
    []
  );

  const excavatorsSplitDetailColumns = useMemo(() => {
    const detailConfig =
      getExcavatorsSplitDetailConfig(activeExcavatorsSplitCase) ??
      ({
        inputSizeKeys: ["<10T"],
        inputSizeLabel: "<10T",
        firstTargetKeys: ["<6T"],
        firstTargetLabel: "<6T",
        secondTargetKeys: ["6<10T"],
        secondTargetLabel: "6<10T",
        panelTitle: "CEX Split Detail",
        description:
          "Lists the CEX OTH rows to split for <10T together with the related CEX TMA rows.",
      } satisfies ExcavatorsSplitDetailConfig);

    return [
      { key: "row_type", label: "Row Type" },
      { key: "year", label: "Year" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "country", label: "Country" },
      { key: "region", label: "Region" },
      { key: "machine_line", label: "Machine Line" },
      { key: "artificial_machine_line", label: "Artificial machine line" },
      { key: "brand_code", label: "Brand Code" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "source", label: "Source" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "size_class", label: "Size Class" },
      {
        key: "before_split_fid_lt_10t",
        label: `Before Split FID ${detailConfig.inputSizeLabel}`,
        summarizable: true,
      },
      {
        key: "copy_fid_lt_10t",
        label: `Copy FID ${detailConfig.inputSizeLabel}`,
        summarizable: true,
      },
      {
        key: "after_split_fid_lt_6t",
        label: `After Split FID ${detailConfig.firstTargetLabel}`,
        summarizable: true,
      },
      {
        key: "after_split_fid_6_10t",
        label: `After Split FID ${detailConfig.secondTargetLabel}`,
        summarizable: true,
      },
      {
        key: "tm_non_vce_lt_6t",
        label: `TM Non-VCE ${detailConfig.firstTargetLabel}`,
        summarizable: true,
      },
      {
        key: "tm_non_vce_6_10t",
        label: `TM Non-VCE ${detailConfig.secondTargetLabel}`,
        summarizable: true,
      },
      { key: "resplit", label: "Resplit" },
      {
        key: "after_resplit_fid_lt_6t",
        label: `After Resplit FID ${detailConfig.firstTargetLabel}`,
        summarizable: true,
      },
      {
        key: "after_resplit_fid_6_10t",
        label: `After Resplit FID ${detailConfig.secondTargetLabel}`,
        summarizable: true,
      },
      { key: "reference_level", label: "Split Level" },
      { key: "split_ratio", label: "Split Ratio" },
      { key: "before_after_difference", label: "Before/After Difference", summarizable: true },
    ];
  }, [activeExcavatorsSplitCase]);

  const wheelLoadersSplitDetailColumns = useMemo(() => {
    const detailConfig =
      (activeWheelLoadersSplitCase === "WLO_LT10"
        ? getExcavatorsSplitDetailConfig("WLO_LT10")
        : activeWheelLoadersSplitCase === "WLO_LT12"
          ? getExcavatorsSplitDetailConfig("WLO_LT12")
        : getExcavatorsSplitDetailConfig("WLO_GT10")) ??
      ({
        inputSizeKeys: [">10"],
        inputSizeLabel: ">10",
        firstTargetKeys: ["10<12"],
        firstTargetLabel: "10<12",
        secondTargetKeys: [">12"],
        secondTargetLabel: ">12",
        panelTitle: "WLO Split Detail (>10)",
        description:
          "Lists the WLO OTH rows to split for >10 together with the related WLO TMA rows.",
      } satisfies ExcavatorsSplitDetailConfig);

    return [
      { key: "row_type", label: "Row Type" },
      { key: "year", label: "Year" },
      { key: "country_grouping", label: "Country Grouping" },
      { key: "country", label: "Country" },
      { key: "region", label: "Region" },
      { key: "machine_line", label: "Machine Line" },
      { key: "artificial_machine_line", label: "Artificial machine line" },
      { key: "brand_code", label: "Brand Code" },
      { key: "reporter_flag", label: "Reporter Flag" },
      { key: "source", label: "Source" },
      { key: "pri_sec", label: "Pri/Sec" },
      { key: "size_class", label: "Size Class" },
      {
        key: "before_split_fid_lt_10t",
        label: `Before Split FID ${detailConfig.inputSizeLabel}`,
        summarizable: true,
      },
      {
        key: "copy_fid_lt_10t",
        label: `Copy FID ${detailConfig.inputSizeLabel}`,
        summarizable: true,
      },
      {
        key: "after_split_fid_lt_6t",
        label: `After Split FID ${detailConfig.firstTargetLabel}`,
        summarizable: true,
      },
      {
        key: "after_split_fid_6_10t",
        label: `After Split FID ${detailConfig.secondTargetLabel}`,
        summarizable: true,
      },
      ...(detailConfig.thirdTargetLabel
        ? [{
            key: "after_split_fid_target_three",
            label: `After Split FID ${detailConfig.thirdTargetLabel}`,
            summarizable: true,
          }]
        : []),
      {
        key: "tm_non_vce_lt_6t",
        label: `TM Non-VCE ${detailConfig.firstTargetLabel}`,
        summarizable: true,
      },
      {
        key: "tm_non_vce_6_10t",
        label: `TM Non-VCE ${detailConfig.secondTargetLabel}`,
        summarizable: true,
      },
      ...(detailConfig.thirdTargetLabel
        ? [{
            key: "tm_non_vce_target_three",
            label: `TM Non-VCE ${detailConfig.thirdTargetLabel}`,
            summarizable: true,
          }]
        : []),
      { key: "resplit", label: "Resplit" },
      {
        key: "after_resplit_fid_lt_6t",
        label: `After Resplit FID ${detailConfig.firstTargetLabel}`,
        summarizable: true,
      },
      {
        key: "after_resplit_fid_6_10t",
        label: `After Resplit FID ${detailConfig.secondTargetLabel}`,
        summarizable: true,
      },
      ...(detailConfig.thirdTargetLabel
        ? [{
            key: "after_resplit_fid_target_three",
            label: `After Resplit FID ${detailConfig.thirdTargetLabel}`,
            summarizable: true,
          }]
        : []),
      { key: "reference_level", label: "Split Level" },
      { key: "split_ratio", label: "Split Ratio" },
      { key: "before_after_difference", label: "Before/After Difference", summarizable: true },
    ];
  }, [activeWheelLoadersSplitCase]);

  const p10Share = useMemo(() => {
    const rowsForShare = p10FilteredRows ?? p10Rows;
    const totalMarketRaw = rowsForShare.reduce((sum, row) => sum + Number(row.total_market || 0), 0);
    const vceRaw = rowsForShare.reduce((sum, row) => sum + Number(row.vce || 0), 0);
    const nonVceRaw = rowsForShare.reduce((sum, row) => sum + Number(row.non_vce || 0), 0);
    const totalMarket = Math.max(totalMarketRaw, 0);
    const safeVce = Math.max(Math.min(vceRaw, totalMarket), 0);
    const safeNonVce = Math.max(Math.min(nonVceRaw, totalMarket), 0);
    const vcePct = totalMarket > 0 ? (safeVce / totalMarket) * 100 : 0;
    const nonVcePct = totalMarket > 0 ? (safeNonVce / totalMarket) * 100 : 0;
    return {
      totalMarket,
      safeVce,
      safeNonVce,
      vcePct,
      nonVcePct,
    };
  }, [p10FilteredRows, p10Rows]);

  const excavatorsSplitCaseSummary = useMemo(() => {
    return excavatorsSplitCaseRows.reduce(
      (summary, row) => ({
        groupedRows: summary.groupedRows + 1,
        matchedRows: summary.matchedRows + row.matched_rows,
        grossFidTotal: summary.grossFidTotal + row.gross_fid,
        volvoDeductionTotal: summary.volvoDeductionTotal + row.volvo_deduction,
        netFidTotal: summary.netFidTotal + row.net_fid,
      }),
      {
        groupedRows: 0,
        matchedRows: 0,
        grossFidTotal: 0,
        volvoDeductionTotal: 0,
        netFidTotal: 0,
      }
    );
  }, [excavatorsSplitCaseRows]);

  const wheelLoadersSplitCaseSummary = useMemo(() => {
    return wheelLoadersSplitCaseRows.reduce(
      (summary, row) => ({
        groupedRows: summary.groupedRows + 1,
        matchedRows: summary.matchedRows + row.matched_rows,
        grossFidTotal: summary.grossFidTotal + row.gross_fid,
        volvoDeductionTotal: summary.volvoDeductionTotal + row.volvo_deduction,
        netFidTotal: summary.netFidTotal + row.net_fid,
      }),
      {
        groupedRows: 0,
        matchedRows: 0,
        grossFidTotal: 0,
        volvoDeductionTotal: 0,
        netFidTotal: 0,
      }
    );
  }, [wheelLoadersSplitCaseRows]);

  const activeExcavatorsSplitCaseDetail = EXCAVATORS_SPLIT_CASE_DETAILS[activeExcavatorsSplitCase];
  const activeWheelLoadersSplitCaseDetail = WHEEL_LOADERS_SPLIT_CASE_DETAILS[activeWheelLoadersSplitCase];
  const activeExcavatorsSplitDetailConfig = getExcavatorsSplitDetailConfig(activeExcavatorsSplitCase);
  const activeWheelLoadersSplitDetailConfig =
    activeWheelLoadersSplitCase === "WLO_GT10" ||
    activeWheelLoadersSplitCase === "WLO_LT10" ||
    activeWheelLoadersSplitCase === "WLO_LT12"
      ? getExcavatorsSplitDetailConfig(activeWheelLoadersSplitCase)
      : null;
  const showExcavatorsSplitDetail = activeExcavatorsSplitDetailConfig !== null;
  const showExcavatorsGroupedSummary = !showExcavatorsSplitDetail;
  const showWheelLoadersSplitDetail = activeWheelLoadersSplitDetailConfig !== null;
  const canEditExcavatorsManual =
    (activeExcavatorsSplitCase === "CEX" ||
      activeExcavatorsSplitCase === "GEC" ||
      activeExcavatorsSplitCase === "GEW") &&
    excavatorsResplitReadyByCase[activeExcavatorsSplitCase];
  const canEditWheelManual =
    (activeWheelLoadersSplitCase === "WLO_GT10" || activeWheelLoadersSplitCase === "WLO_LT10") &&
    wheelResplitReadyByCase[activeWheelLoadersSplitCase];

  const getLatestSizeClassRows = async (): Promise<UploadRow[]> => {
    try {
      const latestSizeClass = await getLatestUploadByMatrixType("size_class");
      return latestSizeClass.rows;
    } catch (error) {
      console.error(error);
      return [];
    }
  };

  const getLatestSourceMatrixRows = async (): Promise<UploadRow[]> => {
    try {
      const latestSourceMatrix = await getLatestUploadByMatrixType("source_matrix");
      return latestSourceMatrix.rows;
    } catch (error) {
      console.error(error);
      return [];
    }
  };

  const handleStartExcavatorsManualEdit = () => {
    if (!canEditExcavatorsManual || excavatorsSplitDetailRows.length === 0) {
      return;
    }
    setEditingExcavatorsManual(true);
    setExcavatorsManualRows(excavatorsSplitDetailRows.map((row) => ({ ...row })));
    setExcavatorsManualMessage("");
    setExcavatorsManualError("");
  };

  const handleCancelExcavatorsManualEdit = () => {
    setEditingExcavatorsManual(false);
    setExcavatorsManualRows([]);
    setExcavatorsManualMessage("");
    setExcavatorsManualError("");
  };

  const handleSaveExcavatorsManualEdit = async () => {
    if (!editingExcavatorsManual || excavatorsManualRows.length === 0) {
      setExcavatorsManualError("No edited rows to save.");
      return;
    }

    const matrixType = getSplitManualMatrixType(activeExcavatorsSplitCase);
    if (!matrixType) {
      setExcavatorsManualError("Unsupported split case for manual save.");
      return;
    }

    try {
      setSavingExcavatorsManual(true);
      setExcavatorsManualError("");
      setExcavatorsManualMessage("");

      const rowsToSave: UploadRow[] = excavatorsManualRows.map((row) => ({
        ...row,
        case_type: activeExcavatorsSplitCase,
      }));
      const saveResult = await saveEditedUpload(matrixType, rowsToSave);
      const latestResult = await getLatestUploadByMatrixType(matrixType);
      const latestRows = normalizeSplitManualRows(latestResult.rows);

      setExcavatorsSplitDetailRows(latestRows);
      setExcavatorsSplitCaseResetToken((prev) => prev + 1);
      setEditingExcavatorsManual(false);
      setExcavatorsManualRows([]);
      setExcavatorsManualMessage(`Saved. New Upload ID: ${saveResult.upload_run_id}.`);
    } catch (error) {
      console.error(error);
      setExcavatorsManualError(
        error instanceof Error ? error.message : "Failed to save manual edits."
      );
    } finally {
      setSavingExcavatorsManual(false);
    }
  };

  const handleApplyExcavatorsResplit = async () => {
    if (excavatorsSplitDetailRows.length === 0) {
      setExcavatorsSplitCaseMessage("No split detail rows available for re-split.");
      return;
    }

    if (
      activeExcavatorsSplitCase !== "CEX" &&
      activeExcavatorsSplitCase !== "GEC" &&
      activeExcavatorsSplitCase !== "GEW"
    ) {
      setExcavatorsSplitCaseMessage("Resplit is currently available for CEX, GEC, and GEW.");
      return;
    }

    const detailConfig = getExcavatorsSplitDetailConfig(activeExcavatorsSplitCase);
    if (!detailConfig) {
      setExcavatorsSplitCaseMessage("Split detail configuration is unavailable.");
      return;
    }

    try {
      setRunningExcavatorsSplitCase(true);
      setExcavatorsSplitCaseError("");
      setExcavatorsSplitCaseMessage(`Applying ${activeExcavatorsSplitCase} re-split logic...`);

      const [sourceMatrixRows, sizeClassRows] = await Promise.all([
        getLatestSourceMatrixRows(),
        getLatestSizeClassRows(),
      ]);

      const resplitRows = applyExcavatorsResplitByCrpSource(
        excavatorsSplitDetailRows,
        sourceMatrixRows,
        sizeClassRows,
        detailConfig
      );

      const flaggedRows = resplitRows.filter(
        (row) => typeof row.resplit === "string" && row.resplit.trim() !== ""
      ).length;

      setExcavatorsSplitDetailRows(resplitRows);
      setExcavatorsSplitCaseResetToken((prev) => prev + 1);
      setExcavatorsResplitReadyByCase((prev) => ({
        ...prev,
        [activeExcavatorsSplitCase]: true,
      }));
      setEditingExcavatorsManual(false);
      setExcavatorsManualRows([]);
      setExcavatorsManualMessage("");
      setExcavatorsManualError("");
      setExcavatorsSplitCaseMessage(
        flaggedRows > 0
          ? `${activeExcavatorsSplitCase} re-split applied. ${flaggedRows} row(s) require re-allocation.`
          : `${activeExcavatorsSplitCase} re-split applied. No rows required re-allocation.`
      );
    } catch (error) {
      console.error(error);
      setExcavatorsSplitCaseError(
        error instanceof Error ? error.message : "Failed to apply split re-split logic."
      );
    } finally {
      setRunningExcavatorsSplitCase(false);
    }
  };

  const handleStartWheelManualEdit = () => {
    if (!canEditWheelManual || wheelLoadersSplitDetailRows.length === 0) {
      return;
    }
    setEditingWheelManual(true);
    setWheelManualRows(wheelLoadersSplitDetailRows.map((row) => ({ ...row })));
    setWheelManualMessage("");
    setWheelManualError("");
  };

  const handleCancelWheelManualEdit = () => {
    setEditingWheelManual(false);
    setWheelManualRows([]);
    setWheelManualMessage("");
    setWheelManualError("");
  };

  const handleSaveWheelManualEdit = async () => {
    if (!editingWheelManual || wheelManualRows.length === 0) {
      setWheelManualError("No edited rows to save.");
      return;
    }

    const matrixType = getSplitManualMatrixType(activeWheelLoadersSplitCase);
    if (!matrixType) {
      setWheelManualError("Unsupported split case for manual save.");
      return;
    }

    try {
      setSavingWheelManual(true);
      setWheelManualError("");
      setWheelManualMessage("");

      const rowsToSave: UploadRow[] = wheelManualRows.map((row) => ({
        ...row,
        case_type: activeWheelLoadersSplitCase,
      }));
      const saveResult = await saveEditedUpload(matrixType, rowsToSave);
      const latestResult = await getLatestUploadByMatrixType(matrixType);
      const latestRows = normalizeSplitManualRows(latestResult.rows);

      setWheelLoadersSplitDetailRows(latestRows);
      setWheelLoadersSplitCaseResetToken((prev) => prev + 1);
      setEditingWheelManual(false);
      setWheelManualRows([]);
      await persistSplitCaseSnapshot(
        activeWheelLoadersSplitCase,
        wheelLoadersSplitCaseRows,
        latestRows,
        {
          grouped_rows: wheelLoadersSplitCaseRows.length,
          matched_rows: wheelLoadersSplitCaseRows.reduce((sum, item) => sum + item.matched_rows, 0),
          gross_fid_total: wheelLoadersSplitCaseRows.reduce((sum, item) => sum + item.gross_fid, 0),
          volvo_deduction_total: wheelLoadersSplitCaseRows.reduce(
            (sum, item) => sum + item.volvo_deduction,
            0
          ),
          net_fid_total: wheelLoadersSplitCaseRows.reduce((sum, item) => sum + item.net_fid, 0),
        },
        latestRows.length,
        wheelLoadersSplitCaseRows.length,
        latestRows.length
      );
      setWheelManualMessage(`Saved. New Upload ID: ${saveResult.upload_run_id}.`);
    } catch (error) {
      console.error(error);
      setWheelManualError(error instanceof Error ? error.message : "Failed to save manual edits.");
    } finally {
      setSavingWheelManual(false);
    }
  };

  const handleApplyWheelLoadersResplit = async () => {
    if (wheelLoadersSplitDetailRows.length === 0) {
      setWheelLoadersSplitMessage("No split detail rows available for re-split.");
      return;
    }

    if (
      activeWheelLoadersSplitCase !== "WLO_GT10" &&
      activeWheelLoadersSplitCase !== "WLO_LT10" &&
      activeWheelLoadersSplitCase !== "WLO_LT12"
    ) {
      setWheelLoadersSplitMessage("Resplit is currently available for WLO >10, WLO <10, and WLO <12.");
      return;
    }

    const detailConfig = getExcavatorsSplitDetailConfig(activeWheelLoadersSplitCase);
    if (!detailConfig) {
      setWheelLoadersSplitMessage("Split detail configuration is unavailable.");
      return;
    }

    try {
      setRunningWheelLoadersSplitCase(true);
      setWheelLoadersSplitError("");
      setWheelLoadersSplitMessage(`Applying ${activeWheelLoadersSplitCase} re-split logic...`);

      const [sourceMatrixRows, sizeClassRows] = await Promise.all([
        getLatestSourceMatrixRows(),
        getLatestSizeClassRows(),
      ]);

      const resplitRows = applyExcavatorsResplitByCrpSource(
        wheelLoadersSplitDetailRows,
        sourceMatrixRows,
        sizeClassRows,
        detailConfig
      );

      const flaggedRows = resplitRows.filter(
        (row) => typeof row.resplit === "string" && row.resplit.trim() !== ""
      ).length;

      setWheelLoadersSplitDetailRows(resplitRows);
      setWheelLoadersSplitCaseResetToken((prev) => prev + 1);
      setWheelResplitReadyByCase((prev) => ({
        ...prev,
        [activeWheelLoadersSplitCase]: true,
      }));
      setEditingWheelManual(false);
      setWheelManualRows([]);
      setWheelManualMessage("");
      setWheelManualError("");
      setWheelLoadersSplitMessage(
        flaggedRows > 0
          ? `${activeWheelLoadersSplitCase} re-split applied. ${flaggedRows} row(s) require re-allocation.`
          : `${activeWheelLoadersSplitCase} re-split applied. No rows required re-allocation.`
      );
    } catch (error) {
      console.error(error);
      setWheelLoadersSplitError(
        error instanceof Error ? error.message : "Failed to apply split re-split logic."
      );
    } finally {
      setRunningWheelLoadersSplitCase(false);
    }
  };

  const handleRunCrpD1CombinedReport = async () => {
    try {
      setRunningCombinedReport(true);
      setCombinedReportError("");
      setCombinedReportMessage("");

      const result = await getCrpD1CombinedReport(true);
      setCombinedReportRows(result.rows);
      setCombinedReportResetToken((prev) => prev + 1);
      setCombinedReportMessage(`Run successful. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setCombinedReportRows([]);
      setCombinedReportResetToken((prev) => prev + 1);
      setCombinedReportError(
        error instanceof Error ? error.message : "Failed to run CRP D1 Combined Report."
      );
    } finally {
      setRunningCombinedReport(false);
    }
  };

  const handleShowLatestCrpD1CombinedReport = async () => {
    try {
      setRunningCombinedReport(true);
      setCombinedReportError("");
      setCombinedReportMessage("Loading latest CRP D1 Combined Report...");

      const result = await getLatestCrpD1CombinedReport();
      setCombinedReportRows(result.rows);
      setCombinedReportResetToken((prev) => prev + 1);
      setCombinedReportMessage(`Latest loaded. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setCombinedReportRows([]);
      setCombinedReportResetToken((prev) => prev + 1);
      setCombinedReportError(
        error instanceof Error ? error.message : "Failed to show latest CRP D1 Combined Report."
      );
    } finally {
      setRunningCombinedReport(false);
    }
  };

  const handleRunOthDeletionFlagReport = async () => {
    try {
      setRunningOthDeletionFlagReport(true);
      setOthDeletionFlagStartedAt(Date.now());
      setOthDeletionFlagError("");
      setOthDeletionFlagMessage("");

      const result = await getOthDeletionFlagReport(true);
      setOthDeletionFlagRows(result.rows);
      setOthDeletionFlagResetToken((prev) => prev + 1);
      setOthDeletionFlagMessage(`Run successful. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setOthDeletionFlagRows([]);
      setOthDeletionFlagResetToken((prev) => prev + 1);
      setOthDeletionFlagError(
        error instanceof Error ? error.message : "Failed to run OTH Deletion Flag Report."
      );
    } finally {
      setRunningOthDeletionFlagReport(false);
      setOthDeletionFlagStartedAt(null);
      setOthDeletionFlagElapsedSeconds(0);
    }
  };

  const handleShowLatestOthDeletionFlagReport = async () => {
    try {
      setRunningOthDeletionFlagReport(true);
      setOthDeletionFlagStartedAt(Date.now());
      setOthDeletionFlagError("");
      setOthDeletionFlagMessage("Loading latest OTH Deletion Flag Report...");

      const result = await getLatestOthDeletionFlagReport();
      setOthDeletionFlagRows(result.rows);
      setOthDeletionFlagResetToken((prev) => prev + 1);
      setOthDeletionFlagMessage(`Latest loaded. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setOthDeletionFlagRows([]);
      setOthDeletionFlagResetToken((prev) => prev + 1);
      setOthDeletionFlagError(
        error instanceof Error ? error.message : "Failed to show latest OTH Deletion Flag Report."
      );
    } finally {
      setRunningOthDeletionFlagReport(false);
      setOthDeletionFlagStartedAt(null);
      setOthDeletionFlagElapsedSeconds(0);
    }
  };

  const handleRunP10Report = async () => {
    try {
      setRunningP10Report(true);
      setP10Error("");
      setP10Message("");

      const result = await getP10VceNonVceReport();
      setP10Rows(result.rows);
      setP10FilteredRows(result.rows);
      setP10Summary(result.summary);
      setP10ResetToken((prev) => prev + 1);
      setP10Message(`Run successful. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setP10Rows([]);
      setP10FilteredRows(null);
      setP10Summary({
        total_market_sum: 0,
        vce_sum: 0,
        non_vce_sum: 0,
      });
      setP10ResetToken((prev) => prev + 1);
      setP10Error(error instanceof Error ? error.message : "Failed to run P10 VCE / Non-VCE Report.");
    } finally {
      setRunningP10Report(false);
    }
  };

  const handleRunThreeCheckReport = async () => {
    try {
      setRunningThreeCheckReport(true);
      setThreeCheckError("");
      setThreeCheckMessage("");

      const result = await getP00ThreeCheckReport(true);
      setThreeCheckRows(result.rows);
      setThreeCheckResetToken((prev) => prev + 1);
      setThreeCheckMessage(`Run successful. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setThreeCheckRows([]);
      setThreeCheckResetToken((prev) => prev + 1);
      setThreeCheckError(error instanceof Error ? error.message : "Failed to run 3 Check Report.");
    } finally {
      setRunningThreeCheckReport(false);
    }
  };

  const handleShowLatestThreeCheckReport = async () => {
    try {
      setRunningThreeCheckReport(true);
      setThreeCheckError("");
      setThreeCheckMessage("Loading latest Check Report...");

      const result = await getLatestP00ThreeCheckReport();
      setThreeCheckRows(result.rows);
      setThreeCheckResetToken((prev) => prev + 1);
      setThreeCheckMessage(`Latest loaded. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setThreeCheckRows([]);
      setThreeCheckResetToken((prev) => prev + 1);
      setThreeCheckError(error instanceof Error ? error.message : "Failed to show latest 3 Check Report.");
    } finally {
      setRunningThreeCheckReport(false);
    }
  };

  const handleRunA10Report = async () => {
    try {
      setRunningA10Report(true);
      setA10Error("");
      setA10Message("");

      const result = await getA10AdjustmentReport();
      setA10Rows(result.rows);
      setA10ResetToken((prev) => prev + 1);
      setA10Message(`Run successful. Row Count: ${result.row_count}`);
    } catch (error) {
      console.error(error);
      setA10Rows([]);
      setA10ResetToken((prev) => prev + 1);
      setA10Error(error instanceof Error ? error.message : "Failed to run A10 Adjustment Report.");
    } finally {
      setRunningA10Report(false);
    }
  };

  useEffect(() => {
    setAutoRunHandled(false);
  }, [layerCode, location.search]);

  useEffect(() => {
    if (!layer || layer.code !== "P10" || autoRunHandled) {
      return;
    }

    const search = new URLSearchParams(location.search);
    if (search.get("auto_run") !== "p10") {
      return;
    }

    setAutoRunHandled(true);
    void handleRunP10Report();
  }, [autoRunHandled, layer, location.search]);

  const sleep = (ms: number) => new Promise<void>((resolve) => window.setTimeout(resolve, ms));

  const waitForExcavatorsSplitRun = async (caseType: string, runId: number) => {
    const maxAttempts = 180;
    for (let attempt = 0; attempt < maxAttempts; attempt += 1) {
      const run = await getExcavatorsSplitCaseRun(caseType, runId);
      if (run.status === "success") {
        return run;
      }
      if (run.status === "failed") {
        throw new Error(run.message || `${caseType} Split Case run failed.`);
      }
      await sleep(2000);
    }

    throw new Error(`Timed out waiting for ${caseType} Split Case to finish.`);
  };

  const normalizeExcavatorsSplitDetailRowsForDisplay = (rows: ExcavatorsSplitDetailRow[]) => {
    if (rows.length === 0) {
      return rows;
    }

    const hasResplitColumns = Object.prototype.hasOwnProperty.call(rows[0], "resplit");
    return hasResplitColumns ? rows : initializeResplitColumns(rows);
  };

  const persistSplitCaseSnapshot = async (
    caseType: string,
    summaryRows: ExcavatorsSplitCaseRow[],
    detailRows: ExcavatorsSplitDetailRow[],
    summary: {
      grouped_rows: number;
      matched_rows: number;
      gross_fid_total: number;
      volvo_deduction_total: number;
      net_fid_total: number;
    },
    sourceRowCount: number,
    othRowCount: number,
    p10RowCount: number
  ) => {
    await saveExcavatorsSplitCaseSnapshot({
      case_type: caseType,
      summary_rows: summaryRows,
      detail_rows: detailRows,
      summary,
      source_row_count: sourceRowCount,
      oth_row_count: othRowCount,
      p10_row_count: p10RowCount,
      message: `${caseType} split case snapshot saved successfully`,
    });
  };

  const handleShowLatestExcavatorsSplitCase = async (caseType: ExcavatorsSplitCaseType) => {
    try {
      setShowExcavatorsSplitCasePanel(true);
      setActiveExcavatorsSplitCase(caseType);
      setRunningExcavatorsSplitCase(true);
      setExcavatorsSplitCaseError("");
      setExcavatorsSplitCaseMessage(`Loading latest ${caseType} split case...`);
      setEditingExcavatorsManual(false);
      setExcavatorsManualRows([]);
      setExcavatorsManualMessage("");
      setExcavatorsManualError("");

      const latest = await getLatestExcavatorsSplitCaseReport(caseType);
      const detailRowsWithResplit = normalizeExcavatorsSplitDetailRowsForDisplay(latest.detail_rows);
      setExcavatorsSplitCaseRows(latest.summary_rows);
      setExcavatorsSplitDetailRows(detailRowsWithResplit);
      setExcavatorsSplitCaseResetToken((prev) => prev + 1);
      setExcavatorsSplitCaseMessage(
        `Latest loaded. Matching grouped rows: ${latest.summary.grouped_rows ?? latest.summary_rows.length}`
      );
    } catch (error) {
      console.error(error);
      setExcavatorsSplitCaseRows([]);
      setExcavatorsSplitDetailRows([]);
      setExcavatorsSplitCaseResetToken((prev) => prev + 1);
      setExcavatorsSplitCaseError(
        error instanceof Error ? error.message : `Failed to show latest ${caseType} split case.`
      );
    } finally {
      setRunningExcavatorsSplitCase(false);
    }
  };

  const handleRunExcavatorsSplitCase = async (caseType: ExcavatorsSplitCaseType) => {
    try {
      setShowExcavatorsSplitCasePanel(true);
      setRunningExcavatorsSplitCase(true);
      setExcavatorsSplitCaseError("");
      setExcavatorsSplitCaseMessage("");
      setActiveExcavatorsSplitCase(caseType);
      if (caseType === "CEX" || caseType === "GEC" || caseType === "GEW") {
        setExcavatorsResplitReadyByCase((prev) => ({ ...prev, [caseType]: false }));
      }
      setEditingExcavatorsManual(false);
      setExcavatorsManualRows([]);
      setExcavatorsManualMessage("");
      setExcavatorsManualError("");
      setExcavatorsSplitCaseRows([]);
      setExcavatorsSplitDetailRows([]);
      setExcavatorsSplitCaseResetToken((prev) => prev + 1);

      const run = await runExcavatorsSplitCaseReport(caseType);
      setExcavatorsSplitCaseMessage(
        `Run submitted. Waiting for ${caseType} Split Case run #${run.run_id} to finish...`
      );
      const finishedRun = await waitForExcavatorsSplitRun(caseType, run.run_id);
      setExcavatorsSplitCaseMessage(
        `Run successful. Run #${finishedRun.run_id} is saved. Click Show Latest ${caseType} to load it.`
      );
    } catch (error) {
      console.error(error);
      setExcavatorsSplitCaseRows([]);
      setExcavatorsSplitDetailRows([]);
      setExcavatorsSplitCaseResetToken((prev) => prev + 1);
      setExcavatorsSplitCaseError(
        error instanceof Error ? error.message : "Failed to build Excavators Split Case."
      );
    } finally {
      setRunningExcavatorsSplitCase(false);
    }
  };

  const handleShowLatestWheelLoadersSplitCase = async (caseType: WheelLoadersSplitCaseType) => {
    try {
      setShowWheelLoadersSplitCasePanel(true);
      setRunningWheelLoadersSplitCase(true);
      setWheelLoadersSplitError("");
      setWheelLoadersSplitMessage(`Loading latest ${caseType} split case...`);
      setActiveWheelLoadersSplitCase(caseType);
      setEditingWheelManual(false);
      setWheelManualRows([]);
      setWheelManualMessage("");
      setWheelManualError("");
      const latest = await getLatestExcavatorsSplitCaseReport(caseType);
      const detailRowsWithResplit = normalizeExcavatorsSplitDetailRowsForDisplay(latest.detail_rows);
      setWheelLoadersSplitCaseRows(latest.summary_rows);
      setWheelLoadersSplitDetailRows(detailRowsWithResplit);
      setWheelLoadersSplitCaseResetToken((prev) => prev + 1);
      setWheelResplitReadyByCase((prev) => ({
        ...prev,
        [caseType]: false,
      }));
      setWheelLoadersSplitMessage(
        `Latest loaded. Matching grouped rows: ${latest.summary.grouped_rows ?? latest.summary_rows.length}`
      );
    } catch (error) {
      console.error(error);
      setWheelLoadersSplitCaseRows([]);
      setWheelLoadersSplitDetailRows([]);
      setWheelLoadersSplitCaseResetToken((prev) => prev + 1);
      setWheelLoadersSplitError(
        error instanceof Error ? error.message : `Failed to show latest ${caseType} split case.`
      );
    } finally {
      setRunningWheelLoadersSplitCase(false);
    }
  };

  const handleRunWheelLoadersSplitCase = async (caseType: WheelLoadersSplitCaseType) => {
    try {
      setShowWheelLoadersSplitCasePanel(true);
      setRunningWheelLoadersSplitCase(true);
      setWheelLoadersSplitError("");
      setWheelLoadersSplitMessage("");
      setActiveWheelLoadersSplitCase(caseType);
      if (caseType === "WLO_GT10" || caseType === "WLO_LT10") {
        setWheelResplitReadyByCase((prev) => ({ ...prev, [caseType]: false }));
      }
      setEditingWheelManual(false);
      setWheelManualRows([]);
      setWheelManualMessage("");
      setWheelManualError("");
      setWheelLoadersSplitCaseRows([]);
      setWheelLoadersSplitDetailRows([]);
      setWheelLoadersSplitCaseResetToken((prev) => prev + 1);

      const run = await runExcavatorsSplitCaseReport(caseType);
      setWheelLoadersSplitMessage(
        `Run submitted. Waiting for ${caseType} Split Case run #${run.run_id} to finish...`
      );
      const finishedRun = await waitForExcavatorsSplitRun(caseType, run.run_id);
      setWheelLoadersSplitMessage(
        `Run successful. Run #${finishedRun.run_id} is saved. Click Show Latest ${caseType} to load it.`
      );
    } catch (error) {
      console.error(error);
      setWheelLoadersSplitCaseRows([]);
      setWheelLoadersSplitDetailRows([]);
      setWheelLoadersSplitCaseResetToken((prev) => prev + 1);
      setWheelLoadersSplitError(
        error instanceof Error ? error.message : "Failed to build Wheel Loaders Split Case."
      );
    } finally {
      setRunningWheelLoadersSplitCase(false);
    }
  };

  if (!layer) {
    return (
      <div className="page">
        <section className="section">
          <div className="section-header">
            <p className="section-tag">Layer</p>
            <h2 className="section-title">Layer Not Found</h2>
            <p className="section-description">
              The requested layer does not exist in this prototype.
            </p>
          </div>
          <Link to="/" className="btn btn--primary">
            Back to Home
          </Link>
        </section>
      </div>
    );
  }

  return (
    <div className="page">
      <section className={`section ${layer.code === "MLS" ? "section--layer-detail-wide" : ""}`.trim()}>
        <div className="section-header">
          <p className="section-tag">Layer Detail</p>
          <h2 className="section-title">
            {layer.code} - {layer.title}
          </h2>
          {layer.description ? <p className="section-description">{layer.description}</p> : null}
        </div>

        {layer.code === "P00" ? (
          <div className="summary-card">
            <div className="summary-row">
              <span className="summary-value">{layer.highlights[0]}</span>
            </div>
            <div style={{ marginTop: "4px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
              <button
                type="button"
                className="btn btn--overview"
                onClick={handleRunCrpD1CombinedReport}
                disabled={runningCombinedReport}
              >
                Run CRP D1 Combined Report
              </button>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={handleShowLatestCrpD1CombinedReport}
                disabled={runningCombinedReport}
              >
                Show Latest
              </button>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={() => setShowSqlGuide((prev) => !prev)}
              >
                {showSqlGuide ? "Hide SQL Logic" : "View SQL Logic"}
              </button>
            </div>

            {runningCombinedReport ? (
              <p style={{ color: "blue" }}>Running CRP D1 Combined Report...</p>
            ) : null}
            {combinedReportMessage ? (
              <p style={{ color: "green" }}>{combinedReportMessage}</p>
            ) : null}
            {combinedReportError ? (
              <p style={{ color: "red" }}>Error: {combinedReportError}</p>
            ) : null}
            {combinedReportRows.length > 0 ? (
              <div className="section summary-card" style={{ marginTop: "8px" }}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }}>
                  <strong>CRP D1 Combined Report</strong>
                  <button
                    type="button"
                    className="btn btn--tiny"
                    onClick={() => {
                      setCombinedReportRows([]);
                      setCombinedReportMessage("");
                      setCombinedReportError("");
                    }}
                    aria-label="Close CRP D1 Combined Report"
                  >
                    x
                  </button>
                </div>
                <FilterableTable
                  columns={combinedReportColumns}
                  rows={combinedReportRows}
                  maxHeight={REPORT_TABLE_MAX_HEIGHT}
                  resetToken={combinedReportResetToken}
                  compact
                />
              </div>
            ) : null}
            {showSqlGuide ? (
              <div className="sql-guide">
                <h4 className="sql-guide__title">Business Rules</h4>
                <ul className="sql-guide__list">
                  {CRP_D1_RULE_BULLETS.map((rule) => (
                    <li key={rule}>{rule}</li>
                  ))}
                </ul>

                <h4 className="sql-guide__title">SQL Map</h4>
                <ul className="sql-guide__list">
                  {CRP_D1_SQL_MAP_BULLETS.map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>

                <h4 className="sql-guide__title">Key SQL Snippets</h4>
                <div className="sql-guide__snippets">
                  {CRP_D1_KEY_SQL_SNIPPETS.map((snippet) => (
                    <div key={snippet.title} className="sql-guide__snippet">
                      <strong>{snippet.title}</strong>
                      <p>{snippet.explain}</p>
                      <pre>
                        <code>{snippet.sql}</code>
                      </pre>
                    </div>
                  ))}
                </div>

                <details className="sql-guide__details">
                  <summary>View Full SQL</summary>
                  <pre>
                    <code>{CRP_D1_COMBINED_SQL}</code>
                  </pre>
                </details>
              </div>
            ) : null}

            <div className="summary-row">
              <span className="summary-value">{layer.highlights[1]}</span>
            </div>
            {layer.highlights.slice(2, 5).map((item) => (
              <div key={item} className="summary-row">
                <span className="summary-value">{item}</span>
              </div>
            ))}
            <div style={{ marginTop: "4px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
              <button
                type="button"
                className="btn btn--overview"
                onClick={handleRunOthDeletionFlagReport}
                disabled={runningOthDeletionFlagReport}
              >
                Run OTH Deletion Flag Report
              </button>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={handleShowLatestOthDeletionFlagReport}
                disabled={runningOthDeletionFlagReport}
              >
                Show Latest
              </button>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={() => setShowOthSqlGuide((prev) => !prev)}
              >
                {showOthSqlGuide ? "Hide SQL Logic" : "View SQL Logic"}
              </button>
            </div>
            {layer.code === "P00" && runningOthDeletionFlagReport ? (
              <p style={{ color: "blue" }}>
                Running OTH Deletion Flag Report...
                <br />
                <span style={{ fontSize: "0.95em" }}>
                  Elapsed: {formatElapsedTime(othDeletionFlagElapsedSeconds)}. This report does a full
                  join across OTH, Source Matrix, Reporter List, and mapping tables, so the first run
                  can take a while.
                </span>
              </p>
            ) : null}
            {layer.code === "P00" && othDeletionFlagMessage ? (
              <p style={{ color: "green" }}>{othDeletionFlagMessage}</p>
            ) : null}
            {layer.code === "P00" && othDeletionFlagError ? (
              <p style={{ color: "red" }}>Error: {othDeletionFlagError}</p>
            ) : null}
            {layer.code === "P00" && othDeletionFlagRows.length > 0 ? (
              <div className="section summary-card" style={{ marginTop: "16px" }}>
                <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }}>
                  <strong>OTH Deletion Flag Report</strong>
                  <button
                    type="button"
                    className="btn btn--tiny"
                    onClick={() => {
                      setOthDeletionFlagRows([]);
                      setOthDeletionFlagMessage("");
                      setOthDeletionFlagError("");
                    }}
                    aria-label="Close OTH Deletion Flag Report"
                  >
                    x
                  </button>
                </div>
                <FilterableTable
                  columns={othDeletionFlagColumns}
                  rows={othDeletionFlagRows}
                  maxHeight={REPORT_TABLE_MAX_HEIGHT}
                  resetToken={othDeletionFlagResetToken}
                  compact
                />
              </div>
            ) : null}
            {layer.code === "P00" && showOthSqlGuide ? (
              <div className="sql-guide">
                <h4 className="sql-guide__title">OTH Rules (2.1 / 2.2 / 2.3)</h4>
                <ul className="sql-guide__list">
                  {OTH_RULE_BULLETS.map((rule) => (
                    <li key={rule}>{rule}</li>
                  ))}
                </ul>

                <h4 className="sql-guide__title">SQL Map</h4>
                <ul className="sql-guide__list">
                  {OTH_SQL_MAP_BULLETS.map((item) => (
                    <li key={item}>{item}</li>
                  ))}
                </ul>

                <h4 className="sql-guide__title">Key SQL Snippets</h4>
                <div className="sql-guide__snippets">
                  {OTH_KEY_SQL_SNIPPETS.map((snippet) => (
                    <div key={snippet.title} className="sql-guide__snippet">
                      <strong>{snippet.title}</strong>
                      <p>{snippet.explain}</p>
                      <pre>
                        <code>{snippet.sql}</code>
                      </pre>
                    </div>
                  ))}
                </div>

                <details className="sql-guide__details">
                  <summary>View Full SQL</summary>
                  <pre>
                    <code>{OTH_DELETION_FLAG_SQL}</code>
                  </pre>
                </details>
              </div>
            ) : null}
            <div className="summary-row">
              <span className="summary-value">{layer.highlights[5]}</span>
            </div>
            <div style={{ marginTop: "4px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
              <button
                type="button"
                className="btn btn--overview"
                onClick={handleRunThreeCheckReport}
                disabled={runningThreeCheckReport}
              >
                Run Check Report
              </button>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={handleShowLatestThreeCheckReport}
                disabled={runningThreeCheckReport}
              >
                Show Latest
              </button>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={() => setShowThreeCheckSqlGuide((prev) => !prev)}
              >
                {showThreeCheckSqlGuide ? "Hide SQL Logic" : "View SQL Logic"}
              </button>
            </div>
          </div>
        ) : layer.code === "MLS" ? (
          <div className="summary-card">
            <div
              style={{
                display: "flex",
                gap: "52px",
                alignItems: "center",
                flexWrap: "wrap",
                marginTop: "20px",
                marginBottom: "8px",
              }}
            >
              <div className="summary-row" style={{ marginBottom: 0 }}>
                <span className="summary-value">{EXCAVATORS_SPLIT_CASE_DETAILS.ALL.heading}</span>
              </div>
              {(["CEX", "GEC", "GEW"] as const).map((caseType) => (
                <div key={caseType} className="summary-row" style={{ marginBottom: 0 }}>
                  <span className="summary-value">{EXCAVATORS_SPLIT_CASE_DETAILS[caseType].heading}</span>
                </div>
              ))}
            </div>
            <div style={{ marginTop: "24px", display: "flex", gap: "16px", alignItems: "center", flexWrap: "wrap" }}>
              {(["ALL", "CEX", "GEC", "GEW"] as ExcavatorsSplitCaseType[]).map((caseType) => (
                <button
                  key={caseType}
                  type="button"
                  className="btn btn--overview"
                  onClick={() => handleRunExcavatorsSplitCase(caseType)}
                  disabled={runningExcavatorsSplitCase}
                >
                  {EXCAVATORS_SPLIT_CASE_DETAILS[caseType].buttonLabel}
                </button>
              ))}
            </div>
            <div style={{ marginTop: "10px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
              {(["ALL", "CEX", "GEC", "GEW"] as ExcavatorsSplitCaseType[]).map((caseType) => (
                <button
                  key={`latest-${caseType}`}
                  type="button"
                  className="btn btn--tiny"
                  onClick={() => {
                    void handleShowLatestExcavatorsSplitCase(caseType);
                  }}
                  disabled={runningExcavatorsSplitCase || savingExcavatorsManual}
                >
                  {`Show Latest ${caseType}`}
                </button>
              ))}
            </div>
            {runningExcavatorsSplitCase ? (
              <p style={{ color: "blue", marginTop: "14px" }}>
                Running {activeExcavatorsSplitCaseDetail.panelTitle}...
              </p>
            ) : null}
            {excavatorsSplitCaseMessage ? (
              <p style={{ color: "green", marginTop: "14px" }}>{excavatorsSplitCaseMessage}</p>
            ) : null}
            {excavatorsSplitCaseError ? (
              <p style={{ color: "red", marginTop: "14px" }}>Error: {excavatorsSplitCaseError}</p>
            ) : null}
            {excavatorsManualMessage ? (
              <p style={{ color: "green", marginTop: "8px" }}>{excavatorsManualMessage}</p>
            ) : null}
            {excavatorsManualError ? (
              <p style={{ color: "red", marginTop: "8px" }}>Error: {excavatorsManualError}</p>
            ) : null}
            {showExcavatorsSplitCasePanel ? (
              <>
                {showExcavatorsGroupedSummary ? (
                  <>
                    <div className="card-grid card-grid--three" style={{ marginTop: "16px" }}>
                      <article className="card">
                        <h4 className="card__title">Grouped Rows</h4>
                        <p className="summary-value">{excavatorsSplitCaseSummary.groupedRows}</p>
                      </article>
                      <article className="card">
                        <h4 className="card__title">Matched OTH Rows</h4>
                        <p className="summary-value">{excavatorsSplitCaseSummary.matchedRows}</p>
                      </article>
                      <article className="card">
                        <h4 className="card__title">Net FID</h4>
                        <p className="summary-value">
                          {formatNumberDisplay(excavatorsSplitCaseSummary.netFidTotal)}
                        </p>
                      </article>
                    </div>
                    <div className="card-grid card-grid--three" style={{ marginTop: "16px" }}>
                      <article className="card">
                        <h4 className="card__title">Gross FID</h4>
                        <p className="summary-value">
                          {formatNumberDisplay(excavatorsSplitCaseSummary.grossFidTotal)}
                        </p>
                      </article>
                      <article className="card">
                        <h4 className="card__title">Volvo Deduction</h4>
                        <p className="summary-value">
                          {formatNumberDisplay(excavatorsSplitCaseSummary.volvoDeductionTotal)}
                        </p>
                      </article>
                      <article className="card">
                        <h4 className="card__title">Net FID Check</h4>
                        <p className="summary-value">
                          {formatNumberDisplay(
                            excavatorsSplitCaseSummary.grossFidTotal -
                              excavatorsSplitCaseSummary.volvoDeductionTotal
                          )}
                        </p>
                      </article>
                    </div>
                  </>
                ) : null}
                <div className="section summary-card" style={{ marginTop: "16px" }}>
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }}>
                    <div>
                      <strong>{showExcavatorsSplitDetail ? activeExcavatorsSplitDetailConfig?.panelTitle : activeExcavatorsSplitCaseDetail.panelTitle}</strong>
                      <div style={{ color: "#64748b", fontSize: "12px", marginTop: "4px" }}>
                        {showExcavatorsSplitDetail
                          ? activeExcavatorsSplitDetailConfig?.description
                          : activeExcavatorsSplitCaseDetail.description}
                      </div>
                    </div>
                    <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                      {showExcavatorsSplitDetail &&
                      (activeExcavatorsSplitCase === "CEX" ||
                        activeExcavatorsSplitCase === "GEC" ||
                        activeExcavatorsSplitCase === "GEW") ? (
                        <>
                          <button
                            type="button"
                            className="btn btn--tiny"
                            onClick={() => {
                              void handleApplyExcavatorsResplit();
                            }}
                            disabled={runningExcavatorsSplitCase || excavatorsSplitDetailRows.length === 0}
                          >
                            Resplit FID
                          </button>
                          {!editingExcavatorsManual ? (
                            <button
                              type="button"
                              className="btn btn--tiny"
                              onClick={handleStartExcavatorsManualEdit}
                              disabled={!canEditExcavatorsManual || savingExcavatorsManual}
                              title={!canEditExcavatorsManual ? "Run Resplit FID first." : undefined}
                            >
                              Manual Edit
                            </button>
                          ) : (
                            <>
                              <button
                                type="button"
                                className="btn btn--tiny"
                                onClick={() => {
                                  void handleSaveExcavatorsManualEdit();
                                }}
                                disabled={savingExcavatorsManual}
                              >
                                {savingExcavatorsManual ? "Saving..." : "Save"}
                              </button>
                              <button
                                type="button"
                                className="btn btn--tiny"
                                onClick={handleCancelExcavatorsManualEdit}
                                disabled={savingExcavatorsManual}
                              >
                                Cancel
                              </button>
                            </>
                          )}
                        </>
                      ) : null}
                      <button
                        type="button"
                        className="btn btn--tiny"
                        onClick={() => {
                          setShowExcavatorsSplitCasePanel(false);
                          setExcavatorsSplitCaseRows([]);
                          setExcavatorsSplitDetailRows([]);
                          setExcavatorsSplitCaseMessage("");
                          setExcavatorsSplitCaseError("");
                          setEditingExcavatorsManual(false);
                          setExcavatorsManualRows([]);
                          setExcavatorsManualMessage("");
                          setExcavatorsManualError("");
                        }}
                        aria-label={`Close ${activeExcavatorsSplitCaseDetail.panelTitle}`}
                      >
                        x
                      </button>
                    </div>
                  </div>
                  {showExcavatorsGroupedSummary ? (
                    <FilterableTable
                      columns={excavatorsSplitCaseColumns}
                      rows={excavatorsSplitCaseRows}
                      maxHeight={REPORT_TABLE_MAX_HEIGHT}
                      resetToken={excavatorsSplitCaseResetToken}
                      compact
                    />
                  ) : null}
                  {showExcavatorsSplitDetail ? (
                    <FilterableTable
                      columns={excavatorsSplitDetailColumns}
                      rows={editingExcavatorsManual ? excavatorsManualRows : excavatorsSplitDetailRows}
                      maxHeight={REPORT_TABLE_MAX_HEIGHT}
                      resetToken={excavatorsSplitCaseResetToken}
                      editable={editingExcavatorsManual}
                      onRowsChange={(nextRows) =>
                        setExcavatorsManualRows(nextRows as ExcavatorsSplitDetailRow[])
                      }
                      nonEditableColumns={SPLIT_MANUAL_NON_EDITABLE_COLUMNS}
                      compact
                    />
                  ) : null}
                </div>
              </>
            ) : null}
            <div
              style={{
                display: "flex",
                gap: "52px",
                alignItems: "center",
                flexWrap: "wrap",
                marginTop: "36px",
                marginBottom: "8px",
              }}
            >
              <div className="summary-row" style={{ marginBottom: 0 }}>
                <span className="summary-value">{WHEEL_LOADERS_SPLIT_CASE_DETAILS.ALL.heading}</span>
              </div>
              {(["WLO_GT10", "WLO_LT10", "WLO_LT12"] as const).map((caseType) => (
                <div key={caseType} className="summary-row" style={{ marginBottom: 0 }}>
                  <span className="summary-value">{WHEEL_LOADERS_SPLIT_CASE_DETAILS[caseType].heading}</span>
                </div>
              ))}
            </div>
            <div style={{ marginTop: "24px", display: "flex", gap: "16px", alignItems: "center", flexWrap: "wrap" }}>
              {(["ALL", "WLO_GT10", "WLO_LT10", "WLO_LT12"] as WheelLoadersSplitCaseType[]).map((caseType) => (
                <button
                  key={caseType}
                  type="button"
                  className="btn btn--overview"
                  onClick={() => handleRunWheelLoadersSplitCase(caseType)}
                  disabled={runningWheelLoadersSplitCase}
                >
                  {WHEEL_LOADERS_SPLIT_CASE_DETAILS[caseType].buttonLabel}
                </button>
              ))}
            </div>
            <div style={{ marginTop: "10px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
              {(["ALL", "WLO_GT10", "WLO_LT10", "WLO_LT12"] as WheelLoadersSplitCaseType[]).map(
                (caseType) => (
                  <button
                    key={`latest-${caseType}`}
                    type="button"
                    className="btn btn--tiny"
                    onClick={() => {
                      void handleShowLatestWheelLoadersSplitCase(caseType);
                    }}
                    disabled={runningWheelLoadersSplitCase || savingWheelManual}
                  >
                    {caseType === "ALL"
                      ? "Show Latest ALL"
                      : caseType === "WLO_LT12"
                        ? "Show Latest WLO (<12)"
                        : caseType === "WLO_GT10"
                          ? "Show Latest WLO (>10)"
                          : "Show Latest WLO (<10)"}
                  </button>
                )
              )}
            </div>
            {runningWheelLoadersSplitCase ? (
              <p style={{ color: "blue", marginTop: "14px" }}>
                Running {activeWheelLoadersSplitCaseDetail.panelTitle}...
              </p>
            ) : null}
            {wheelLoadersSplitMessage ? (
              <p style={{ color: "green", marginTop: "14px" }}>{wheelLoadersSplitMessage}</p>
            ) : null}
            {wheelLoadersSplitError ? (
              <p style={{ color: "red", marginTop: "14px" }}>Error: {wheelLoadersSplitError}</p>
            ) : null}
            {wheelManualMessage ? (
              <p style={{ color: "green", marginTop: "8px" }}>{wheelManualMessage}</p>
            ) : null}
            {wheelManualError ? (
              <p style={{ color: "red", marginTop: "8px" }}>Error: {wheelManualError}</p>
            ) : null}
            {showWheelLoadersSplitCasePanel ? (
              <>
                {!showWheelLoadersSplitDetail ? (
                  <>
                    <div className="card-grid card-grid--three" style={{ marginTop: "16px" }}>
                      <article className="card">
                        <h4 className="card__title">Grouped Rows</h4>
                        <p className="summary-value">{wheelLoadersSplitCaseSummary.groupedRows}</p>
                      </article>
                      <article className="card">
                        <h4 className="card__title">Matched OTH Rows</h4>
                        <p className="summary-value">{wheelLoadersSplitCaseSummary.matchedRows}</p>
                      </article>
                      <article className="card">
                        <h4 className="card__title">Net FID</h4>
                        <p className="summary-value">
                          {formatNumberDisplay(wheelLoadersSplitCaseSummary.netFidTotal)}
                        </p>
                      </article>
                    </div>
                    <div className="card-grid card-grid--three" style={{ marginTop: "16px" }}>
                      <article className="card">
                        <h4 className="card__title">Gross FID</h4>
                        <p className="summary-value">
                          {formatNumberDisplay(wheelLoadersSplitCaseSummary.grossFidTotal)}
                        </p>
                      </article>
                      <article className="card">
                        <h4 className="card__title">Volvo Deduction</h4>
                        <p className="summary-value">
                          {formatNumberDisplay(wheelLoadersSplitCaseSummary.volvoDeductionTotal)}
                        </p>
                      </article>
                      <article className="card">
                        <h4 className="card__title">Net FID Check</h4>
                        <p className="summary-value">
                          {formatNumberDisplay(
                            wheelLoadersSplitCaseSummary.grossFidTotal -
                              wheelLoadersSplitCaseSummary.volvoDeductionTotal
                          )}
                        </p>
                      </article>
                    </div>
                  </>
                ) : null}
                <div className="section summary-card" style={{ marginTop: "16px" }}>
                  <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }}>
                    <div>
                      <strong>
                        {showWheelLoadersSplitDetail
                          ? activeWheelLoadersSplitDetailConfig?.panelTitle
                          : activeWheelLoadersSplitCaseDetail.panelTitle}
                      </strong>
                      <div style={{ color: "#64748b", fontSize: "12px", marginTop: "4px" }}>
                        {showWheelLoadersSplitDetail
                          ? activeWheelLoadersSplitDetailConfig?.description
                          : activeWheelLoadersSplitCaseDetail.description}
                      </div>
                    </div>
                    <div style={{ display: "flex", alignItems: "center", gap: "8px" }}>
                      {showWheelLoadersSplitDetail &&
                      (activeWheelLoadersSplitCase === "WLO_GT10" ||
                        activeWheelLoadersSplitCase === "WLO_LT10" ||
                        activeWheelLoadersSplitCase === "WLO_LT12") ? (
                        <>
                          <button
                            type="button"
                            className="btn btn--tiny"
                            onClick={() => {
                              void handleApplyWheelLoadersResplit();
                            }}
                            disabled={
                              runningWheelLoadersSplitCase || wheelLoadersSplitDetailRows.length === 0
                            }
                          >
                            Resplit FID
                          </button>
                          {!editingWheelManual ? (
                            <button
                              type="button"
                              className="btn btn--tiny"
                              onClick={handleStartWheelManualEdit}
                              disabled={!canEditWheelManual || savingWheelManual}
                              title={!canEditWheelManual ? "Run Resplit FID first." : undefined}
                            >
                              Manual Edit
                            </button>
                          ) : (
                            <>
                              <button
                                type="button"
                                className="btn btn--tiny"
                                onClick={() => {
                                  void handleSaveWheelManualEdit();
                                }}
                                disabled={savingWheelManual}
                              >
                                {savingWheelManual ? "Saving..." : "Save"}
                              </button>
                              <button
                                type="button"
                                className="btn btn--tiny"
                                onClick={handleCancelWheelManualEdit}
                                disabled={savingWheelManual}
                              >
                                Cancel
                              </button>
                            </>
                          )}
                        </>
                      ) : null}
                      <button
                        type="button"
                        className="btn btn--tiny"
                        onClick={() => {
                          setShowWheelLoadersSplitCasePanel(false);
                          setWheelLoadersSplitCaseRows([]);
                          setWheelLoadersSplitDetailRows([]);
                          setWheelLoadersSplitMessage("");
                          setWheelLoadersSplitError("");
                          setEditingWheelManual(false);
                          setWheelManualRows([]);
                          setWheelManualMessage("");
                          setWheelManualError("");
                        }}
                        aria-label={`Close ${activeWheelLoadersSplitCaseDetail.panelTitle}`}
                      >
                        x
                      </button>
                    </div>
                  </div>
                  {!showWheelLoadersSplitDetail ? (
                    <FilterableTable
                      columns={excavatorsSplitCaseColumns}
                      rows={wheelLoadersSplitCaseRows}
                      maxHeight={REPORT_TABLE_MAX_HEIGHT}
                      resetToken={wheelLoadersSplitCaseResetToken}
                      compact
                    />
                  ) : null}
                  {showWheelLoadersSplitDetail ? (
                    <FilterableTable
                      columns={wheelLoadersSplitDetailColumns}
                      rows={editingWheelManual ? wheelManualRows : wheelLoadersSplitDetailRows}
                      maxHeight={REPORT_TABLE_MAX_HEIGHT}
                      resetToken={wheelLoadersSplitCaseResetToken}
                      editable={editingWheelManual}
                      onRowsChange={(nextRows) => setWheelManualRows(nextRows as ExcavatorsSplitDetailRow[])}
                      nonEditableColumns={SPLIT_MANUAL_NON_EDITABLE_COLUMNS}
                      compact
                    />
                  ) : null}
                </div>
              </>
            ) : null}
          </div>
        ) : (
          <div className="summary-card">
            {layer.highlights.map((item) => (
              <div key={item} className="summary-row">
                <span className="summary-value">{item}</span>
              </div>
            ))}
          </div>
        )}

        {layer.code === "P10" ? (
          <div style={{ marginTop: "12px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
            <button
              type="button"
              className="btn btn--overview"
              onClick={handleRunP10Report}
              disabled={runningP10Report}
            >
              Run P10 VCE / Non-VCE
            </button>
          </div>
        ) : null}
        {layer.code === "A10" ? (
          <div style={{ marginTop: "12px", display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
            <button
              type="button"
              className="btn btn--overview"
              onClick={handleRunA10Report}
              disabled={runningA10Report}
            >
              Run A10 Adjustment Report
            </button>
          </div>
        ) : null}
        {layer.code === "P10" ? (
          <div className="sql-guide">
            <h4 className="sql-guide__title">Calculation Rules</h4>
            <ul className="sql-guide__list">
              {P10_RULE_BULLETS.map((rule) => (
                <li key={rule}>{rule}</li>
              ))}
            </ul>
          </div>
        ) : null}
        {layer.code === "A10" ? (
          <div className="sql-guide">
            <h4 className="sql-guide__title">Calculation Rules</h4>
            <ul className="sql-guide__list">
              {A10_RULE_BULLETS.map((rule) => (
                <li key={rule}>{rule}</li>
              ))}
            </ul>
          </div>
        ) : null}
        {layer.code === "P00" && runningThreeCheckReport ? (
          <p style={{ color: "blue" }}>Running 3 Check Report...</p>
        ) : null}
        {layer.code === "P00" && threeCheckMessage ? (
          <p style={{ color: "green" }}>{threeCheckMessage}</p>
        ) : null}
        {layer.code === "P00" && threeCheckError ? (
          <p style={{ color: "red" }}>Error: {threeCheckError}</p>
        ) : null}
        {layer.code === "P00" && threeCheckRows.length > 0 ? (
          <div className="section summary-card" style={{ marginTop: "16px" }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }}>
              <strong>Check Report</strong>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={() => {
                  setThreeCheckRows([]);
                  setThreeCheckMessage("");
                  setThreeCheckError("");
                }}
                aria-label="Close 3 Check Report"
              >
                x
              </button>
            </div>
            <FilterableTable
              columns={threeCheckColumns}
              rows={threeCheckRows}
              maxHeight={REPORT_TABLE_MAX_HEIGHT}
              resetToken={threeCheckResetToken}
              compact
            />
          </div>
        ) : null}
        {layer.code === "P00" && showThreeCheckSqlGuide ? (
          <div className="sql-guide">
            <h4 className="sql-guide__title">Check Rules</h4>
            <ul className="sql-guide__list">
              {THREE_CHECK_RULE_BULLETS.map((rule) => (
                <li key={rule}>{rule}</li>
              ))}
            </ul>

            <h4 className="sql-guide__title">Logic Map</h4>
            <ul className="sql-guide__list">
              {THREE_CHECK_SQL_MAP_BULLETS.map((item) => (
                <li key={item}>{item}</li>
              ))}
            </ul>

            <h4 className="sql-guide__title">Key Logic Snippets</h4>
            <div className="sql-guide__snippets">
              {THREE_CHECK_KEY_SQL_SNIPPETS.map((snippet) => (
                <div key={snippet.title} className="sql-guide__snippet">
                  <strong>{snippet.title}</strong>
                  <p>{snippet.explain}</p>
                  <pre>
                    <code>{snippet.sql}</code>
                  </pre>
                </div>
              ))}
            </div>
          </div>
        ) : null}
        {layer.code === "P10" && runningP10Report ? (
          <p style={{ color: "blue" }}>Running P10 VCE / Non-VCE calculation...</p>
        ) : null}
        {layer.code === "P10" && p10Message ? (
          <p style={{ color: "green" }}>{p10Message}</p>
        ) : null}
        {layer.code === "P10" && p10Error ? (
          <p style={{ color: "red" }}>Error: {p10Error}</p>
        ) : null}
        {layer.code === "P10" && p10Rows.length > 0 ? (
          <>
            <div className="card-grid card-grid--three" style={{ marginTop: "16px" }}>
              <article className="card">
                <h4 className="card__title">Total Market (TMA)</h4>
                <p className="summary-value">
                  {formatNumberDisplay(p10Summary.total_market_sum)}
                </p>
              </article>
              <article className="card">
                <h4 className="card__title">Volvo CE (VCE)</h4>
                <p className="summary-value">
                  {formatNumberDisplay(p10Summary.vce_sum)}
                </p>
              </article>
              <article className="card">
                <h4 className="card__title">Non-Volvo CE</h4>
                <p className="summary-value">
                  {formatNumberDisplay(p10Summary.non_vce_sum)}
                </p>
              </article>
            </div>
            <div className="section p10-share-card" style={{ marginTop: "16px" }}>
              <strong>VCE vs Non-VCE Share</strong>
              <div style={{ color: "#64748b", fontSize: "12px", marginTop: "4px" }}>
                Chart follows the filters applied in the table below.
              </div>
              <div className="p10-share-layout">
                <div
                  className="p10-donut"
                  style={{
                    background: `conic-gradient(#2563eb 0% ${p10Share.vcePct}%, #93c5fd ${p10Share.vcePct}% 100%)`,
                  }}
                >
                  <div className="p10-donut__hole">
                    <div className="p10-donut__value">{p10Share.vcePct.toFixed(2)}%</div>
                    <div className="p10-donut__label">VCE Share</div>
                  </div>
                </div>
                <div className="p10-share-legend">
                  <div className="p10-share-legend__item">
                    <span className="p10-share-legend__dot p10-share-legend__dot--vce" />
                    <span>Volvo CE (VCE)</span>
                    <strong>
                      {formatNumberDisplay(p10Share.safeVce)} ({p10Share.vcePct.toFixed(2)}
                      %)
                    </strong>
                  </div>
                  <div className="p10-share-legend__item">
                    <span className="p10-share-legend__dot p10-share-legend__dot--non-vce" />
                    <span>Non-Volvo CE</span>
                    <strong>
                      {formatNumberDisplay(p10Share.safeNonVce)} ({p10Share.nonVcePct.toFixed(2)}%)
                    </strong>
                  </div>
                  <div className="p10-share-legend__total">
                    Total Market:{" "}
                    <strong>{formatNumberDisplay(p10Share.totalMarket)}</strong>
                  </div>
                </div>
              </div>
            </div>
            <div className="section summary-card" style={{ marginTop: "16px" }}>
              <strong>P10 VCE / Non-VCE Report</strong>
              <FilterableTable
                columns={p10Columns}
                rows={p10Rows}
                maxHeight={REPORT_TABLE_MAX_HEIGHT}
                resetToken={p10ResetToken}
                onFilteredRowsChange={(rows) => setP10FilteredRows(rows as P10VceNonVceRow[])}
                compact
              />
            </div>
          </>
        ) : null}
        {layer.code === "A10" && runningA10Report ? (
          <p style={{ color: "blue" }}>Running A10 Adjustment Report...</p>
        ) : null}
        {layer.code === "A10" && a10Message ? (
          <p style={{ color: "green" }}>{a10Message}</p>
        ) : null}
        {layer.code === "A10" && a10Error ? (
          <p style={{ color: "red" }}>Error: {a10Error}</p>
        ) : null}
        {layer.code === "A10" && a10Rows.length > 0 ? (
          <div className="section summary-card" style={{ marginTop: "16px" }}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", gap: "8px" }}>
              <strong>A10 Adjustment Report</strong>
              <button
                type="button"
                className="btn btn--tiny"
                onClick={() => {
                  setA10Rows([]);
                  setA10Message("");
                  setA10Error("");
                }}
                aria-label="Close A10 Adjustment Report"
              >
                x
              </button>
            </div>
            <FilterableTable
              columns={a10Columns}
              rows={a10Rows}
              maxHeight={REPORT_TABLE_MAX_HEIGHT}
              resetToken={a10ResetToken}
              getRowClassName={(row) =>
                String(row.brand_code ?? "").trim().toUpperCase() === "RESULT"
                  ? "data-table__row--result"
                  : undefined
              }
              compact
            />
          </div>
        ) : null}

        <div style={{ marginTop: "16px", display: "flex", gap: "10px", flexWrap: "wrap" }}>
          <Link to="/" className="btn btn--primary">
            Back to Home
          </Link>
          <Link to="/pipeline" className="btn btn--secondary">
            Open Pipeline Viewer
          </Link>
        </div>
      </section>
    </div>
  );
}

export default LayerDetailPage;
