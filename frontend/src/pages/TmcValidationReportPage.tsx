import { useEffect, useMemo, useState } from "react";
import FilterableTable from "../components/table/FilterableTable";
import {
  createPlanningYear,
  getPlanningYears,
  getLatestTotalMarketCalculationCalculatedRows,
} from "../api/uploads";
import type { OthDeletionFlagRow } from "../types/upload";

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
  reporter_flag: string;
  fid: number;
};

type ValidationSummaryRow = {
  country: string;
  country_code: string;
  machine_line_name: string;
  restatement: number;
  tm_crp: number;
  volvo_sal: number;
  not_assigned: number;
  difference: number;
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

function getRestatementGroupKey(row: OthDeletionFlagRow): string {
  return [toKey(row.country), toKey(row.artificial_machine_line), toKey(row.size_class_flag)].join("||");
}

function getRestatementBaseKey(row: OthDeletionFlagRow): string {
  return [toKey(row.country), toKey(row.artificial_machine_line)].join("||");
}

function getFinalRestatementGroupKey(row: OthDeletionFlagRow): string {
  return [toKey(row.country), toKey(row.artificial_machine_line), toKey(row.size_class_flag)].join("||");
}

function isReporterFlagY(row: OthDeletionFlagRow): boolean {
  return toKey(row.reporter_flag) === "Y";
}

function TmcValidationReportPage() {
  const [planningYears, setPlanningYears] = useState<number[]>([]);
  const [planningYearsLoading, setPlanningYearsLoading] = useState(false);
  const [planningYearsError, setPlanningYearsError] = useState("");
  const [selectedPlanningYear, setSelectedPlanningYear] = useState("");
  const [newPlanningYearInput, setNewPlanningYearInput] = useState("");
  const [creatingPlanningYear, setCreatingPlanningYear] = useState(false);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");
  const [error, setError] = useState("");
  const [validationSummaryRows, setValidationSummaryRows] = useState<ValidationSummaryRow[]>([]);

  const selectedPlanningYearNumber = selectedPlanningYear ? Number(selectedPlanningYear) : undefined;
  const hasSelectedPlanningYear =
    selectedPlanningYearNumber !== undefined && Number.isFinite(selectedPlanningYearNumber);

  const validationSummaryColumns = useMemo(
    () => [
      { key: "country", label: "Country" },
      { key: "country_code", label: "" },
      { key: "machine_line_name", label: "Machine Line" },
      { key: "restatement", label: "Restatement" },
      { key: "tm_crp", label: "TM(CRP)" },
      { key: "volvo_sal", label: "VOLVO SAL" },
      { key: "not_assigned", label: "Not assigned" },
      { key: "difference", label: "Difference" },
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

  async function handleCreatePlanningYear() {
    const parsed = Number(newPlanningYearInput.trim());
    if (!Number.isInteger(parsed) || parsed < 1900 || parsed > 2999) {
      setPlanningYearsError("Year must be an integer between 1900 and 2999.");
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
    } catch (err) {
      setPlanningYearsError(err instanceof Error ? err.message : "Failed to create planning year.");
    } finally {
      setCreatingPlanningYear(false);
    }
  }

  async function handleShowRestatementValidation(cleanReporterOnly = false) {
    if (!hasSelectedPlanningYear || selectedPlanningYearNumber === undefined) {
      setError("Please select a planning year first.");
      setMessage("");
      return;
    }

    setLoading(true);
    setError("");
    setMessage(
      cleanReporterOnly
        ? "Loading latest Calculate Total Market snapshot and generating clean restatement result..."
        : "Loading latest Calculate Total Market snapshot and generating final restatement result..."
    );
    setValidationSummaryRows([]);

    try {
      let result;
      try {
        result = await getLatestTotalMarketCalculationCalculatedRows(selectedPlanningYearNumber);
      } catch {
        throw new Error(
          "No Calculate Total Market snapshot found for this year. Please finish Delete Double Brand in Total Market Calculation, click Calculate Total Market, then return to TMC Validation Report."
        );
      }

      const fullRows = result.rows;
      const rows = fullRows.filter((row) => {
        if (!isOthNonVceNonZeroRow(row)) {
          return false;
        }
        return cleanReporterOnly ? isReporterFlagY(row) : true;
      });

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
          (toKey(row.brand_code) === "VCE" || toKey(row.brand_name).includes("VOLVO"));
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

      const groupMeta = new Map<
        string,
        {
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
        const tma = useFallback ? (tmaByBase.get(baseKey) ?? 0) : (tmaByGroup.get(groupKey) ?? 0);
        const vceSal = useFallback ? (vceSalByBase.get(baseKey) ?? 0) : (vceSalByGroup.get(groupKey) ?? 0);
        const targetNonVce = Math.max(tma - vceSal, 0);
        const othSum = useFallback ? (othByBase.get(baseKey) ?? 0) : (othByGroup.get(groupKey) ?? 0);
        groupMeta.set(groupKey, { targetNonVce, othSum });
      }

      const nextRows: RestatementPreviewRow[] = rows.map((row) => {
        const groupKey = getRestatementGroupKey(row);
        const meta = groupMeta.get(groupKey);
        const before = toNumber(row.fid);
        const targetNonVce = meta?.targetNonVce ?? 0;
        const othSum = meta?.othSum ?? 0;

        let after = before;
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

      const topUpRowsByGroup = new Map<string, RestatementPreviewRow[]>();
      for (const [groupKey, meta] of groupMeta.entries()) {
        const othSum = meta.othSum;
        const targetNonVce = meta.targetNonVce;
        if (targetNonVce - othSum <= 0.0001) {
          continue;
        }
        const gap = targetNonVce - othSum;
        const groupIndexes = rowsByGroup.get(groupKey) ?? [];
        const template = groupIndexes.length > 0 ? nextRows[groupIndexes[0]] : undefined;
        if (!template) {
          continue;
        }
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

      const groupMetaByKey = new Map<
        string,
        Pick<
          FinalRestatementResultRow,
          | "country_grouping"
          | "country"
          | "country_code"
          | "region"
          | "machine_line_code"
          | "machine_line_name"
          | "artificial_machine_line"
          | "size_class_flag"
        >
      >();
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

      for (const row of orderedNextRows) {
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
          reporter_flag: "#",
          fid: Number((tmaByKey.get(groupKey) ?? 0).toFixed(2)),
        });

        resultRows.push({
          ...meta,
          row_type: "SAL",
          source: "SAL",
          brand_code: "VCE",
          brand_name: "VOLVO CE",
          reporter_flag: "Y",
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
            reporter_flag: toDisplayText(brandRow.reporter_flag),
            fid: Number(toNumber(brandRow.after_restatement).toFixed(2)),
          });
        }
      }

      const summaryByCountryMachine = new Map<
        string,
        {
          country: string;
          country_code: string;
          machine_line_name: string;
          restatement: number;
          tm_crp: number;
          volvo_sal: number;
          not_assigned: number;
        }
      >();

      for (const row of resultRows) {
        const summaryKey = [toKey(row.country), toKey(row.country_code), toKey(row.machine_line_name)].join("||");
        if (!summaryByCountryMachine.has(summaryKey)) {
          summaryByCountryMachine.set(summaryKey, {
            country: row.country,
            country_code: row.country_code,
            machine_line_name: row.machine_line_name,
            restatement: 0,
            tm_crp: 0,
            volvo_sal: 0,
            not_assigned: 0,
          });
        }

        const summaryRow = summaryByCountryMachine.get(summaryKey);
        if (!summaryRow) {
          continue;
        }

        if (row.row_type === "TMA") {
          summaryRow.tm_crp += toNumber(row.fid);
        } else if (row.row_type === "SAL") {
          summaryRow.volvo_sal += toNumber(row.fid);
        } else if (row.row_type === "Restated OTH") {
          summaryRow.restatement += toNumber(row.fid);
        }
      }

      const sortedSummaryRows = Array.from(summaryByCountryMachine.values()).sort((a, b) => {
        return (
          a.country.localeCompare(b.country) ||
          a.country_code.localeCompare(b.country_code) ||
          a.machine_line_name.localeCompare(b.machine_line_name)
        );
      });

      const visibleSummaryRows = cleanReporterOnly
        ? sortedSummaryRows.filter((row) => Math.abs(row.restatement) > 0.0001)
        : sortedSummaryRows;

      const summaryRows = visibleSummaryRows.map((row, index) => {
        const previous = index > 0 ? visibleSummaryRows[index - 1] : undefined;
        const sameCountryAsPrevious =
          previous &&
          previous.country === row.country &&
          previous.country_code === row.country_code;

        const difference = row.tm_crp - row.restatement - row.volvo_sal - row.not_assigned;

        return {
          country: sameCountryAsPrevious ? "" : row.country,
          country_code: sameCountryAsPrevious ? "" : row.country_code,
          machine_line_name: row.machine_line_name,
          restatement: Number(row.restatement.toFixed(4)),
          tm_crp: Number(row.tm_crp.toFixed(4)),
          volvo_sal: Number(row.volvo_sal.toFixed(4)),
          not_assigned: Number(row.not_assigned.toFixed(4)),
          difference: Number(difference.toFixed(4)),
        };
      });

      setValidationSummaryRows(summaryRows);
      setMessage(
        cleanReporterOnly
          ? `Clean validation report generated: ${summaryRows.length} rows from ${groupOrder.length} final restatement groups with Reporter Flag = Y.`
          : `Validation report generated: ${summaryRows.length} rows from ${groupOrder.length} final restatement groups.`
      );
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to generate validation result.");
      setMessage("");
      setValidationSummaryRows([]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="page">
      <section className="section section--layer-detail-wide">
        <div className="section-header">
          <p className="section-tag">Validation Layer</p>
          <h1 className="section-title">TMC Validation Report</h1>
          <p className="section-description">
            Review calculated Total Market output through a dedicated validation layer before
            final business sign-off and downstream use.
          </p>
        </div>

        <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", alignItems: "center", marginBottom: "10px" }}>
          <label htmlFor="tmc-validation-planning-year" style={{ fontWeight: 700 }}>
            Planning Year
          </label>
          <select
            id="tmc-validation-planning-year"
            value={selectedPlanningYear}
            onChange={(event) => setSelectedPlanningYear(event.target.value)}
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
            Select a planning year first, then show the validation result.
          </p>
        ) : null}

        {hasSelectedPlanningYear ? (
          <>
            <div className="overview-actions" style={{ marginBottom: "16px" }}>
              <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", alignItems: "flex-start" }}>
                <button
                  type="button"
                  className="btn btn--overview"
                  onClick={() => void handleShowRestatementValidation(false)}
                  disabled={loading}
                >
                  {loading ? "Loading..." : "Show Restatement Result"}
                </button>
                <button
                  type="button"
                  className="btn btn--overview"
                  onClick={() => void handleShowRestatementValidation(true)}
                  disabled={loading}
                  style={{ minWidth: "240px" }}
                >
                  {loading ? "Loading..." : "Clean Restatement Result"}
                </button>
              </div>
            </div>

            {message ? <p style={{ color: "#0a8f3d", marginBottom: "12px" }}>{message}</p> : null}
            {error ? <p style={{ color: "#d62828", marginBottom: "12px" }}>{error}</p> : null}

            <div className="section summary-card" style={{ marginTop: "8px" }}>
              <strong>TMC Validation Report (Country / Machine Line Summary)</strong>
              <FilterableTable
                columns={validationSummaryColumns}
                rows={validationSummaryRows}
                maxHeight="520px"
                compact
                virtualize
                emptyMessage='No validation report rows yet. Click "Show Restatement Result" first.'
              />
            </div>
          </>
        ) : null}
      </section>
    </div>
  );
}

export default TmcValidationReportPage;
