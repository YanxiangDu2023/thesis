import { useMemo, useState } from "react";
import FilterableTable from "../components/table/FilterableTable";
import {
  getTotalMarketCalculationEligibleOthRun,
  getLatestTotalMarketCalculationEligibleOthRows,
  getTotalMarketCalculationDoubleBrandCheckRows,
  runTotalMarketCalculationEligibleOthReport,
} from "../api/uploads";
import type {
  OthDeletionFlagRow,
  TotalMarketCalculationDoubleBrandCheckRow,
} from "../types/upload";

type CompactionDoubleBrandRow = OthDeletionFlagRow & {
  db_indicator_by_country: number;
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
  const [loading, setLoading] = useState(false);
  const [latestLoading, setLatestLoading] = useState(false);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  const [rows, setRows] = useState<OthDeletionFlagRow[]>([]);
  const [sourceRowCount, setSourceRowCount] = useState(0);
  const [splitMachineLines, setSplitMachineLines] = useState<string[]>([]);
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

  async function handleLoad() {
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
      setRows(result.rows.map((row) => ({ ...row, source_flag: "OTH" })));
      setSourceRowCount(result.source_row_count);
      setSplitMachineLines(result.split_machine_lines);
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
    setDoubleBrandError("");
    setDoubleBrandMessage("Delete Double Brand action is ready. Select case buttons below.");
  }

  async function handleLoadDeleteDoubleBrandCompactionMachine() {
    setCompactionRequested(true);
    setCompactionLoading(true);
    setCompactionError("");
    setCompactionMessage("Loading Compaction Machine OTH rows...");

    try {
      const latest = await getLatestTotalMarketCalculationEligibleOthRows();
      const compactionOnlyRows = latest.rows.filter(isCompactionMachineRow);
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
    setDoubleBrandMessage(`Selected ${caseName}.`);
  }

  function handleSaveDeleteCaseSelection() {
    setDoubleBrandMessage(`Saved Delete Double Brand selection: ${selectedDeleteCase}.`);
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

  function handleSaveCompactionEdits() {
    setCompactionSavedRows(cloneCompactionRows(compactionRows));
    setCompactionDirty(false);
    setCompactionMessage("Compaction edits saved in this page session.");
  }

  function handleResetCompactionEdits() {
    setCompactionRows(cloneCompactionRows(compactionSavedRows));
    setCompactionDirty(false);
    setCompactionMessage("Compaction edits reverted to last saved state.");
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
              onClick={handleLoad}
              disabled={loading || latestLoading}
            >
              {loading ? "Running..." : "Show Raw Total Market Calculation Rows"}
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
              >
                Save
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
                  disabled={compactionRows.length === 0 || !compactionDirty}
                >
                  Save edits
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
      </section>
    </div>
  );
}

export default TotalMarketCalculationPage;
