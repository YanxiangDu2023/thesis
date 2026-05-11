import { useEffect, useMemo, useRef, useState } from "react";

export type FilterableColumn = {
  key: string;
  label?: string;
  filterable?: boolean;
  summarizable?: boolean;
  className?: string;
};

type FilterableTableProps = {
  columns: FilterableColumn[];
  rows: Array<Record<string, string | number | null>>;
  maxHeight?: string;
  emptyMessage?: string;
  resetToken?: string | number;
  editable?: boolean;
  onRowsChange?: (nextRows: Array<Record<string, string | number | null>>) => void;
  onDeleteRow?: (rowIndex: number) => void;
  onFiltersChange?: (filters: Record<string, string[]>) => void;
  onFilteredRowsChange?: (rows: Array<Record<string, string | number | null>>) => void;
  nonEditableColumns?: string[];
  compact?: boolean;
  getRowClassName?: (row: Record<string, string | number | null>, index: number) => string | undefined;
  virtualize?: boolean;
};

type CellSelectionRange = {
  columnKey: string;
  startIndex: number;
  endIndex: number;
};

const EMPTY_FILTER_VALUE = "__EMPTY_FILTER__";
const VIRTUALIZATION_ROW_THRESHOLD = 300;
const COMPACT_ROW_HEIGHT = 38;
const DEFAULT_ROW_HEIGHT = 52;
const VIRTUALIZATION_OVERSCAN = 10;

function toCellText(value: string | number | null | undefined): string {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value);
}

function toFilterMatchKey(value: string | number | null | undefined): string {
  return toCellText(value).toLocaleUpperCase();
}

function toNumericValue(value: string | number | null | undefined): number | null {
  if (value === null || value === undefined || value === "") {
    return null;
  }

  if (typeof value === "number") {
    return Number.isFinite(value) ? value : null;
  }

  const normalized = value.replace(/,/g, "").replace(/%/g, "").trim();
  if (!normalized) {
    return null;
  }

  const parsed = Number(normalized);
  return Number.isFinite(parsed) ? parsed : null;
}

function formatNumericDisplayValue(value: number, fractionDigits: number): string {
  return value.toLocaleString(undefined, {
    minimumFractionDigits: fractionDigits,
    maximumFractionDigits: fractionDigits,
  });
}

function isEffectivelyInteger(value: number): boolean {
  return Math.abs(value - Math.round(value)) < 1e-9;
}

function shouldHideDotZeroForP10Metrics(columnKey: string): boolean {
  return (
    columnKey === "total_market" ||
    columnKey === "vce" ||
    columnKey === "non_vce" ||
    columnKey === "vce_share_pct"
  );
}

function shouldRenderAsText(columnKey: string): boolean {
  return /(^|_)(calendar|year|code|flag|id|index|row)(_|$)/i.test(columnKey) || /^machine$/i.test(columnKey);
}

function getSummaryFractionDigits(columnKey: string, value: number): number {
  if (columnKey === "before_after_difference") {
    return 0;
  }

  if (shouldHideDotZeroForP10Metrics(columnKey) && isEffectivelyInteger(value)) {
    return 0;
  }

  return 2;
}

function formatCellValue(value: string | number | null | undefined, columnKey: string): string {
  if (value === null || value === undefined) {
    return "";
  }

  if (columnKey === "split_ratio") {
    const text = String(value);
    return text.replace(/(-?\d+(?:\.\d+)?)%/g, (_, num: string) => `${Number(num).toFixed(2)}%`);
  }

  if (columnKey === "year") {
    return String(value);
  }

  if (columnKey === "calendar") {
    const numericValue = toNumericValue(value);
    return numericValue !== null ? String(Math.round(numericValue)) : String(value);
  }

  if (columnKey === "machine") {
    const numericValue = toNumericValue(value);
    return numericValue !== null ? String(Math.round(numericValue)) : String(value);
  }

  if (shouldRenderAsText(columnKey)) {
    return String(value);
  }

  const numericValue = toNumericValue(value);
  if (numericValue !== null) {
    if (columnKey === "db_indicator_by_country") {
      return formatNumericDisplayValue(Math.round(numericValue), 0);
    }

    if (columnKey === "before_after_difference") {
      return formatNumericDisplayValue(Math.round(numericValue), 0);
    }

    if (shouldHideDotZeroForP10Metrics(columnKey)) {
      const fractionDigits = isEffectivelyInteger(numericValue) ? 0 : 2;
      return formatNumericDisplayValue(numericValue, fractionDigits);
    }

    return formatNumericDisplayValue(numericValue, 2);
  }

  return String(value);
}

function shouldSummarizeColumn(
  column: FilterableColumn,
  rows: Array<Record<string, string | number | null>>,
): boolean {
  if (column.summarizable === false) {
    return false;
  }

  if (column.summarizable === true) {
    return true;
  }

  if (shouldRenderAsText(column.key)) {
    return false;
  }

  const nonEmptyValues = rows
    .map((row) => row[column.key])
    .filter((value) => value !== null && value !== undefined && value !== "");

  if (nonEmptyValues.length === 0) {
    return false;
  }

  return nonEmptyValues.every((value) => toNumericValue(value) !== null);
}

function rowMatchesFilters(
  row: Record<string, string | number | null>,
  columns: FilterableColumn[],
  filters: Record<string, string[]>,
  excludeColumnKey?: string,
): boolean {
  return columns.every((column) => {
    if (column.key === excludeColumnKey) {
      return true;
    }

    const selected = filters[column.key] ?? [];
    if (selected.length === 0) {
      return true;
    }

    const cellText = toCellText(row[column.key]);
    const cellMatchKey = toFilterMatchKey(cellText);
    return selected.some((value) => {
      if (value === EMPTY_FILTER_VALUE) {
        return cellText === "";
      }
      return cellMatchKey === toFilterMatchKey(value);
    });
  });
}

function getFilterSummaryLabel(selectedValues: string[]): string {
  if (selectedValues.length === 0) {
    return "All";
  }

  if (selectedValues.length === 1) {
    return selectedValues[0] === EMPTY_FILTER_VALUE ? "(empty)" : selectedValues[0];
  }

  return `${selectedValues.length} selected`;
}

function FilterableTable({
  columns,
  rows,
  maxHeight,
  emptyMessage = "No rows found.",
  resetToken,
  editable = false,
  onRowsChange,
  onDeleteRow,
  onFiltersChange,
  onFilteredRowsChange,
  nonEditableColumns = [],
  compact = false,
  getRowClassName,
  virtualize = false,
}: FilterableTableProps) {
  const [filters, setFilters] = useState<Record<string, string[]>>({});
  const [openFilterKey, setOpenFilterKey] = useState<string | null>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportHeight, setViewportHeight] = useState(0);
  const [selectionRange, setSelectionRange] = useState<CellSelectionRange | null>(null);
  const [selectionDragging, setSelectionDragging] = useState(false);
  const filterMenuRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const tableWrapperRef = useRef<HTMLDivElement | null>(null);
  const selectionAnchorRef = useRef<{ columnKey: string; startIndex: number } | null>(null);
  const selectionFrameRef = useRef<number | null>(null);
  const pendingSelectionRef = useRef<{ columnKey: string; startIndex: number; endIndex: number } | null>(null);
  const scrollFrameRef = useRef<number | null>(null);
  const pendingScrollTopRef = useRef(0);
  const showRowActions = editable && typeof onDeleteRow === "function";
  const isSelectionEnabled = !editable;
  const normalizedFilters = useMemo(() => {
    const next: Record<string, string[]> = {};
    Object.entries(filters).forEach(([key, value]) => {
      next[key] = value.map((item) => (item === EMPTY_FILTER_VALUE ? "" : item));
    });
    return next;
  }, [filters]);

  useEffect(() => {
    setFilters({});
    setOpenFilterKey(null);
    setScrollTop(0);
    setSelectionRange(null);
    setSelectionDragging(false);
    selectionAnchorRef.current = null;
    tableWrapperRef.current?.scrollTo({ top: 0 });
  }, [resetToken]);

  useEffect(() => {
    if (!editable) {
      return;
    }
    // Editing with active filters can make rows disappear while typing.
    setFilters({});
    setOpenFilterKey(null);
  }, [editable]);

  useEffect(() => {
    const wrapper = tableWrapperRef.current;
    if (!wrapper) {
      return undefined;
    }

    const syncViewportHeight = () => {
      setViewportHeight(wrapper.clientHeight);
    };

    syncViewportHeight();

    if (typeof ResizeObserver === "undefined") {
      return undefined;
    }

    const resizeObserver = new ResizeObserver(() => {
      syncViewportHeight();
    });

    resizeObserver.observe(wrapper);

    return () => {
      resizeObserver.disconnect();
    };
  }, [maxHeight]);

  useEffect(() => {
    if (!openFilterKey) {
      return undefined;
    }

    const activeFilterKey = openFilterKey;

    function handlePointerDown(event: MouseEvent) {
      const container = filterMenuRefs.current[activeFilterKey];
      if (container && !container.contains(event.target as Node)) {
        setOpenFilterKey(null);
      }
    }

    function handleEscape(event: KeyboardEvent) {
      if (event.key === "Escape") {
        setOpenFilterKey(null);
      }
    }

    document.addEventListener("mousedown", handlePointerDown);
    document.addEventListener("keydown", handleEscape);

    return () => {
      document.removeEventListener("mousedown", handlePointerDown);
      document.removeEventListener("keydown", handleEscape);
    };
  }, [openFilterKey]);

  useEffect(() => {
    if (!selectionDragging) {
      return undefined;
    }

    function handlePointerUp() {
      setSelectionDragging(false);
      selectionAnchorRef.current = null;
    }

    document.addEventListener("mouseup", handlePointerUp);
    return () => {
      document.removeEventListener("mouseup", handlePointerUp);
    };
  }, [selectionDragging]);

  useEffect(() => {
    return () => {
      if (selectionFrameRef.current !== null) {
        cancelAnimationFrame(selectionFrameRef.current);
      }
      if (scrollFrameRef.current !== null) {
        cancelAnimationFrame(scrollFrameRef.current);
      }
    };
  }, []);

  useEffect(() => {
    if (onFiltersChange) {
      onFiltersChange(normalizedFilters);
    }
  }, [normalizedFilters, onFiltersChange]);

  const filterOptions = useMemo(() => {
    const options: Record<string, string[]> = {};

    columns.forEach((column) => {
      if (openFilterKey && column.key !== openFilterKey) {
        const selectedOptions = new Map<string, string>();
        (filters[column.key] ?? []).forEach((value) => {
          const displayValue = value === EMPTY_FILTER_VALUE ? "" : value;
          selectedOptions.set(toFilterMatchKey(displayValue), displayValue);
        });
        options[column.key] = Array.from(selectedOptions.values()).sort((a, b) =>
          a.localeCompare(b)
        );
        return;
      }

      const uniqueValues = new Map<string, string>();
      const relevantRows = rows.filter((row) =>
        rowMatchesFilters(row, columns, filters, column.key)
      );

      relevantRows.forEach((row) => {
        const displayValue = toCellText(row[column.key]);
        const matchKey = toFilterMatchKey(displayValue);
        if (!uniqueValues.has(matchKey)) {
          uniqueValues.set(matchKey, displayValue);
        }
      });

      (filters[column.key] ?? []).forEach((selectedValue) => {
        const displayValue = selectedValue === EMPTY_FILTER_VALUE ? "" : selectedValue;
        const matchKey = toFilterMatchKey(displayValue);
        if (!uniqueValues.has(matchKey)) {
          uniqueValues.set(matchKey, displayValue);
        }
      });

      options[column.key] = Array.from(uniqueValues.values()).sort((a, b) =>
        a.localeCompare(b)
      );
    });

    return options;
  }, [columns, filters, openFilterKey, rows]);

  const filteredRows = useMemo(() => {
    return rows
      .map((row, index) => ({ row, index }))
      .filter(({ row }) => rowMatchesFilters(row, columns, filters));
  }, [columns, filters, rows]);

  const summaryItems = useMemo(() => {
    const filteredOnlyRows = filteredRows.map((item) => item.row);

    return columns
      .filter((column) => shouldSummarizeColumn(column, rows))
      .map((column) => {
        const sum = filteredOnlyRows.reduce((total, row) => {
          const numericValue = toNumericValue(row[column.key]);
          return total + (numericValue ?? 0);
        }, 0);

        return {
          key: column.key,
          label: column.label ?? column.key,
          sum,
        };
      });
  }, [columns, filteredRows, rows]);

  const selectionNumericProfile = useMemo(() => {
    if (!selectionRange) {
      return null;
    }

    const prefixSum: number[] = new Array(filteredRows.length + 1).fill(0);
    const prefixCount: number[] = new Array(filteredRows.length + 1).fill(0);
    for (let idx = 0; idx < filteredRows.length; idx += 1) {
      const numericValue = toNumericValue(filteredRows[idx].row[selectionRange.columnKey]);
      prefixSum[idx + 1] = prefixSum[idx] + (numericValue ?? 0);
      prefixCount[idx + 1] = prefixCount[idx] + (numericValue !== null ? 1 : 0);
    }

    return {
      columnKey: selectionRange.columnKey,
      prefixSum,
      prefixCount,
    };
  }, [filteredRows, selectionRange]);

  const selectedRangeStats = useMemo(() => {
    if (!selectionRange) {
      return null;
    }

    const minIndex = Math.max(0, Math.min(selectionRange.startIndex, selectionRange.endIndex));
    const maxIndex = Math.min(
      filteredRows.length - 1,
      Math.max(selectionRange.startIndex, selectionRange.endIndex),
    );

    if (minIndex > maxIndex || minIndex >= filteredRows.length) {
      return null;
    }

    const column = columns.find((item) => item.key === selectionRange.columnKey);
    if (!column) {
      return null;
    }

    if (!selectionNumericProfile || selectionNumericProfile.columnKey !== selectionRange.columnKey) {
      return null;
    }

    const sum = selectionNumericProfile.prefixSum[maxIndex + 1] - selectionNumericProfile.prefixSum[minIndex];
    const numericCount =
      selectionNumericProfile.prefixCount[maxIndex + 1] -
      selectionNumericProfile.prefixCount[minIndex];
    const average = numericCount > 0 ? sum / numericCount : null;

    return {
      columnKey: selectionRange.columnKey,
      columnLabel: column.label ?? column.key,
      rowCount: maxIndex - minIndex + 1,
      numericCount,
      sum,
      average,
    };
  }, [columns, filteredRows.length, selectionNumericProfile, selectionRange]);

  useEffect(() => {
    if (!selectionRange) {
      return;
    }

    if (filteredRows.length === 0 || selectionRange.startIndex >= filteredRows.length) {
      setSelectionRange(null);
    }
  }, [filteredRows.length, selectionRange]);

  useEffect(() => {
    if (onFilteredRowsChange) {
      onFilteredRowsChange(filteredRows.map((item) => item.row));
    }
  }, [filteredRows, onFilteredRowsChange]);

  const shouldVirtualize =
    virtualize && Boolean(maxHeight) && filteredRows.length > VIRTUALIZATION_ROW_THRESHOLD;
  const estimatedRowHeight = compact ? COMPACT_ROW_HEIGHT : DEFAULT_ROW_HEIGHT;

  const visibleRange = useMemo(() => {
    if (!shouldVirtualize) {
      return {
        startIndex: 0,
        endIndex: filteredRows.length,
      };
    }

    const safeViewportHeight = viewportHeight || 480;
    const startIndex = Math.max(0, Math.floor(scrollTop / estimatedRowHeight) - VIRTUALIZATION_OVERSCAN);
    const visibleCount = Math.ceil(safeViewportHeight / estimatedRowHeight) + VIRTUALIZATION_OVERSCAN * 2;
    const endIndex = Math.min(filteredRows.length, startIndex + visibleCount);

    return { startIndex, endIndex };
  }, [estimatedRowHeight, filteredRows.length, scrollTop, shouldVirtualize, viewportHeight]);

  const displayedRows = useMemo(() => {
    if (!shouldVirtualize) {
      return filteredRows;
    }

    return filteredRows.slice(visibleRange.startIndex, visibleRange.endIndex);
  }, [filteredRows, shouldVirtualize, visibleRange.endIndex, visibleRange.startIndex]);

  const topSpacerHeight = shouldVirtualize ? visibleRange.startIndex * estimatedRowHeight : 0;
  const bottomSpacerHeight = shouldVirtualize
    ? Math.max(0, (filteredRows.length - visibleRange.endIndex) * estimatedRowHeight)
    : 0;

  const tableBodyContent = useMemo(() => {
    if (filteredRows.length === 0) {
      return (
        <tr>
          <td colSpan={columns.length + (showRowActions ? 1 : 0)}>{emptyMessage}</td>
        </tr>
      );
    }

    return (
      <>
        {topSpacerHeight > 0 ? (
          <tr className="data-table__spacer-row">
            <td
              colSpan={columns.length + (showRowActions ? 1 : 0)}
              style={{ height: `${topSpacerHeight}px` }}
            />
          </tr>
        ) : null}
        {displayedRows.map(({ row, index }, displayedOffset) => {
          const filteredIndex = shouldVirtualize
            ? visibleRange.startIndex + displayedOffset
            : displayedOffset;
          const selectedStart = selectionRange
            ? Math.min(selectionRange.startIndex, selectionRange.endIndex)
            : -1;
          const selectedEnd = selectionRange
            ? Math.max(selectionRange.startIndex, selectionRange.endIndex)
            : -1;

          return (
          <tr
            key={`${row.id ?? index}-${index}`}
            className={getRowClassName?.(row, index)}
            onMouseEnter={() => {
              if (!isSelectionEnabled || !selectionDragging) {
                return;
              }

              const anchor = selectionAnchorRef.current;
              if (!anchor) {
                return;
              }

              pendingSelectionRef.current = {
                columnKey: anchor.columnKey,
                startIndex: anchor.startIndex,
                endIndex: filteredIndex,
              };

              if (selectionFrameRef.current !== null) {
                return;
              }

              selectionFrameRef.current = requestAnimationFrame(() => {
                selectionFrameRef.current = null;
                const pending = pendingSelectionRef.current;
                if (!pending) {
                  return;
                }
                setSelectionRange((previous) => {
                  if (
                    previous &&
                    previous.columnKey === pending.columnKey &&
                    previous.startIndex === pending.startIndex &&
                    previous.endIndex === pending.endIndex
                  ) {
                    return previous;
                  }

                  return pending;
                });
              });
            }}
          >
            {columns.map((column) => {
              const isSelectedCell =
                Boolean(selectionRange) &&
                selectionRange?.columnKey === column.key &&
                filteredIndex >= selectedStart &&
                filteredIndex <= selectedEnd;

              return (
              <td
                key={`${row.id ?? index}-${column.key}`}
                className={`${column.className ?? ""}${isSelectedCell ? " data-table__cell--selected" : ""}`.trim()}
                onMouseDown={(event) => {
                  if (!isSelectionEnabled || event.button !== 0) {
                    return;
                  }

                  if (
                    event.shiftKey &&
                    selectionRange &&
                    selectionRange.columnKey === column.key
                  ) {
                    setSelectionRange({
                      columnKey: column.key,
                      startIndex: selectionRange.startIndex,
                      endIndex: filteredIndex,
                    });
                    return;
                  }

                  setSelectionRange({
                    columnKey: column.key,
                    startIndex: filteredIndex,
                    endIndex: filteredIndex,
                  });
                  selectionAnchorRef.current = {
                    columnKey: column.key,
                    startIndex: filteredIndex,
                  };
                  setSelectionDragging(true);
                  event.preventDefault();
                }}
              >
                {editable && onRowsChange && !nonEditableColumns.includes(column.key) ? (
                  <input
                    className={compact ? "data-table__cell-input data-table__cell-input--compact" : "data-table__cell-input"}
                    type="text"
                    value={toCellText(row[column.key])}
                    onChange={(e) => {
                      const nextRows = [...rows];
                      nextRows[index] = {
                        ...nextRows[index],
                        [column.key]: e.target.value,
                      };
                      onRowsChange(nextRows);
                    }}
                    style={{ width: "100%" }}
                  />
                ) : (
                  formatCellValue(row[column.key], column.key)
                )}
              </td>
            )})}
            {showRowActions ? (
              <td>
                <button
                  type="button"
                  className={compact ? "data-table__row-delete data-table__row-delete--compact" : "data-table__row-delete"}
                  onClick={() => onDeleteRow(index)}
                >
                  Delete
                </button>
              </td>
            ) : null}
          </tr>
        )})}
        {bottomSpacerHeight > 0 ? (
          <tr className="data-table__spacer-row">
            <td
              colSpan={columns.length + (showRowActions ? 1 : 0)}
              style={{ height: `${bottomSpacerHeight}px` }}
            />
          </tr>
        ) : null}
      </>
    );
  }, [
    bottomSpacerHeight,
    columns,
    compact,
    displayedRows,
    editable,
    emptyMessage,
    filteredRows.length,
    getRowClassName,
    isSelectionEnabled,
    selectionDragging,
    selectionRange,
    nonEditableColumns,
    onDeleteRow,
    onRowsChange,
    rows,
    showRowActions,
    shouldVirtualize,
    topSpacerHeight,
    visibleRange.startIndex,
  ]);

  return (
    <div className="data-table-shell">
      <div className="data-table__summary">
        <div className="data-table__summary-chip">
          Rows: <strong>{filteredRows.length}</strong> / {rows.length}
        </div>
        {summaryItems.map((item) => (
          <div key={item.key} className="data-table__summary-chip">
            {item.label}:{" "}
            <strong>
              {formatNumericDisplayValue(
                item.key === "before_after_difference" ? Math.round(item.sum) : item.sum,
                getSummaryFractionDigits(item.key, item.sum),
              )}
            </strong>
          </div>
        ))}
        {selectedRangeStats ? (
          <div className="data-table__summary-chip data-table__summary-chip--selection">
            Selection [{selectedRangeStats.columnLabel}]{" "}
            <strong>Count: {selectedRangeStats.rowCount}</strong>
            {selectedRangeStats.numericCount > 0 ? (
              <>
                {" "}
                | Sum:{" "}
                <strong>
                  {formatNumericDisplayValue(
                    selectedRangeStats.sum,
                    getSummaryFractionDigits(selectedRangeStats.columnKey, selectedRangeStats.sum),
                  )}
                </strong>{" "}
                | Avg:{" "}
                <strong>
                  {formatNumericDisplayValue(
                    selectedRangeStats.average ?? 0,
                    getSummaryFractionDigits(
                      selectedRangeStats.columnKey,
                      selectedRangeStats.average ?? 0,
                    ),
                  )}
                </strong>
              </>
            ) : (
              <>
                {" "}
                | Numeric: <strong>0</strong>
              </>
            )}
          </div>
        ) : null}
      </div>
      <div
        ref={tableWrapperRef}
        className={`table-wrapper${selectionDragging ? " table-wrapper--selecting" : ""}`}
        style={maxHeight ? { maxHeight } : undefined}
        onScroll={(event) => {
          if (!shouldVirtualize) {
            return;
          }
          pendingScrollTopRef.current = event.currentTarget.scrollTop;
          if (scrollFrameRef.current !== null) {
            return;
          }
          scrollFrameRef.current = requestAnimationFrame(() => {
            scrollFrameRef.current = null;
            setScrollTop(pendingScrollTopRef.current);
          });
        }}
      >
        <table className={`data-table${compact ? " data-table--compact" : ""}`}>
          <thead>
            <tr>
              {columns.map((column) => (
                <th key={column.key} className={column.className}>
                  {column.label ?? column.key}
                </th>
              ))}
              {showRowActions ? <th>Actions</th> : null}
            </tr>
            <tr>
              {columns.map((column) => {
                const isFilterable = !editable && column.filterable !== false;
                if (!isFilterable) {
                  return <th key={`${column.key}-filter`} className={column.className} />;
                }

                return (
                  <th key={`${column.key}-filter`} className={column.className}>
                    <div
                      className="data-table__filter-menu"
                      ref={(node) => {
                        filterMenuRefs.current[column.key] = node;
                      }}
                    >
                      <button
                        type="button"
                        className={
                          compact
                            ? "data-table__filter-select data-table__filter-select--compact data-table__filter-summary"
                            : "data-table__filter-select data-table__filter-summary"
                        }
                        onClick={() =>
                          setOpenFilterKey((prev) => (prev === column.key ? null : column.key))
                        }
                      >
                        {getFilterSummaryLabel(filters[column.key] ?? [])}
                      </button>
                      {openFilterKey === column.key ? (
                        <div
                          className="data-table__filter-panel"
                          onMouseDown={(event) => event.stopPropagation()}
                          onClick={(event) => event.stopPropagation()}
                        >
                          <div className="data-table__filter-actions">
                            <button
                              type="button"
                              className="data-table__filter-clear"
                              onClick={() =>
                                setFilters((prev) => ({
                                  ...prev,
                                  [column.key]: filterOptions[column.key].map((value) =>
                                    value === "" ? EMPTY_FILTER_VALUE : value
                                  ),
                                }))
                              }
                            >
                              Select All
                            </button>
                            <button
                              type="button"
                              className="data-table__filter-clear"
                              onClick={() =>
                                setFilters((prev) => ({
                                  ...prev,
                                  [column.key]: [],
                                }))
                              }
                            >
                              Clear
                            </button>
                            <button
                              type="button"
                              className="data-table__filter-done"
                              onClick={() => setOpenFilterKey(null)}
                            >
                              Done
                            </button>
                          </div>
                          <div className="data-table__filter-options">
                            {filterOptions[column.key].map((value) => {
                              const optionValue = value === "" ? EMPTY_FILTER_VALUE : value;
                              const selectedValues = filters[column.key] ?? [];
                              const checked = selectedValues.includes(optionValue);

                              return (
                                <label
                                  key={`${column.key}-${value}`}
                                  className="data-table__filter-option"
                                >
                                  <input
                                    type="checkbox"
                                    checked={checked}
                                    onChange={(e) =>
                                      setFilters((prev) => {
                                        const currentValues = prev[column.key] ?? [];
                                        const nextValues = e.target.checked
                                          ? [...currentValues, optionValue]
                                          : currentValues.filter((item) => item !== optionValue);

                                        return {
                                          ...prev,
                                          [column.key]: nextValues,
                                        };
                                      })
                                    }
                                  />
                                  <span>{value || "(empty)"}</span>
                                </label>
                              );
                            })}
                          </div>
                        </div>
                      ) : null}
                    </div>
                  </th>
                );
              })}
              {showRowActions ? <th /> : null}
            </tr>
          </thead>
          <tbody>{tableBodyContent}</tbody>
        </table>
      </div>
    </div>
  );
}

export default FilterableTable;
