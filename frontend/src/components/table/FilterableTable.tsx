import { useEffect, useMemo, useState } from "react";

export type FilterableColumn = {
  key: string;
  label?: string;
  filterable?: boolean;
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
  onFiltersChange?: (filters: Record<string, string>) => void;
  onFilteredRowsChange?: (rows: Array<Record<string, string | number | null>>) => void;
  nonEditableColumns?: string[];
  compact?: boolean;
  getRowClassName?: (row: Record<string, string | number | null>, index: number) => string | undefined;
};

const EMPTY_FILTER_VALUE = "__EMPTY_FILTER__";

function toCellText(value: string | number | null | undefined): string {
  if (value === null || value === undefined) {
    return "";
  }
  return String(value);
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
}: FilterableTableProps) {
  const [filters, setFilters] = useState<Record<string, string>>({});
  const showRowActions = editable && typeof onDeleteRow === "function";
  const normalizedFilters = useMemo(() => {
    const next: Record<string, string> = {};
    Object.entries(filters).forEach(([key, value]) => {
      next[key] = value === EMPTY_FILTER_VALUE ? "" : value;
    });
    return next;
  }, [filters]);

  useEffect(() => {
    setFilters({});
  }, [resetToken]);

  useEffect(() => {
    if (onFiltersChange) {
      onFiltersChange(normalizedFilters);
    }
  }, [normalizedFilters, onFiltersChange]);

  const filterOptions = useMemo(() => {
    const options: Record<string, string[]> = {};

    columns.forEach((column) => {
      const uniqueValues = new Set<string>();
      rows.forEach((row) => {
        uniqueValues.add(toCellText(row[column.key]));
      });
      options[column.key] = Array.from(uniqueValues).sort((a, b) => a.localeCompare(b));
    });

    return options;
  }, [columns, rows]);

  const filteredRows = useMemo(() => {
    return rows
      .map((row, index) => ({ row, index }))
      .filter(({ row }) =>
      columns.every((column) => {
        const selected = filters[column.key] ?? "";
        if (!selected) {
          return true;
        }
        const cellText = toCellText(row[column.key]);
        if (selected === EMPTY_FILTER_VALUE) {
          return cellText === "";
        }
        return cellText === selected;
      })
    );
  }, [columns, filters, rows]);

  useEffect(() => {
    if (onFilteredRowsChange) {
      onFilteredRowsChange(filteredRows.map((item) => item.row));
    }
  }, [filteredRows, onFilteredRowsChange]);

  return (
    <div className="table-wrapper" style={maxHeight ? { maxHeight } : undefined}>
      <table className={`data-table${compact ? " data-table--compact" : ""}`}>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column.key}>{column.label ?? column.key}</th>
            ))}
            {showRowActions ? <th>Actions</th> : null}
          </tr>
          <tr>
            {columns.map((column) => {
              const isFilterable = column.filterable !== false;
              if (!isFilterable) {
                return <th key={`${column.key}-filter`} />;
              }

              return (
                <th key={`${column.key}-filter`}>
                  <select
                    className={compact ? "data-table__filter-select data-table__filter-select--compact" : "data-table__filter-select"}
                    value={filters[column.key] ?? ""}
                    onChange={(e) =>
                      setFilters((prev) => ({
                        ...prev,
                        [column.key]: e.target.value,
                      }))
                    }
                    style={{ width: "100%" }}
                  >
                    <option value="">All</option>
                    {filterOptions[column.key].map((value) => (
                      <option
                        key={`${column.key}-${value}`}
                        value={value === "" ? EMPTY_FILTER_VALUE : value}
                      >
                        {value || "(empty)"}
                      </option>
                    ))}
                  </select>
                </th>
              );
            })}
            {showRowActions ? <th /> : null}
          </tr>
        </thead>
        <tbody>
          {filteredRows.length === 0 ? (
            <tr>
              <td colSpan={columns.length + (showRowActions ? 1 : 0)}>{emptyMessage}</td>
            </tr>
          ) : (
            filteredRows.map(({ row, index }) => (
              <tr
                key={`${row.id ?? index}-${index}`}
                className={getRowClassName?.(row, index)}
              >
                {columns.map((column) => (
                  <td key={`${row.id ?? index}-${column.key}`}>
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
                      toCellText(row[column.key])
                    )}
                  </td>
                ))}
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
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

export default FilterableTable;
