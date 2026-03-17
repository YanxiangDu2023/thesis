import { useMemo, useState } from "react";

export type FilterableColumn = {
  key: string;
  label?: string;
  filterable?: boolean;
};

type FilterableTableProps = {
  columns: FilterableColumn[];
  rows: Array<Record<string, string | number | null | undefined>>;
  maxHeight?: string;
  emptyMessage?: string;
};

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
}: FilterableTableProps) {
  const [filters, setFilters] = useState<Record<string, string>>({});

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
    return rows.filter((row) =>
      columns.every((column) => {
        const selected = filters[column.key] ?? "";
        if (!selected) {
          return true;
        }
        return toCellText(row[column.key]) === selected;
      })
    );
  }, [columns, filters, rows]);

  return (
    <div className="table-wrapper" style={maxHeight ? { maxHeight } : undefined}>
      <table className="data-table">
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column.key}>{column.label ?? column.key}</th>
            ))}
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
                      <option key={`${column.key}-${value}`} value={value}>
                        {value || "(empty)"}
                      </option>
                    ))}
                  </select>
                </th>
              );
            })}
          </tr>
        </thead>
        <tbody>
          {filteredRows.length === 0 ? (
            <tr>
              <td colSpan={columns.length}>{emptyMessage}</td>
            </tr>
          ) : (
            filteredRows.map((row, rowIndex) => (
              <tr key={`${row.id ?? rowIndex}-${rowIndex}`}>
                {columns.map((column) => (
                  <td key={`${row.id ?? rowIndex}-${column.key}`}>{toCellText(row[column.key])}</td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}

export default FilterableTable;
