import { useMemo, useState } from "react";
import { getLatestUploadByMatrixType, uploadCsv } from "../../api/uploads";
import type { UploadRow, UploadRun, UploadStatus } from "../../types/upload";
import FilterableTable from "../table/FilterableTable";

type UploadFormProps = {
  label: string;
  title: string;
};

const PREFERRED_COLUMN_ORDER: Record<string, string[]> = {
  oth_data: [
    "year",
    "source",
    "brand_name",
    "machine_line",
    "placeholder_1",
    "country",
    "placeholder_2",
    "size_class",
    "quantity",
  ],
  group_country: [
    "year",
    "country_grouping",
    "group_code",
    "country_code",
    "country_name",
    "market_area",
    "market_area_code",
    "region",
  ],
  volvo_sale_data: [
    "calendar",
    "region",
    "market",
    "country",
    "machine",
    "machine_line",
    "size_class",
    "brand_owner_code",
    "brand_owner",
    "brand",
    "brand_nationality",
    "source",
    "fid",
  ],
  tma_data: [
    "year",
    "geographical_region",
    "geographical_market_area",
    "end_country",
    "end_country_code",
    "machine_family",
    "machine_line",
    "machine_line_code",
    "size_class",
    "size_class_mapping",
    "total_market_fid_sales",
  ],
};

const BASE_HIDDEN_COLUMNS = ["id", "upload_run_id", "row_index"];

const HIDDEN_COLUMNS_BY_MATRIX_TYPE: Record<string, string[]> = {
  source_matrix: [...BASE_HIDDEN_COLUMNS],
  reporter_list: [...BASE_HIDDEN_COLUMNS],
  size_class: [...BASE_HIDDEN_COLUMNS],
  brand_mapping: [...BASE_HIDDEN_COLUMNS],
  group_country: [...BASE_HIDDEN_COLUMNS],
  machine_line_mapping: [...BASE_HIDDEN_COLUMNS],
  oth_data: [...BASE_HIDDEN_COLUMNS],
  volvo_sale_data: [...BASE_HIDDEN_COLUMNS, "brand_code_1", "brand_code_2", "brand_name"],
  tma_data: [...BASE_HIDDEN_COLUMNS],
};

const NON_FILTERABLE_COLUMNS_BY_MATRIX_TYPE: Record<string, string[]> = {
  oth_data: ["placeholder_1", "placeholder_2"],
};

const COLUMN_LABEL_OVERRIDES_BY_MATRIX_TYPE: Record<string, Record<string, string>> = {
  oth_data: {
    placeholder_1: "empty_1",
    placeholder_2: "empty_2",
  },
};

function getRowsForDisplay(label: string, rows: UploadRow[]): UploadRow[] {
  if (label !== "oth_data") {
    return rows;
  }

  return rows.map((row) => ({
    ...row,
    placeholder_1: "",
    placeholder_2: "",
  }));
}

function getVisibleColumns(label: string, rows: UploadRow[]): string[] {
  if (rows.length === 0) {
    return [];
  }

  const hiddenColumns = HIDDEN_COLUMNS_BY_MATRIX_TYPE[label] ?? [];
  const rowKeys = Object.keys(rows[0]).filter((column) => !hiddenColumns.includes(column));
  const preferredOrder = PREFERRED_COLUMN_ORDER[label] ?? [];
  const orderedPreferred = preferredOrder.filter((column) => rowKeys.includes(column));
  const remainingColumns = rowKeys.filter((column) => !orderedPreferred.includes(column));

  return [...orderedPreferred, ...remainingColumns];
}

function toCsvCell(value: string | number | null | undefined): string {
  const text = value === null || value === undefined ? "" : String(value);
  if (/[",\r\n]/.test(text)) {
    return `"${text.replace(/"/g, "\"\"")}"`;
  }
  return text;
}

function buildCsvContent(label: string, columns: string[], rows: UploadRow[]): string {
  const labelOverrides = COLUMN_LABEL_OVERRIDES_BY_MATRIX_TYPE[label] ?? {};
  const header = columns.map((column) => toCsvCell(labelOverrides[column] ?? column)).join(",");
  const dataLines = rows.map((row) => columns.map((column) => toCsvCell(row[column])).join(","));
  return [header, ...dataLines].join("\r\n");
}

function UploadForm({ label, title }: UploadFormProps) {
  const [file, setFile] = useState<File | null>(null);
  const [message, setMessage] = useState("");
  const [status, setStatus] = useState<UploadStatus>("idle");

  const [latestUpload, setLatestUpload] = useState<UploadRun | null>(null);
  const [latestRows, setLatestRows] = useState<UploadRow[]>([]);
  const [latestLoading, setLatestLoading] = useState(false);
  const [latestError, setLatestError] = useState("");
  const [showLatestPanel, setShowLatestPanel] = useState(false);
  const [downloadLoading, setDownloadLoading] = useState(false);

  const latestRowsForDisplay = useMemo(() => {
    return getRowsForDisplay(label, latestRows);
  }, [label, latestRows]);

  const latestColumns = useMemo(() => {
    return getVisibleColumns(label, latestRowsForDisplay);
  }, [label, latestRowsForDisplay]);

  const handleUpload = async () => {
    if (!file) {
      alert("Please select a CSV file first.");
      return;
    }

    try {
      setStatus("uploading");
      setMessage("Uploading...");

      const result = await uploadCsv(label, file);
      setStatus("success");
      setMessage(`Upload successful. Upload ID: ${result.upload_run_id}`);
    } catch (error) {
      console.error(error);
      setStatus("error");
      setMessage(error instanceof Error ? error.message : "Upload failed.");
    }
  };

  const handleShowLatest = async () => {
    try {
      setShowLatestPanel(true);
      setLatestLoading(true);
      setLatestError("");

      const result = await getLatestUploadByMatrixType(label);
      setLatestUpload(result.upload_run);
      setLatestRows(result.rows);
    } catch (error) {
      console.error(error);
      setLatestUpload(null);
      setLatestRows([]);
      setLatestError(error instanceof Error ? error.message : "Failed to load latest upload data.");
    } finally {
      setLatestLoading(false);
    }
  };

  const handleCloseLatest = () => {
    setShowLatestPanel(false);
    setLatestLoading(false);
    setLatestError("");
    setLatestUpload(null);
    setLatestRows([]);
  };

  const handleDownloadLatest = async () => {
    try {
      setDownloadLoading(true);
      setLatestError("");

      let uploadForDownload = latestUpload;
      let rowsForDownload = latestRows;

      if (!uploadForDownload || rowsForDownload.length === 0) {
        const result = await getLatestUploadByMatrixType(label);
        uploadForDownload = result.upload_run;
        rowsForDownload = result.rows;
        setLatestUpload(result.upload_run);
        setLatestRows(result.rows);
      }

      const rowsForDisplay = getRowsForDisplay(label, rowsForDownload);
      const columnsForDownload = getVisibleColumns(label, rowsForDisplay);

      if (columnsForDownload.length === 0) {
        throw new Error("No row data found in SQL for this upload.");
      }

      const csvContent = buildCsvContent(label, columnsForDownload, rowsForDisplay);
      const blob = new Blob(["\uFEFF", csvContent], { type: "text/csv;charset=utf-8;" });
      const objectUrl = URL.createObjectURL(blob);
      const fileName = `${label}_latest_${uploadForDownload?.id ?? "unknown"}.csv`;

      const downloadLink = document.createElement("a");
      downloadLink.href = objectUrl;
      downloadLink.download = fileName;
      document.body.appendChild(downloadLink);
      downloadLink.click();
      downloadLink.remove();
      URL.revokeObjectURL(objectUrl);
    } catch (error) {
      console.error(error);
      setLatestError(error instanceof Error ? error.message : "Failed to download latest upload data.");
    } finally {
      setDownloadLoading(false);
    }
  };

  return (
    <div className="upload-card">
      <h3>{title}</h3>

      <input
        type="file"
        accept=".csv"
        onChange={(e) => {
          const selectedFile = e.target.files?.[0] || null;
          setFile(selectedFile);
          setMessage("");
          setStatus("idle");
        }}
      />

      {file && <p>Selected file: {file.name}</p>}

      <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
        <button type="button" onClick={handleUpload}>
          Upload
        </button>
        <button type="button" onClick={handleShowLatest}>
          Show Latest
        </button>
        <button type="button" onClick={handleDownloadLatest} disabled={downloadLoading}>
          {downloadLoading ? "Downloading..." : "Download Latest"}
        </button>
      </div>

      {status === "uploading" && <p style={{ color: "blue" }}>Uploading...</p>}
      {status === "success" && <p style={{ color: "green" }}>Success: {message}</p>}
      {status === "error" && <p style={{ color: "red" }}>Error: {message}</p>}

      {showLatestPanel && (
        <div style={{ marginTop: "12px" }}>
          <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center" }}>
            <strong>Latest Data</strong>
            <button type="button" onClick={handleCloseLatest} aria-label="Close latest data" title="Close">
              x
            </button>
          </div>

          {latestLoading && <p style={{ color: "blue" }}>Loading latest upload data...</p>}
          {latestError && <p style={{ color: "red" }}>Error: {latestError}</p>}

          {latestUpload && (
            <>
              <p>
                Latest upload: ID {latestUpload.id}, rows {latestUpload.row_count ?? 0}, status{" "}
                {latestUpload.status ?? "unknown"}
              </p>

              {latestRows.length === 0 ? (
                <p>No row data found in SQL for this upload.</p>
              ) : (
                <FilterableTable
                  columns={latestColumns.map((column) => ({
                    key: column,
                    label: COLUMN_LABEL_OVERRIDES_BY_MATRIX_TYPE[label]?.[column] ?? column,
                    filterable: !((NON_FILTERABLE_COLUMNS_BY_MATRIX_TYPE[label] ?? []).includes(column)),
                  }))}
                  rows={latestRowsForDisplay}
                  maxHeight="360px"
                />
              )}
            </>
          )}
        </div>
      )}
    </div>
  );
}

export default UploadForm;
