import UploadForm from "../upload/UploadForm";

function MatrixForm() {
  return (
    <div className="matrix-form">
      <UploadForm
        label="source_matrix"
        title="Source Matrix CSV"
        compact
      />

      <UploadForm
        label="reporter_list"
        title="Reporter List CSV"
        compact
      />

      <UploadForm
        label="size_class"
        title="Size Class CSV"
        compact
      />

      <UploadForm
        label="brand_mapping"
        title="Brand Mapping CSV"
        compact
      />

      <UploadForm
        label="group_country"
        title="Group Country CSV"
        compact
      />

      <UploadForm
        label="machine_line_mapping"
        title="Machine Line Mapping CSV"
        compact
      />

    </div>
  );
}

export default MatrixForm;
