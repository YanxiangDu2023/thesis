import UploadForm from "../upload/UploadForm";

function MatrixForm() {
  return (
    <div className="matrix-form">

      <UploadForm
        label="source_matrix"
        title="Source Matrix CSV"
      />

      <UploadForm
        label="reporter_list"
        title="Reporter List CSV"
      />

      <UploadForm
        label="size_class"
        title="Size Class CSV"
      />

      <UploadForm
        label="brand_mapping"
        title="Brand Mapping CSV"
      />

      <UploadForm
        label="group_country"
        title="Group Country CSV"
      />

      <UploadForm
        label="machine_line_mapping"
        title="Machine Line Mapping CSV"
      />

    </div>
  );
}

export default MatrixForm;
