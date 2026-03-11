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

    </div>
  );
}

export default MatrixForm;