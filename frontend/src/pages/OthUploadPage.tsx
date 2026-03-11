import UploadForm from "../components/upload/UploadForm";

function OthUploadPage() {
  return (
    <div className="page-container">
      <h1>Upload OTH Data</h1>
      <p>Upload the CSV file required for OTH data configuration.</p>
      <div className="matrix-form">
        <UploadForm label="oth_data" title="OTH Data CSV" />
      </div>
    </div>
  );
}

export default OthUploadPage;
