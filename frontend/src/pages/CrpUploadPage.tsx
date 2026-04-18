import UploadForm from "../components/upload/UploadForm";

const VOLVO_SALE_COLUMNS = [
  "Calendar",
  "Region",
  "Market",
  "Country",
  "Machine",
  "Machine Line",
  "Size Class",
  "Brand Owner code",
  "Brand Owner",
  "Brand",
  "Brand Nationality",
  "Source",
  "FID",
];

function CrpUploadPage() {
  return (
    <div className="page">
      <section className="section">
        <div className="section-header">
          <p className="section-tag">CRP Data</p>
          <h2 className="section-title">Upload CRP Data</h2>
          <p className="section-description">
            This is a dedicated page for CRP data upload and validation.
          </p>
        </div>

        <div className="matrix-form">
          <UploadForm label="volvo_sale_data" title="Volvo Sale Data CSV" />
          <UploadForm label="tma_data" title="TMA Data CSV" />
        </div>

        <div className="crp-columns">
          <h3 className="crp-columns__title">Required Columns</h3>
          <p className="crp-columns__description">
            Upload file should contain these columns in order:
          </p>
          <div className="crp-columns__chips">
            {VOLVO_SALE_COLUMNS.map((column) => (
              <span key={column} className="crp-columns__chip">
                {column}
              </span>
            ))}
          </div>
        </div>
      </section>
    </div>
  );
}

export default CrpUploadPage;
