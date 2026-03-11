import MatrixForm from "../components/matrix/MatrixForm";

function MatrixSubmissionPage() {
  return (
    <div className="page-container">
      <h1>Matrix Submission</h1>
      <p>Upload the CSV files required for matrix configuration.</p>
      <MatrixForm />
    </div>
  );
}

export default MatrixSubmissionPage;