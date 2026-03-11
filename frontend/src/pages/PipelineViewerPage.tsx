
type Step = {
  code: string;
  name: string;
  description: string;
  records: number;
  status: "Ready" | "Pending";
};

const steps: Step[] = [
  {
    code: "P00",
    name: "Preparation Raw Layer",
    description:
      "Raw source-aligned market and sales records before structural processing.",
    records: 1280,
    status: "Ready",
  },
  {
    code: "P10",
    name: "Prepared Layer",
    description:
      "Prepared output after initial logic, copy rules, and data structuring.",
    records: 1154,
    status: "Ready",
  },
  {
    code: "A10",
    name: "Adjustment Layer",
    description:
      "Intermediate adjusted output produced after selected business rules.",
    records: 1092,
    status: "Ready",
  },
  {
    code: "A20",
    name: "Final Adjustment Layer",
    description:
      "Latest adjusted result prepared for downstream analysis and review.",
    records: 1088,
    status: "Pending",
  },
];

const sampleRows = [
  {
    countryGroup: "CEE",
    machineLine: "Excavators",
    brand: "Volvo",
    sales: 220,
    totalMarket: 840,
  },
  {
    countryGroup: "CEE",
    machineLine: "Wheel Loaders",
    brand: "CAT",
    sales: 180,
    totalMarket: 840,
  },
  {
    countryGroup: "MAC",
    machineLine: "Excavators",
    brand: "Komatsu",
    sales: 145,
    totalMarket: 510,
  },
  {
    countryGroup: "INA",
    machineLine: "Haulers",
    brand: "Volvo",
    sales: 76,
    totalMarket: 190,
  },
];

function PipelineViewerPage() {
  const selectedStep = steps[0];

  return (
    <div className="page">
      <section className="section">
        <div className="section-header">
          <p className="section-tag">Pipeline Viewer</p>
          <h2 className="section-title">Calculation Step Explorer</h2>
          <p className="section-description">
            This page is designed to display intermediate outputs and make the TMC
            calculation process easier to understand step by step.
          </p>
        </div>

        <div className="pipeline-layout">
          <aside className="panel">
            <h3 className="panel__title">Step List</h3>
            <div className="step-list">
              {steps.map((step) => (
                <div
                  key={step.code}
                  className={`step-item ${
                    step.code === selectedStep.code ? "step-item--active" : ""
                  }`}
                >
                  <div className="step-item__top">
                    <span className="step-item__code">{step.code}</span>
                    <span
                      className={`status-badge ${
                        step.status === "Ready"
                          ? "status-badge--ready"
                          : "status-badge--pending"
                      }`}
                    >
                      {step.status}
                    </span>
                  </div>
                  <p className="step-item__name">{step.name}</p>
                </div>
              ))}
            </div>
          </aside>

          <section className="panel">
            <h3 className="panel__title">Step Summary</h3>
            <div className="summary-card">
              <div className="summary-row">
                <span className="summary-label">Step</span>
                <span className="summary-value">{selectedStep.code}</span>
              </div>
              <div className="summary-row">
                <span className="summary-label">Name</span>
                <span className="summary-value">{selectedStep.name}</span>
              </div>
              <div className="summary-row">
                <span className="summary-label">Records</span>
                <span className="summary-value">{selectedStep.records}</span>
              </div>
              <div className="summary-row">
                <span className="summary-label">Status</span>
                <span className="summary-value">{selectedStep.status}</span>
              </div>

              <p className="summary-description">{selectedStep.description}</p>
            </div>
          </section>
        </div>
      </section>

      <section className="section">
        <div className="section-header">
          <p className="section-tag">Sample Output</p>
          <h3 className="section-title">Preview Table</h3>
          <p className="section-description">
            This preview uses placeholder data to demonstrate how intermediate
            outputs could be displayed for validation and review.
          </p>
        </div>

        <div className="table-wrapper">
          <table className="data-table">
            <thead>
              <tr>
                <th>Country Group</th>
                <th>Machine Line</th>
                <th>Brand</th>
                <th>Sales</th>
                <th>Total Market</th>
              </tr>
            </thead>
            <tbody>
              {sampleRows.map((row, index) => (
                <tr key={index}>
                  <td>{row.countryGroup}</td>
                  <td>{row.machineLine}</td>
                  <td>{row.brand}</td>
                  <td>{row.sales}</td>
                  <td>{row.totalMarket}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>
    </div>
  );
}

export default PipelineViewerPage;