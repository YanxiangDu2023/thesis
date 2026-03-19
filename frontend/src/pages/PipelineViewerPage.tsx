
import { useMemo, useState } from "react";

type SampleRow = {
  countryGroup: string;
  machineLine: string;
  brand: string;
  sales: number;
  totalMarket: number;
};

type Step = {
  code: string;
  name: string;
  description: string;
  records: number;
  status: "Ready" | "Pending";
  sampleRows: SampleRow[];
};

const steps: Step[] = [
  {
    code: "P00",
    name: "Preparation Raw Layer",
    description:
      "Raw source-aligned market and sales records before structural processing.",
    records: 1280,
    status: "Ready",
    sampleRows: [
      { countryGroup: "CEE", machineLine: "Excavators", brand: "Volvo", sales: 220, totalMarket: 840 },
      { countryGroup: "CEE", machineLine: "Wheel Loaders", brand: "CAT", sales: 180, totalMarket: 840 },
      { countryGroup: "MAC", machineLine: "Excavators", brand: "Komatsu", sales: 145, totalMarket: 510 },
      { countryGroup: "INA", machineLine: "Haulers", brand: "Volvo", sales: 76, totalMarket: 190 },
    ],
  },
  {
    code: "P10",
    name: "Prepared Layer",
    description:
      "Prepared output after initial logic, copy rules, and data structuring.",
    records: 1154,
    status: "Ready",
    sampleRows: [
      { countryGroup: "CEE", machineLine: "Excavators", brand: "Volvo", sales: 208, totalMarket: 810 },
      { countryGroup: "CEE", machineLine: "Wheel Loaders", brand: "CAT", sales: 174, totalMarket: 810 },
      { countryGroup: "MAC", machineLine: "Excavators", brand: "Komatsu", sales: 139, totalMarket: 498 },
      { countryGroup: "INA", machineLine: "Haulers", brand: "Volvo", sales: 74, totalMarket: 186 },
    ],
  },
  {
    code: "A00",
    name: "Adjustment Layer",
    description:
      "Intermediate adjusted output produced after selected business rules.",
    records: 1092,
    status: "Ready",
    sampleRows: [
      { countryGroup: "CEE", machineLine: "Excavators", brand: "Volvo", sales: 210, totalMarket: 808 },
      { countryGroup: "CEE", machineLine: "Wheel Loaders", brand: "CAT", sales: 170, totalMarket: 808 },
      { countryGroup: "MAC", machineLine: "Excavators", brand: "Komatsu", sales: 136, totalMarket: 492 },
      { countryGroup: "INA", machineLine: "Haulers", brand: "Volvo", sales: 72, totalMarket: 182 },
    ],
  },
  {
    code: "A20",
    name: "Final Adjustment Layer",
    description:
      "Latest adjusted result prepared for downstream analysis and review.",
    records: 1088,
    status: "Pending",
    sampleRows: [
      { countryGroup: "CEE", machineLine: "Excavators", brand: "Volvo", sales: 207, totalMarket: 800 },
      { countryGroup: "CEE", machineLine: "Wheel Loaders", brand: "CAT", sales: 169, totalMarket: 800 },
      { countryGroup: "MAC", machineLine: "Excavators", brand: "Komatsu", sales: 134, totalMarket: 488 },
      { countryGroup: "INA", machineLine: "Haulers", brand: "Volvo", sales: 71, totalMarket: 180 },
    ],
  },
];

function PipelineViewerPage() {
  const [selectedStepCode, setSelectedStepCode] = useState<string | null>(null);

  const selectedStep = useMemo(
    () => steps.find((step) => step.code === selectedStepCode) ?? null,
    [selectedStepCode]
  );

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
                <button
                  type="button"
                  key={step.code}
                  className={`step-item ${
                    step.code === selectedStep?.code ? "step-item--active" : ""
                  }`}
                  onClick={() => setSelectedStepCode(step.code)}
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
                </button>
              ))}
            </div>
          </aside>

          <section className="panel">
            <h3 className="panel__title">Step Summary</h3>
            {selectedStep ? (
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
            ) : (
              <p className="summary-description">
                Select a step from the left list to display its summary.
              </p>
            )}
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

        {selectedStep ? (
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
                {selectedStep.sampleRows.map((row, index) => (
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
        ) : (
          <p className="summary-description">No data to preview yet. Select a step first.</p>
        )}
      </section>
    </div>
  );
}

export default PipelineViewerPage;
