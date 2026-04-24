import { Link } from "react-router-dom";

type LayerCard = {
  code: string;
  title: string;
  description?: string;
  bullets?: string[];
};

type TimelineStep = {
  code: string;
  title: string;
  description: string;
};

const layers: LayerCard[] = [
  {
    code: "P00",
    title: "Preparation Raw Layer",
    bullets: [
      "Merge latest TMA + SAL and prepare CRP fields.",
      "Prepare OTH flags and source matching.",
    ],
  },
  {
    code: "P10",
    title: "Prepared Layer",
    bullets: [
      "Show prepared TMA values.",
      "Calculate VCE from valid Volvo/SAL rows.",
      "Compute Non-Volvo CE as max(TMA - VCE, 0).",
    ],
  },
  {
    code: "A10",
    title: "Adjustment Layer",
    description:
      "Summarizes prepared SAL and TMA rows into reviewable A10 result rows before later split logic.",
  },
];

const machineLineSplitCard: LayerCard = {
  code: "MLS",
  title: "Machine Line Split Layer",
  bullets: [
    "Apply machine line split logic to the reviewed adjustment output.",
    "Prepare split-ready result rows for downstream analysis and follow-up review.",
  ],
};

const totalMarketCalculationCard: LayerCard = {
  code: "TMC",
  title: "Total Market Calculation",
  bullets: [
    "Handle double-brand deletion cases before market aggregation.",
    "Build total market values from FID, using primary and secondary sources when available.",
  ],
};

const tmcTimeline: TimelineStep[] = [
  {
    code: "SET",
    title: "Input Setup",
    description: "Maintain matrix rules and load source data for the run.",
  },
  {
    code: "P00",
    title: "Raw Preparation",
    description: "Merge TMA, SAL, OTH, and CRP-ready fields into a clean base.",
  },
  {
    code: "P10",
    title: "Prepared Output",
    description: "Calculate prepared market values, VCE, and non-Volvo CE.",
  },
  {
    code: "A10",
    title: "Adjustment Build",
    description: "Group prepared records into adjustment-ready result rows.",
  },
  {
    code: "SPL",
    title: "Machine Line Split",
    description: "Split adjusted results into machine-line level output.",
  },
  {
    code: "TMC",
    title: "Total Market Calculation",
    description: "Start the core TMC process and define the calculation scope.",
  },
  {
    code: "RES",
    title: "Restatement",
    description: "Restate the split result for reporting consistency.",
  },
  {
    code: "RPT",
    title: "Reporting",
    description: "Publish the final numbers for business review and reporting.",
  },
];

function HomePage() {
  return (
    <div className="page page--home-wide">
      <section className="hero">
        <div className="hero__background-photo" aria-hidden="true" />
        <div className="hero__layout">
          <div className="hero__content">
            <div className="hero__brand">
              <img
                src="/volvo_construction_equipment_logo.jpg"
                alt="Volvo logo"
                className="brand-image-logo brand-image-logo--hero"
              />

              <div className="hero__brand-copy">
                <p className="hero__brand-name">Volvo Construction Equipment</p>
                <p className="hero__brand-subtitle">TMC Process Visualizer</p>
              </div>
            </div>

            <p className="section-tag">Volvo CE Thesis Prototype</p>
            <h2 className="hero__title">Visualizing the TMC Calculation Workflow</h2>
            <p className="hero__text">
              A more industrial, Volvo CE-inspired interface for exposing hidden SAP
              calculation steps, intermediate layers, and operational logic in one
              navigable workspace.
            </p>

            <div className="hero__actions">
              <Link to="/pipeline" className="btn btn--primary">
                Open Pipeline
              </Link>
            </div>
          </div>
        </div>
      </section>

      <section className="section">
        <div className="section-header">
          <p className="section-tag">Overview</p>
          <h3 className="section-title">What this prototype aims to do</h3>
          {/* <p className="section-description">
            This first version focuses on showing hidden SAP calculation logic in
            a clearer interface, while leaving room for future matrix submission
            and run management.
          </p> */}
        </div>

        <div className="card-grid card-grid--three">
          <article className="card">
            <h4 className="card__title">Transparent Pipeline Steps</h4>
            <p className="card__text">
              Show SAP calculation layers step by step.
            </p>
          </article>

          <article className="card">
            <h4 className="card__title">Matrix Submission Entry</h4>
            <p className="card__text">
              Future entry for matrix upload and maintenance.
            </p>
          </article>

          <article className="card">
            <h4 className="card__title">Run Tracking</h4>
            <p className="card__text">
              Future support for run setup and tracking.
            </p>
          </article>
        </div>

        <div className="tmc-timeline">
          <div className="tmc-timeline__header">
            <p className="tmc-timeline__eyebrow">TMC sequence</p>
            <p className="tmc-timeline__intro">
              A compact view of how the workflow moves from setup to reporting.
            </p>
          </div>

          <div className="tmc-timeline__scroller">
            <div className="tmc-timeline__track">
              {tmcTimeline.map((step) => (
                <article key={step.code} className="tmc-timeline__step">
                  <div className="tmc-timeline__marker">
                    <span className="tmc-timeline__dot" aria-hidden="true" />
                    <span className="tmc-timeline__code">{step.code}</span>
                  </div>
                  <div className="tmc-timeline__card">
                    <h4 className="tmc-timeline__title">{step.title}</h4>
                    <p className="tmc-timeline__text">{step.description}</p>
                  </div>
                </article>
              ))}
            </div>
          </div>
        </div>
      </section>

      <section className="section">
        <div className="section-header">
          <p className="section-tag">Master Data</p>
          <h3 className="section-title">Master Data Maintenance & Upload Entry</h3>
          <p className="section-description">
            Jump directly to pipeline review and master data upload pages for maintenance tasks.
          </p>
        </div>

        <div className="overview-actions">
          <div className="overview-actions__buttons">
            <Link to="/pipeline" className="btn btn--overview">
              View Pipeline
            </Link>
            <Link to="/matrix" className="btn btn--overview">
              Submit Matrix
            </Link>
            <Link to="/upload/oth" className="btn btn--overview">
              Upload OTH data
            </Link>
            <Link to="/upload/crp" className="btn btn--overview">
              Upload CRP data
            </Link>
          </div>
        </div>
      </section>

      <section className="section">
        <div className="section-header">
          <p className="section-tag">Preparation AND Adjustment Layers</p>
          <h3 className="section-title">Initial scope for the first prototype</h3>
          <p className="section-description">
            The current thesis prototype focuses on the preparation and adjustment
            stages.
          </p>
        </div>

        <div className="card-grid card-grid--four">
          {layers.map((layer) => (
            <Link key={layer.code} to={`/layers/${layer.code}`} className="layer-card-link">
              <article className="card layer-card">
                <span className="layer-card__code">{layer.code}</span>
                <h4 className="card__title">{layer.title}</h4>
                {layer.bullets ? (
                  <ul className="card__list">
                    {layer.bullets.map((item) => (
                      <li key={item}>{item}</li>
                    ))}
                  </ul>
                ) : (
                  <p className="card__text">{layer.description}</p>
                )}
              </article>
            </Link>
          ))}
        </div>
      </section>

      <section className="section">
        <div className="section-header">
          <p className="section-tag">Machine Line Split</p>
          <h3 className="section-title">Machine Line Split</h3>
          <p className="section-description">
            The next planned scope introduces machine line split logic after the
            current adjustment flow.
          </p>
        </div>

        <div className="card-grid card-grid--three">
          <Link to={`/layers/${machineLineSplitCard.code}`} className="layer-card-link">
            <article className="card layer-card">
              <span className="layer-card__code">{machineLineSplitCard.code}</span>
              <h4 className="card__title">{machineLineSplitCard.title}</h4>
              <ul className="card__list">
                {machineLineSplitCard.bullets?.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </article>
          </Link>
        </div>
      </section>

      <section className="section">
        <div className="section-header">
          <p className="section-tag">Total Market Calculation</p>
          <h3 className="section-title">Total Market Calculation</h3>
          <p className="section-description">
            Resolve double-brand logic first, then calculate the total market and
            prepare the final F00 output.
          </p>
        </div>

        <div className="card-grid card-grid--three">
          <Link to="/total-market-calculation" className="layer-card-link">
            <article className="card layer-card">
              <span className="layer-card__code">{totalMarketCalculationCard.code}</span>
              <h4 className="card__title">{totalMarketCalculationCard.title}</h4>
              <ul className="card__list">
                {totalMarketCalculationCard.bullets?.map((item) => (
                  <li key={item}>{item}</li>
                ))}
              </ul>
            </article>
          </Link>
        </div>

        <div className="overview-actions" style={{ marginTop: "20px" }}>
          <div className="overview-actions__buttons">
            <Link to="/total-market-calculation" className="btn btn--overview">
              Open Total Market Calculation
            </Link>
          </div>
        </div>
      </section>
    </div>
  );
}

export default HomePage;
