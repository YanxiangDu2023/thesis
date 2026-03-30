import { Link } from "react-router-dom";

type LayerCard = {
  code: string;
  title: string;
  description?: string;
  bullets?: string[];
};

const layers: LayerCard[] = [
  {
    code: "P00",
    title: "Preparation Raw Layer",
    bullets: [
      "For CRP data:",
      "Merge latest TMA + SAL, map country fields, and assign reporter/deletion flags.",
      "For OTH data: mark Deletion flag = Y when Machine Line Code = 390, or when Country + Machine Line Name is not found in Source Matrix.",
    ],
  },
  {
    code: "P10",
    title: "Prepared Layer",
    bullets: [
      "Display TMA (Total Market) records at prepared layer granularity.",
      "Calculate Volvo CE (VCE) from Volvo/SAL rows that have a non-empty CRP Source in Source Matrix for the matched Country + Machine Line Name, exclude Deletion flag = Y, and exclude Motor Graders.",
      "Compute Non-Volvo CE as max(TMA - VCE, 0) for downstream steps.",
    ],
  },
  {
    code: "A10",
    title: "Adjustment Layer",
    description:
      "Captures adjusted values after business rules and allocation logic are applied.",
  },
  {
    code: "A20",
    title: "Final Adjustment Layer",
    description:
      "Represents a more finalized adjusted result for downstream review and analysis.",
  },
];

function HomePage() {
  return (
    <div className="page">
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
              <Link to="/matrix" className="btn btn--secondary">
                Submit Matrix
              </Link>
            </div>
          </div>
        </div>
      </section>

      <section className="section">
        <div className="section-header">
          <p className="section-tag">Overview</p>
          <h3 className="section-title">What this prototype aims to do</h3>
          <p className="section-description">
            This first version focuses on showing hidden SAP calculation logic in
            a clearer interface, while leaving room for future matrix submission
            and run management.
          </p>
        </div>

        <div className="card-grid card-grid--three">
          <article className="card">
            <h4 className="card__title">Transparent Pipeline Steps</h4>
            <p className="card__text">
              Show hidden SAP calculation layers step by step instead of keeping
              the process as a black box.
            </p>
          </article>

          <article className="card">
            <h4 className="card__title">Matrix Submission Entry</h4>
            <p className="card__text">
              Reserve a place for future matrix upload and maintenance, including
              source rules and business input structures.
            </p>
          </article>

          <article className="card">
            <h4 className="card__title">Run Tracking</h4>
            <p className="card__text">
              Prepare for future run setup, monitoring, and result comparison
              across different calculation scenarios.
            </p>
          </article>
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
          <p className="section-tag">Calculation Layers</p>
          <h3 className="section-title">Initial scope for the first prototype</h3>
          <p className="section-description">
            The current thesis prototype focuses on the preparation and adjustment
            stages that are most realistic to implement first.
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
          <p className="section-tag">Pipeline Overview</p>
          <h3 className="section-title">Current visualized flow</h3>
          <p className="section-description">
            The current frontend mockup illustrates a simplified progression from
            preparation to adjustment.
          </p>
        </div>

        <div className="pipeline-flow">
          <div className="pipeline-node">P00</div>
          <div className="pipeline-arrow">{"\u2192"}</div>
          <div className="pipeline-node">P10</div>
          <div className="pipeline-arrow">{"\u2192"}</div>
          <div className="pipeline-node">A10</div>
          <div className="pipeline-arrow">{"\u2192"}</div>
          <div className="pipeline-node">A20</div>
        </div>
      </section>
    </div>
  );
}

export default HomePage;
