# ERP-Based Market Reporting Workflow Visualizer 🚀

A full-stack prototype for making complex ERP-based market reporting and calculation workflows more transparent, traceable, and reviewable for business users.

This project was developed as part of a master’s thesis in Information Systems. It explores how complex enterprise reporting workflows can be transformed into a staged, inspectable web application where users can upload source data, maintain business-rule matrices, review intermediate outputs, and validate final calculation results.

> This repository is a thesis and portfolio prototype. It is not connected to any official enterprise system or production environment. Sensitive business data, credentials, and confidential documents should not be committed to this public repository.

---

## 1. Project Overview 🔍

Large enterprise reporting workflows often rely on ERP systems, spreadsheets, master-data matrices, and manually validated business rules. Although these workflows can be technically functional, their internal logic is often difficult for business users to inspect, validate, and explain.

This prototype addresses that problem by turning a complex market reporting workflow into a staged web application where users can:

* upload and maintain matrix/source files;
* inspect intermediate calculation layers;
* review deletion, reporter, source, and size-class flags;
* validate source-driven total market calculations;
* save edited snapshots;
* compare calculated outputs against control reports;
* follow the workflow from raw preparation to validation and reporting.

The goal is not to replace the ERP or reporting system, but to provide a business-facing inspection layer around complex calculation logic.

---

## 2. Core Use Case 🧩

The prototype models a market reporting workflow based on multiple source files, mapping tables, and rule-based calculation steps.

The workflow includes stages such as:

1. **Input Setup**
   Upload and maintain source data and matrix files.

2. **Raw Preparation**
   Combine source data and prepare flags for downstream calculation.

3. **Market View**
   Prepare market-level views and calculate source-based market values.

4. **Adjustment Layer**
   Aggregate and prepare adjustment-ready rows.

5. **Machine Line Split**
   Apply selected split logic for machine-line-level outputs.

6. **Total Market Calculation**
   Handle source-priority, duplicate-source, and double-brand cases before calculating total market output.

7. **Restatement**
   Rebalance outputs to maintain reporting consistency.

8. **Validation Report**
   Compare final outputs against control figures before business review.

---

## 3. Main Features ✨

### Data Upload and Matrix Maintenance

The application supports CSV upload and review for several matrix and source-data types, including:

* Source Matrix
* Reporter List
* Size Class Matrix
* Brand Mapping
* Group Country Mapping
* Machine Line Mapping
* External Market Data
* Internal Sales Data
* Total Market Reference Data

Uploaded rows are stored with upload-run metadata, planning year, upload status, row count, and original file information.

### Reviewable Intermediate Layers

Instead of showing only final outputs, the prototype exposes intermediate stages of the calculation workflow. This allows users to understand how source rows are transformed, filtered, matched, flagged, aggregated, and prepared for downstream reporting.

### Flag and Rule Calculation

The prototype includes logic for business-review fields such as:

* Deletion Flag
* Reporter Flag
* Primary/Secondary source indicator
* Size Class Flag
* Source Flag
* Double-brand checks
* Restatement validation values

### Editable Snapshots

Users can edit uploaded or calculated rows in the web interface and save them as new upload runs or calculation snapshots. This supports traceability between original uploaded data, edited versions, and generated outputs.

### Planning-Year-Based Runs

The application supports planning-year selection, allowing uploaded data and generated reports to be scoped to a selected year.

### Authentication and Site Protection

The backend supports:

* user registration;
* login/logout;
* bearer-token-based sessions;
* optional local development bypass;
* optional shared site password protection for temporary test deployments.

### Validation-Oriented UI

The frontend provides filterable tables, workflow pages, upload result pages, total market calculation views, restatement views, and validation-report pages for business inspection.

---

## 4. Tech Stack 🛠️

### Frontend

* React
* TypeScript
* Vite
* React Router
* CSS-based custom interface

### Backend

* Python
* FastAPI
* Uvicorn
* Pandas
* Pydantic
* Multipart file upload handling

### Database

* SQLite for local development
* PostgreSQL support for deployed environments

### Deployment

* Docker
* Google Cloud Run
* Google Artifact Registry
* GitHub Actions CI/CD for backend deployment

---

## 5. AI-Assisted Development 🤖

Claude Code was used as a development assistant during parts of the implementation process, mainly to support:

* code navigation and understanding across frontend/backend files;
* debugging and error analysis;
* refactoring suggestions;
* SQL and API logic review;
* documentation and README drafting support.

The system design, business logic interpretation, implementation decisions, validation work, and final review were carried out by the author. Claude Code was used as an assisting tool rather than as a replacement for engineering judgement or domain understanding.

---

## 6. Repository Structure

```text
thesis/
├── backend/
│   ├── app/
│   │   ├── main.py                 # FastAPI application entry point
│   │   ├── database.py             # SQLite/PostgreSQL database abstraction
│   │   ├── init_db.py              # Main workflow database schema
│   │   ├── init_auth_db.py         # Authentication database schema
│   │   ├── settings.py             # Environment configuration
│   │   ├── security.py             # Password hashing, sessions, auth helpers
│   │   ├── routers/
│   │   │   ├── auth.py             # Register, login, logout, current user
│   │   │   └── uploads.py          # Upload, matrix, report, and calculation APIs
│   │   └── services/
│   │       └── csv_service.py      # CSV parsing, validation, and normalization
│   ├── Dockerfile
│   └── requirements.txt
│
├── frontend/
│   ├── src/
│   │   ├── api/                    # API client and upload/report calls
│   │   ├── components/             # Layout, auth, table, upload, and matrix components
│   │   ├── context/                # Auth context
│   │   ├── pages/                  # Main workflow pages
│   │   ├── types/                  # TypeScript data types
│   │   └── App.tsx                 # Frontend route configuration
│   ├── package.json
│   └── vite.config.ts
│
├── .github/
│   └── workflows/
│       └── deploy-backend.yml      # Backend deployment workflow for Google Cloud Run
│
└── package.json
```

---

## 7. Local Development Setup 💻

### 7.1 Backend

From the repository root:

```bash
cd backend
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

For Windows PowerShell:

```powershell
cd backend
python -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Start the backend locally:

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8001
```

The backend should then be available at:

```text
http://127.0.0.1:8001
```

Health check:

```text
http://127.0.0.1:8001/healthz
```

### 7.2 Frontend

Open a second terminal:

```bash
cd frontend
npm install
npm run dev
```

By default, the frontend API client expects the backend at:

```text
http://127.0.0.1:8001
```

To configure the backend URL manually, create a `.env.local` file inside `frontend/`:

```env
VITE_API_BASE_URL=http://127.0.0.1:8001
```

Then restart the frontend development server.

---

## 8. Environment Variables

### Backend

| Variable                | Description                                                                | Example                                                       |
| ----------------------- | -------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `DATABASE_URL`          | Database connection string. If empty, local SQLite is used.                | `sqlite:///./tmc.db` or `postgresql://user:pass@host:5432/db` |
| `UPLOAD_ROOT_DIR`       | Folder where uploaded CSV files are stored.                                | `uploads`                                                     |
| `CORS_ALLOW_ORIGINS`    | Allowed frontend origins. Use comma-separated values for multiple origins. | `http://localhost:5173`                                       |
| `DISABLE_AUTH`          | Bypass user authentication for local development.                          | `true`                                                        |
| `PASSWORD_GATE_ENABLED` | Enable shared site-password protection.                                    | `false`                                                       |
| `PASSWORD_GATE_TOKEN`   | Shared site password token when password gate is enabled.                  | `your-password`                                               |

### Frontend

| Variable                     | Description                                  | Example                 |
| ---------------------------- | -------------------------------------------- | ----------------------- |
| `VITE_API_BASE_URL`          | Backend API base URL.                        | `http://127.0.0.1:8001` |
| `VITE_PASSWORD_GATE_ENABLED` | Enables the frontend shared password screen. | `false`                 |

---

## 9. Key Frontend Routes

| Route                          | Purpose                                            |
| ------------------------------ | -------------------------------------------------- |
| `/`                            | Home and workflow overview                         |
| `/auth`                        | Login and registration                             |
| `/pipeline`                    | Market reporting workflow viewer                   |
| `/matrix`                      | Matrix upload and maintenance                      |
| `/upload/oth`                  | External market data upload                        |
| `/upload/crp`                  | Reference and control data upload entry            |
| `/layers/:layerCode`           | Detailed workflow-layer view                       |
| `/total-market-calculation`    | Total market calculation and double-brand handling |
| `/restatement`                 | Restatement logic and result preparation           |
| `/tmc-validation-report`       | Final validation report                            |
| `/uploads/:uploadRunId/result` | Upload result and uploaded-row review              |

---

## 10. Example Workflow ✅

A typical local workflow is:

1. Start the backend.
2. Start the frontend.
3. Create or select a planning year.
4. Upload required matrix files:

   * Source Matrix
   * Reporter List
   * Size Class Matrix
   * Brand Mapping
   * Group Country
   * Machine Line Mapping
5. Upload source data:

   * External market data
   * Internal sales data
   * Total market reference data
6. Open the pipeline viewer to inspect the workflow.
7. Run or review preparation-layer outputs.
8. Review deletion and reporter flags.
9. Continue to Total Market Calculation.
10. Review double-brand cases and save calculated snapshots.
11. Open Restatement and Validation Report pages.
12. Use validation differences to support business review.

---

## 11. Design Motivation

The project is motivated by a common enterprise problem: important business calculations are often embedded inside ERP systems, spreadsheet transformations, planning sequences, or legacy reporting processes. This makes the logic difficult to inspect and validate, especially for non-technical business users.

The prototype explores how a web-based inspection layer can improve:

* **Transparency**: showing intermediate workflow layers instead of only final numbers;
* **Traceability**: linking outputs back to uploaded data, planning years, and saved runs;
* **Usability**: giving business users navigable tables, filters, and review pages;
* **Validation**: supporting comparison between calculated outputs and control figures;
* **Maintainability**: separating matrix data, source data, calculation logic, and UI review steps.

---

## 12. Current Scope

The current prototype focuses on selected parts of a complex market reporting workflow, especially:

* source and matrix upload;
* preparation-layer review;
* market-view and adjustment-layer inspection;
* selected machine-line split logic;
* total market calculation cases;
* restatement preview and validation reporting.

It does not fully replace all ERP-side logic or downstream reporting processes. Instead, it demonstrates how a reviewable web layer can support business validation around complex ERP-based workflows.

---

## 13. Future Improvements

Potential future work includes:

* completing the full end-to-end reporting workflow;
* improving automated test coverage for calculation logic;
* adding role-based access control;
* adding audit logs for edited rows and saved snapshots;
* improving import validation and error explanation;
* adding downloadable Excel/CSV reports for all key output stages;
* integrating with enterprise data platforms such as Databricks or Power BI;
* supporting larger datasets with server-side pagination and background jobs;
* separating business-rule configuration from application code.

---

## 14. Thesis Context

This project was built as part of a master’s thesis in Information Systems / Computer and Systems Sciences. The research contribution is not a new algorithm, but a design-oriented software artifact that demonstrates how complex ERP-based enterprise workflows can be made more transparent and reviewable through staged data processing, inspectable intermediate outputs, and business-facing validation interfaces.

---

## 15. Disclaimer

This repository is published for academic and portfolio demonstration purposes. It should not contain confidential business data, internal company documents, private credentials, or production system access details.

Before using this project with real enterprise data, ensure that:

* sensitive data is removed or anonymized;
* environment variables and credentials are stored securely;
* database access is properly restricted;
* authentication is enabled in deployed environments;
* company-specific logic is reviewed and approved before publication.
