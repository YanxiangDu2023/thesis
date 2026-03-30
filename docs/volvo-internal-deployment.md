# Volvo Internal Deployment Plan

## Recommended target architecture

This project fits best as an internal data-processing application deployed inside a Volvo-approved Azure landing zone.

- Frontend: Azure App Service or an internal-facing static hosting pattern for the React/Vite app
- Backend: Azure App Service or Azure Container Apps for the FastAPI API
- Database: Azure Database for PostgreSQL
- File storage: Azure Blob Storage or ADLS Gen2 for raw uploads, edited snapshots, and exports
- Identity: Microsoft Entra ID with Volvo SSO and group-based authorization
- Secrets: Azure Key Vault
- Monitoring: Application Insights and Log Analytics
- Networking: VNet integration, private endpoints, and private DNS

## Why this is the right fit

The current codebase is still in local-development shape:

- SQLite is used as a local file database in `backend/app/database.py`
- Uploaded files are written to the local `uploads` folder in `backend/app/services/csv_service.py` and `backend/app/routers/uploads.py`
- The frontend API base URL is hardcoded to `http://127.0.0.1:8001` in `frontend/src/api/uploads.ts`
- FastAPI currently allows all CORS origins in `backend/app/main.py`

That is fine for local work, but not for an internal production rollout.

## Concrete migration steps

### Phase 1: Make the app deployment-ready

- Move database access from SQLite to PostgreSQL
- Move uploaded file storage from local disk to Blob Storage or ADLS
- Replace hardcoded API URLs with environment variables
- Lock down CORS to approved internal origins
- Add Entra ID authentication and role checks

### Phase 2: Deploy non-production

- Create `dev` and `test` environments
- Set up CI/CD for frontend and backend
- Add managed identity for backend access to Key Vault and Storage
- Enable logging, tracing, and basic alerts

### Phase 3: Internal production rollout

- Deploy to `prod` in the approved landing zone
- Restrict access to internal users only
- Define data retention for uploaded CSV files
- Add backup, restore, and audit procedures

## Recommended hosting choice

For this project, `Azure App Service + PostgreSQL + Blob Storage` is the cleanest starting point.

Choose Azure Container Apps only if you already know the team wants container-first deployment and revision-based release management.

## File delivered

Open this file in draw.io / diagrams.net:

- `docs/volvo-internal-deployment.drawio`

Simpler cloud versions:

- `docs/volvo-internal-deployment-simple.drawio`
- `docs/volvo-internal-deployment-simple.svg`

Role-based access version:

- `docs/volvo-role-based-access.drawio`
- `docs/volvo-role-based-access.svg`
