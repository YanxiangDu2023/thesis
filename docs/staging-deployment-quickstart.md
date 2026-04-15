# Staging Deployment Quickstart

This checklist gets the current app online for testers while keeping development ongoing.

## 1) Branch and release flow

- Keep active feature work on `develop` or feature branches.
- Promote stable snapshots to `main`.
- Deploy `main` to a public staging URL for testers.

## 2) Backend environment (`backend/.env`)

Use this as a baseline:

```env
DATABASE_URL=postgresql://<user>:<password>@<host>:5432/<db>
AUTH_DB_PATH=auth.db
UPLOAD_ROOT_DIR=uploads
CORS_ALLOW_ORIGINS=https://<your-frontend-domain>
DISABLE_AUTH=true
```

Notes:

- For local-only development you can keep `DATABASE_URL=sqlite:///./tmc.db`.
- In cloud hosting, `UPLOAD_ROOT_DIR` should point to a persistent volume path.
- Set `DISABLE_AUTH=false` when you are ready to enforce login.

## 3) Frontend environment (`frontend/.env`)

```env
VITE_API_BASE_URL=https://<your-backend-domain>
VITE_DISABLE_AUTH=true
```

## 4) Prepare PostgreSQL schema

1. Set backend `DATABASE_URL` to PostgreSQL.
2. Start backend once so startup creates tables (`init_db()`).
3. Optional: migrate existing local SQLite data.

```bash
python backend/scripts/migrate_sqlite_to_postgres.py \
  --sqlite-path backend/tmc.db \
  --postgres-url postgresql://<user>:<password>@<host>:5432/<db>
```

## 5) Deploy

- Frontend: Vercel/Netlify or your internal static hosting.
- Backend: Render/Railway/Azure App Service (FastAPI service).
- Confirm CORS allows only the frontend staging origin.

## 6) Smoke test after deploy

- Open homepage and pipeline pages.
- Upload one CSV and verify run history updates.
- Open P00/P10/A10 pages and check table rendering and actions.
- Verify no 401/CORS errors in browser console.

## 7) Continue development safely

- Keep pushing new work to `develop`.
- Merge to `main` only tested batches.
- Tag stable milestones before each staging refresh.
