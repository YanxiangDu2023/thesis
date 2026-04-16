# Zero-CLI Web Deployment (Vercel + Render)

This flow does not require Docker or Vercel CLI.

## 1) Deploy backend first (Render)

1. Open Render dashboard and choose `New +` -> `Blueprint`.
2. Connect your GitHub repo and select this project.
3. Render will detect `render.yaml` at repo root.
4. In backend service env vars, set:
   - `DATABASE_URL` = your PostgreSQL URL
   - `CORS_ALLOW_ORIGINS` = your Vercel frontend URL (for example `https://your-app.vercel.app`)
   - `PASSWORD_GATE_ENABLED` = `true`
   - `PASSWORD_GATE_TOKEN` = a shared site password for testers
   - Optional (if you upgrade to paid + persistent disk later):
     - `UPLOAD_ROOT_DIR=/var/data/uploads`
     - `AUTH_DB_PATH=/var/data/auth.db`
5. Deploy and wait until status is `Live`.
6. Copy backend URL (for example `https://thesis-backend.onrender.com`).

## 2) Deploy frontend (Vercel)

1. Open Vercel dashboard and choose `Add New` -> `Project`.
2. Import this GitHub repo.
3. In project settings:
   - `Root Directory` = `frontend`
   - Build command = `npm run build`
   - Output directory = `dist`
4. Add environment variables:
   - `VITE_API_BASE_URL` = backend URL from Render
   - `VITE_DISABLE_AUTH` = `true` (for test mode)
   - `VITE_PASSWORD_GATE_ENABLED` = `true`
5. Deploy.

## 3) Final check

1. Open frontend URL.
2. Verify homepage and pipeline pages load.
3. Run one upload + one report action.
4. If browser shows CORS error, update backend `CORS_ALLOW_ORIGINS` with exact Vercel URL and redeploy backend.
