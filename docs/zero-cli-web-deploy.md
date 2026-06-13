# Zero-CLI Web Deployment (Vercel + Google Cloud Run)

This flow does not require Docker or Vercel CLI.

## 1) Deploy backend first (Google Cloud Run)

1. Open Google Cloud Console and go to `Cloud Run`.
2. Choose `Create service` or open your existing backend service.
3. Deploy the `backend` app as the service source or container image.
4. In backend service env vars, set:
   - `DATABASE_URL` = your PostgreSQL URL
   - `CORS_ALLOW_ORIGINS` = your Vercel frontend URL (for example `https://your-app.vercel.app`)
   - `PASSWORD_GATE_ENABLED` = `true`
   - `PASSWORD_GATE_TOKEN` = a shared site password for testers
   - Optional persistent storage settings if your Cloud Run setup mounts a writable volume:
     - `UPLOAD_ROOT_DIR=/var/data/uploads`
     - `AUTH_DB_PATH=/var/data/auth.db`
5. Deploy and wait until the revision becomes healthy.
6. Copy backend URL (for example `https://your-service-xxxxx.europe-west1.run.app`).

## 2) Deploy frontend (Vercel)

1. Open Vercel dashboard and choose `Add New` -> `Project`.
2. Import this GitHub repo.
3. In project settings:
   - `Root Directory` = `frontend`
   - Build command = `npm run build`
   - Output directory = `dist`
4. Add environment variables:
   - `VITE_API_BASE_URL` = backend URL from Cloud Run
   - `VITE_DISABLE_AUTH` = `true` (for test mode)
   - `VITE_PASSWORD_GATE_ENABLED` = `true`
5. Deploy.

## 3) Final check

1. Open frontend URL.
2. Verify homepage and pipeline pages load.
3. Run one upload + one report action.
4. If browser shows CORS error, update backend `CORS_ALLOW_ORIGINS` with exact Vercel URL and redeploy backend.
