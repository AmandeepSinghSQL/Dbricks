# Databricks CI/CD Deployment Guide

## 🚀 Quick Start

### Local Deployment (from Cursor)

1. **Validate your bundle:**
   ```bash
   databricks bundle validate -t dev
   ```

2. **Deploy to dev:**
   ```bash
   databricks bundle deploy -t dev
   ```

3. **Run the job:**
   ```bash
   databricks bundle run daily_pipeline_job -t dev
   ```

4. **Deploy to prod:**
   ```bash
   databricks bundle deploy -t prod
   ```

### Automated CI/CD (GitHub Actions)

#### Setup (One-time):

1. **Create a Databricks personal access token:**
   - Go to Databricks → User Settings → Access Tokens
   - Generate new token → Copy it

2. **Add GitHub Secrets:**
   - Go to GitHub repo → Settings → Secrets and variables → Actions
   - Add:
     - `DATABRICKS_HOST`: `https://adb-6909468084062887.7.azuredatabricks.net`
     - `DATABRICKS_TOKEN`: Your access token

#### Workflow:

- **Push to `dev` branch** → Auto-deploys to dev environment + runs job
- **Push to `main` branch** → Auto-deploys to prod environment

## 📁 Project Structure

```
DBX_Test/
├── notebooks/
│   └── data_pipeline.py          # Your notebook
├── databricks.yml                 # Bundle configuration
├── .github/workflows/deploy.yml   # CI/CD pipeline
└── DEPLOYMENT.md                  # This file
```

## 🔧 What Gets Deployed

- **Notebook**: `notebooks/data_pipeline.py` → Uploaded to Databricks workspace
- **Job**: "Daily Data Pipeline - {env}" → Created/updated in Databricks jobs
- **Environment-specific**: Uses `${bundle.target}` for dev/prod separation

## 📝 Making Changes

1. Edit `notebooks/data_pipeline.py` in Cursor (with AI autocomplete!)
2. Test locally with Databricks Connect
3. Commit to Git
4. Push to `dev` branch (auto-deploys to dev)
5. Merge to `main` (auto-deploys to prod)

## ✅ Verification

After deployment, check:
- Databricks Workspace → Your bundle path
- Databricks Workflows → "Daily Data Pipeline - dev/prod"
- Run the job manually or via bundle command

