# GitHub Repository Setup Guide

## Repository Name
**Real-Time IoT Data Engineering Pipeline: Streaming Data Processing & Analytics**

## Quick Setup Steps

### 1. Create GitHub Repository

1. Go to [https://github.com/new](https://github.com/new)
2. **Repository name**: `real-time-iot-data-engineering-pipeline`
3. **Description**: `Real-Time IoT Data Engineering Pipeline: Streaming Data Processing & Analytics - A production-grade data pipeline processing 864K+ IoT sensor readings per day`
4. **Visibility**: Select **Public**
5. **Important**: DO NOT check any of these boxes:
   - ❌ Add a README file
   - ❌ Add .gitignore
   - ❌ Choose a license
   
   (We already have these files in the project)

6. Click **"Create repository"**

### 2. Connect Local Repository to GitHub

After creating the repository, GitHub will show you commands. Use these:

**Option A: HTTPS (Recommended for beginners)**
```bash
git remote add origin https://github.com/YOUR_USERNAME/real-time-iot-data-engineering-pipeline.git
git branch -M main
git push -u origin main
```

**Option B: SSH (If you have SSH keys set up)**
```bash
git remote add origin git@github.com:YOUR_USERNAME/real-time-iot-data-engineering-pipeline.git
git branch -M main
git push -u origin main
```

**Replace `YOUR_USERNAME` with your actual GitHub username!**

### 3. Verify Push

After pushing, refresh your GitHub repository page. You should see:
- ✅ All project files
- ✅ README.md with project description
- ✅ Complete folder structure
- ✅ Documentation in `docs/` folder

## Repository Settings (Optional but Recommended)

### Add Topics/Tags
Go to repository settings and add these topics:
- `data-engineering`
- `iot`
- `apache-kafka`
- `apache-spark`
- `python`
- `docker`
- `mongodb`
- `postgresql`
- `dbt`
- `fastapi`
- `airflow`
- `streaming-data`
- `real-time-analytics`

### Add Repository Description
Update the repository description to:
```
Production-grade IoT data pipeline: Kafka → Spark Streaming/Batch → MongoDB/PostgreSQL → dbt → FastAPI. Handles 864K+ readings/day with real-time processing, batch jobs, and analytics API.
```

## Future Updates

To push future changes:
```bash
git add .
git commit -m "Your commit message"
git push origin main
```

## Troubleshooting

### If you get "remote origin already exists"
```bash
git remote remove origin
git remote add origin https://github.com/YOUR_USERNAME/real-time-iot-data-engineering-pipeline.git
```

### If you get authentication errors
- For HTTPS: Use a Personal Access Token instead of password
- For SSH: Make sure your SSH key is added to GitHub

### If branch name is different
```bash
git branch -M main  # Rename current branch to main
git push -u origin main
```

