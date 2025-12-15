# PowerShell script for GitHub setup
# Real-Time IoT Data Engineering Pipeline

$REPO_NAME = "real-time-iot-data-engineering-pipeline"
$GITHUB_USERNAME = "your-username"  # Replace with your GitHub username

Write-Host "==========================================" -ForegroundColor Cyan
Write-Host "GitHub Repository Setup" -ForegroundColor Cyan
Write-Host "==========================================" -ForegroundColor Cyan
Write-Host ""
Write-Host "Repository Name: $REPO_NAME" -ForegroundColor Yellow
Write-Host ""
Write-Host "Steps to create and push to GitHub:" -ForegroundColor Green
Write-Host ""
Write-Host "1. Go to https://github.com/new" -ForegroundColor White
Write-Host "2. Repository name: $REPO_NAME" -ForegroundColor White
Write-Host "3. Description: Real-Time IoT Data Engineering Pipeline: Streaming Data Processing & Analytics" -ForegroundColor White
Write-Host "4. Set to Public" -ForegroundColor White
Write-Host "5. DO NOT initialize with README, .gitignore, or license (we already have these)" -ForegroundColor White
Write-Host "6. Click 'Create repository'" -ForegroundColor White
Write-Host ""
Write-Host "After creating the repository, run these commands:" -ForegroundColor Green
Write-Host ""
Write-Host "  git remote add origin https://github.com/$GITHUB_USERNAME/$REPO_NAME.git" -ForegroundColor Cyan
Write-Host "  git branch -M main" -ForegroundColor Cyan
Write-Host "  git push -u origin main" -ForegroundColor Cyan
Write-Host ""

