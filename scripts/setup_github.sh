#!/bin/bash

# Setup GitHub Repository Script
# This script helps you push your project to GitHub

REPO_NAME="real-time-iot-data-engineering-pipeline"
GITHUB_USERNAME="your-username"  # Replace with your GitHub username

echo "=========================================="
echo "GitHub Repository Setup"
echo "=========================================="
echo ""
echo "Repository Name: $REPO_NAME"
echo ""
echo "Steps to create and push to GitHub:"
echo ""
echo "1. Go to https://github.com/new"
echo "2. Repository name: $REPO_NAME"
echo "3. Description: Real-Time IoT Data Engineering Pipeline: Streaming Data Processing & Analytics"
echo "4. Set to Public"
echo "5. DO NOT initialize with README, .gitignore, or license (we already have these)"
echo "6. Click 'Create repository'"
echo ""
echo "After creating the repository, run these commands:"
echo ""
echo "  git remote add origin https://github.com/$GITHUB_USERNAME/$REPO_NAME.git"
echo "  git branch -M main"
echo "  git push -u origin main"
echo ""
echo "Or if you prefer SSH:"
echo ""
echo "  git remote add origin git@github.com:$GITHUB_USERNAME/$REPO_NAME.git"
echo "  git branch -M main"
echo "  git push -u origin main"
echo ""

