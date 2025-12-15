#!/bin/bash

# Setup Script for IoT Data Engineering Project
# This script sets up the development environment

set -e

echo "=========================================="
echo "IoT Data Engineering Project - Setup"
echo "=========================================="

# Check prerequisites
echo "Checking prerequisites..."

if ! command -v docker &> /dev/null; then
    echo "Error: Docker is not installed. Please install Docker first."
    exit 1
fi

if ! command -v docker-compose &> /dev/null; then
    echo "Error: Docker Compose is not installed. Please install Docker Compose first."
    exit 1
fi

if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 is not installed. Please install Python 3.9+ first."
    exit 1
fi

echo "✓ Prerequisites check passed"

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file from .env.example..."
    cp .env.example .env
    echo "✓ .env file created. Please update it with your configuration."
else
    echo "✓ .env file already exists"
fi

# Start Docker services
echo "Starting Docker services..."
cd docker
docker-compose up -d

echo "Waiting for services to be ready..."
sleep 10

# Check service health
echo "Checking service health..."
docker-compose ps

echo "=========================================="
echo "Setup complete!"
echo "=========================================="
echo ""
echo "Services running:"
echo "  - MongoDB: localhost:27017"
echo "  - PostgreSQL: localhost:5432"
echo "  - Kafka: localhost:9092"
echo "  - Kafka UI: localhost:8080"
echo ""
echo "Next steps:"
echo "  1. Initialize Kafka topics: cd kafka && bash init-topics.sh"
echo "  2. Start data generator: cd data_generator && python generator.py"
echo ""

