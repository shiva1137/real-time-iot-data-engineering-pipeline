#!/usr/bin/env python3
"""
Project Setup Script for IoT Data Engineering Project

This script sets up the development environment by:
1. Checking prerequisites (Docker, Docker Compose, Python)
2. Creating .env file from .env.example if needed
3. Starting Docker services
4. Verifying service health

Why Python over Shell Scripts?
- Better error handling and exception management
- Easier to test and maintain
- Type safety with type hints
- Better integration with Python ecosystem
- Cross-platform compatibility (Windows, Linux, macOS)
- More readable for complex logic

Usage:
    python setup.py [--skip-prereqs] [--skip-docker] [--services-only SERVICE1,SERVICE2] [--dry-run]

Exit Codes:
    0: Success
    1: Prerequisites check failed
    2: File operation error
    3: Docker operation error
"""

import argparse
import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import List, Optional, Tuple

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Constants
PROJECT_ROOT = Path(__file__).parent.parent
DOCKER_DIR = PROJECT_ROOT / "docker"
ENV_EXAMPLE = PROJECT_ROOT / ".env.example"
ENV_FILE = PROJECT_ROOT / ".env"
SERVICE_WAIT_TIME = 10  # seconds


def check_command(command: str, description: str) -> Tuple[bool, Optional[str]]:
    """
    Check if a command is available in PATH.

    Args:
        command: Command name to check
        description: Human-readable description for error messages

    Returns:
        Tuple of (is_available, version_or_error_message)
    """
    # Try shutil.which first (cross-platform)
    if shutil.which(command):
        # Try to get version
        try:
            result = subprocess.run(
                [command, "--version"],
                capture_output=True,
                text=True,
                timeout=5,
            )
            version = result.stdout.strip().split("\n")[0] if result.returncode == 0 else None
            return True, version
        except (subprocess.TimeoutExpired, FileNotFoundError):
            return True, None
    return False, None


def check_prerequisites(skip: bool = False) -> bool:
    """
    Check if all prerequisites are installed.

    Args:
        skip: If True, skip prerequisite checks

    Returns:
        True if all prerequisites are met, False otherwise
    """
    if skip:
        logger.info("Skipping prerequisites check")
        return True

    logger.info("Checking prerequisites...")

    prerequisites = [
        ("docker", "Docker"),
        ("docker-compose", "Docker Compose"),
        ("python3", "Python 3"),
    ]

    all_present = True
    for command, description in prerequisites:
        is_present, version = check_command(command, description)
        if is_present:
            version_str = f" ({version})" if version else ""
            logger.info(f"✓ {description} is installed{version_str}")
        else:
            logger.error(f"✗ {description} is not installed")
            logger.error(f"  Please install {description} first")
            all_present = False

    if not all_present:
        logger.error("\nPrerequisites check failed. Please install missing tools.")
        return False

    logger.info("✓ Prerequisites check passed")
    return True


def check_docker_running() -> bool:
    """
    Check if Docker daemon is running.

    Returns:
        True if Docker is running, False otherwise
    """
    try:
        result = subprocess.run(
            ["docker", "info"],
            capture_output=True,
            text=True,
            timeout=10,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError):
        return False


def create_env_file(dry_run: bool = False) -> bool:
    """
    Create .env file from .env.example if it doesn't exist.

    Args:
        dry_run: If True, only show what would be done

    Returns:
        True if successful, False otherwise
    """
    if ENV_FILE.exists():
        logger.info("✓ .env file already exists")
        return True

    if not ENV_EXAMPLE.exists():
        logger.warning(f"⚠ .env.example not found at {ENV_EXAMPLE}")
        logger.warning("  Skipping .env file creation")
        return True  # Not a critical error

    if dry_run:
        logger.info(f"[DRY RUN] Would create .env from {ENV_EXAMPLE}")
        return True

    try:
        shutil.copy(ENV_EXAMPLE, ENV_FILE)
        logger.info("✓ .env file created from .env.example")
        logger.info("  Please update .env with your configuration")
        return True
    except PermissionError:
        logger.error(f"✗ Permission denied: Cannot create {ENV_FILE}")
        return False
    except Exception as e:
        logger.error(f"✗ Error creating .env file: {e}")
        return False


def start_docker_services(
    services_only: Optional[List[str]] = None, dry_run: bool = False
) -> bool:
    """
    Start Docker services using docker-compose.

    Args:
        services_only: List of specific services to start (None = all services)
        dry_run: If True, only show what would be done

    Returns:
        True if successful, False otherwise
    """
    if not DOCKER_DIR.exists():
        logger.error(f"✗ Docker directory not found: {DOCKER_DIR}")
        return False

    if not check_docker_running():
        logger.error("✗ Docker daemon is not running")
        logger.error("  Please start Docker Desktop or Docker daemon")
        return False

    compose_file = DOCKER_DIR / "docker-compose.yml"
    if not compose_file.exists():
        logger.error(f"✗ docker-compose.yml not found: {compose_file}")
        return False

    logger.info("Starting Docker services...")

    if dry_run:
        services_str = f" {', '.join(services_only)}" if services_only else " all"
        logger.info(f"[DRY RUN] Would start{services_str} services")
        return True

    try:
        # Change to docker directory
        original_cwd = os.getcwd()
        os.chdir(DOCKER_DIR)

        # Build docker-compose command
        cmd = ["docker-compose", "up", "-d"]
        if services_only:
            cmd.extend(services_only)

        logger.debug(f"Running: {' '.join(cmd)}")
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=300,  # 5 minutes timeout
        )

        if result.returncode != 0:
            logger.error(f"✗ Failed to start Docker services")
            logger.error(f"  Error: {result.stderr}")
            return False

        logger.info("✓ Docker services started")
        logger.info("Waiting for services to be ready...")
        time.sleep(SERVICE_WAIT_TIME)

        # Check service health
        logger.info("Checking service health...")
        health_result = subprocess.run(
            ["docker-compose", "ps"],
            capture_output=True,
            text=True,
            timeout=30,
        )

        if health_result.returncode == 0:
            logger.info("\n" + health_result.stdout)
        else:
            logger.warning("Could not get service status")

        return True

    except subprocess.TimeoutExpired:
        logger.error("✗ Docker command timed out")
        return False
    except FileNotFoundError:
        logger.error("✗ docker-compose command not found")
        return False
    except Exception as e:
        logger.error(f"✗ Error starting Docker services: {e}")
        return False
    finally:
        # Restore original directory
        os.chdir(original_cwd)


def print_next_steps():
    """Print next steps for the user."""
    logger.info("=" * 60)
    logger.info("Setup complete!")
    logger.info("=" * 60)
    logger.info("")
    logger.info("Services running:")
    logger.info("  - MongoDB: localhost:27017")
    logger.info("  - PostgreSQL: localhost:5432")
    logger.info("  - Kafka: localhost:9092")
    logger.info("  - Kafka UI: localhost:8080")
    logger.info("")
    logger.info("Next steps:")
    logger.info("  1. Initialize Kafka topics:")
    logger.info("     cd kafka && python init_topics.py")
    logger.info("  2. Start data generator:")
    logger.info("     cd data_generator && python producer.py")
    logger.info("")


def main() -> int:
    """
    Main function to set up the project.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    parser = argparse.ArgumentParser(
        description="Set up IoT Data Engineering Project development environment",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--skip-prereqs",
        action="store_true",
        help="Skip prerequisites check",
    )
    parser.add_argument(
        "--skip-docker",
        action="store_true",
        help="Skip starting Docker services",
    )
    parser.add_argument(
        "--services-only",
        type=str,
        help="Only start specific services (comma-separated list, e.g., 'kafka,mongodb')",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without making changes",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    logger.info("=" * 60)
    logger.info("IoT Data Engineering Project - Setup")
    logger.info("=" * 60)

    # Check prerequisites
    if not check_prerequisites(skip=args.skip_prereqs):
        return 1

    # Create .env file
    if not create_env_file(dry_run=args.dry_run):
        return 2

    # Start Docker services
    if not args.skip_docker:
        services_list = None
        if args.services_only:
            services_list = [s.strip() for s in args.services_only.split(",")]

        if not start_docker_services(services_only=services_list, dry_run=args.dry_run):
            return 3

    # Print next steps
    if not args.dry_run:
        print_next_steps()

    return 0


if __name__ == "__main__":
    sys.exit(main())

