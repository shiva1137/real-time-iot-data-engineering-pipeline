#!/usr/bin/env python3
"""
Kafka Topic Initialization Script

This script initializes all required Kafka topics for the IoT data pipeline.
It reads topic configuration from topics_config.json and creates topics
using the kafka-python library.

Why Python over Shell Scripts?
- Better error handling and exception management
- Easier to test and maintain
- Type safety with type hints
- Better integration with Python ecosystem
- More readable for complex logic
- Production teams prefer Python for infrastructure scripts

Usage:
    python init_topics.py [--bootstrap-servers localhost:9092] [--config-file topics_config.json] [--verbose]

Environment Variables:
    KAFKA_BOOTSTRAP_SERVERS: Kafka broker address (default: localhost:9092)

Exit Codes:
    0: Success
    1: Configuration error
    2: Kafka connection error
    3: Topic creation error
"""

import argparse
import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

try:
    from kafka import KafkaAdminClient
    from kafka.admin import NewTopic, ConfigResource, ConfigResourceType
    from kafka.errors import (
        KafkaError,
        TopicAlreadyExistsError,
        KafkaTimeoutError,
        NodeNotReadyError,
    )
except ImportError:
    print(
        "Error: kafka-python library is required. Install it with: pip install kafka-python>=2.0.2",
        file=sys.stderr,
    )
    sys.exit(1)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Constants
DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"
DEFAULT_CONFIG_FILE = "topics_config.json"
MAX_RETRIES = 30
RETRY_INTERVAL = 2  # seconds


def load_config(config_file: Path) -> Dict:
    """
    Load topic configuration from JSON file.

    Args:
        config_file: Path to JSON configuration file

    Returns:
        Dictionary containing topic configuration

    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON
        ValueError: If config structure is invalid
    """
    if not config_file.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_file}")

    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = json.load(f)
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Invalid JSON in config file: {e.msg}", e.doc, e.pos
        ) from e

    if "topics" not in config:
        raise ValueError("Configuration file must contain 'topics' key")

    if not isinstance(config["topics"], list):
        raise ValueError("'topics' must be a list")

    logger.debug(f"Loaded configuration from {config_file}: {len(config['topics'])} topics")
    return config


def wait_for_kafka(
    bootstrap_servers: str, max_retries: int = MAX_RETRIES, retry_interval: int = RETRY_INTERVAL
) -> bool:
    """
    Wait for Kafka to be ready by attempting to connect.

    Args:
        bootstrap_servers: Kafka broker address
        max_retries: Maximum number of retry attempts
        retry_interval: Seconds to wait between retries

    Returns:
        True if Kafka is ready, False otherwise
    """
    logger.info("Waiting for Kafka to be ready...")

    for attempt in range(1, max_retries + 1):
        try:
            # Try to create admin client and list topics (lightweight operation)
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id="kafka_init_check",
                request_timeout_ms=5000,
            )
            # Try to get cluster metadata (this will fail if Kafka is not ready)
            admin_client.list_topics()
            admin_client.close()
            logger.info("Kafka is ready!")
            return True
        except (KafkaError, KafkaTimeoutError, NodeNotReadyError) as e:
            if attempt < max_retries:
                logger.debug(f"Kafka not ready yet (attempt {attempt}/{max_retries}): {e}")
                logger.info(f"Waiting... ({attempt}/{max_retries})")
                time.sleep(retry_interval)
            else:
                logger.error(f"Kafka not ready after {max_retries} attempts")
                return False
        except Exception as e:
            logger.error(f"Unexpected error checking Kafka: {e}")
            return False

    return False


def create_topic_config(topic_config: Dict) -> Dict[str, str]:
    """
    Convert topic config from JSON format to Kafka AdminClient format.

    Args:
        topic_config: Topic configuration from JSON

    Returns:
        Dictionary of config key-value pairs for Kafka
    """
    kafka_config = {}
    if "config" in topic_config:
        for key, value in topic_config["config"].items():
            # Convert camelCase to kafka format (retention.ms, cleanup.policy)
            kafka_config[key] = str(value)
    return kafka_config


def create_topics(
    admin_client: KafkaAdminClient, topics: List[Dict], bootstrap_servers: str
) -> tuple[int, int]:
    """
    Create Kafka topics from configuration.

    Args:
        admin_client: Kafka AdminClient instance
        topics: List of topic configurations
        bootstrap_servers: Kafka broker address (for logging)

    Returns:
        Tuple of (success_count, failure_count)
    """
    success_count = 0
    failure_count = 0

    new_topics = []
    for topic_config in topics:
        topic_name = topic_config["name"]
        partitions = topic_config.get("partitions", 1)
        replication_factor = topic_config.get("replication_factor", 1)
        config = create_topic_config(topic_config)

        logger.info(f"Creating topic: {topic_name}")
        logger.debug(
            f"  Partitions: {partitions}, Replication: {replication_factor}, Config: {config}"
        )

        new_topic = NewTopic(
            name=topic_name,
            num_partitions=partitions,
            replication_factor=replication_factor,
            topic_configs=config,
        )
        new_topics.append(new_topic)

    # Create all topics at once (more efficient)
    try:
        result = admin_client.create_topics(new_topics=new_topics, validate_only=False)
        
        # Check results
        for topic_name, future in result.items():
            try:
                future.result()  # Wait for topic creation
                logger.info(f"✓ Topic '{topic_name}' created successfully")
                success_count += 1
            except TopicAlreadyExistsError:
                logger.warning(f"⚠ Topic '{topic_name}' already exists (skipping)")
                success_count += 1  # Not a failure, topic exists
            except KafkaError as e:
                logger.error(f"✗ Failed to create topic '{topic_name}': {e}")
                failure_count += 1
            except Exception as e:
                logger.error(f"✗ Unexpected error creating topic '{topic_name}': {e}")
                failure_count += 1

    except Exception as e:
        logger.error(f"Error creating topics: {e}")
        failure_count = len(new_topics)
        success_count = 0

    return success_count, failure_count


def list_topics(admin_client: KafkaAdminClient) -> List[str]:
    """
    List all Kafka topics.

    Args:
        admin_client: Kafka AdminClient instance

    Returns:
        List of topic names
    """
    try:
        topics = admin_client.list_topics()
        return sorted(list(topics))
    except KafkaError as e:
        logger.error(f"Error listing topics: {e}")
        return []


def main() -> int:
    """
    Main function to initialize Kafka topics.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    parser = argparse.ArgumentParser(
        description="Initialize Kafka topics for IoT data pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        help=f"Kafka broker address (default: {DEFAULT_BOOTSTRAP_SERVERS})",
    )
    parser.add_argument(
        "--config-file",
        default=DEFAULT_CONFIG_FILE,
        help=f"Path to topics configuration JSON file (default: {DEFAULT_CONFIG_FILE})",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable debug logging",
    )
    parser.add_argument(
        "--skip-wait",
        action="store_true",
        help="Skip waiting for Kafka to be ready (assume it's already up)",
    )

    args = parser.parse_args()

    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    # Get script directory for relative paths
    script_dir = Path(__file__).parent
    config_file = script_dir / args.config_file

    logger.info("=" * 60)
    logger.info("Initializing Kafka Topics (KRaft Mode)")
    logger.info(f"Bootstrap Server: {args.bootstrap_servers}")
    logger.info("=" * 60)

    # Load configuration
    try:
        config = load_config(config_file)
    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        logger.error(f"Please ensure {config_file} exists")
        return 1
    except (json.JSONDecodeError, ValueError) as e:
        logger.error(f"Configuration error: {e}")
        return 1

    # Wait for Kafka to be ready
    if not args.skip_wait:
        if not wait_for_kafka(args.bootstrap_servers):
            logger.error("Kafka is not available. Please ensure Kafka is running.")
            logger.error(f"Bootstrap servers: {args.bootstrap_servers}")
            return 2

    # Create admin client
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=args.bootstrap_servers,
            client_id="kafka_topic_init",
            request_timeout_ms=30000,
        )
    except Exception as e:
        logger.error(f"Failed to connect to Kafka: {e}")
        logger.error(f"Bootstrap servers: {args.bootstrap_servers}")
        return 2

    # Create topics
    try:
        success_count, failure_count = create_topics(
            admin_client, config["topics"], args.bootstrap_servers
        )

        logger.info("=" * 60)
        logger.info("Listing all topics:")
        topics = list_topics(admin_client)
        for topic in topics:
            logger.info(f"  - {topic}")
        logger.info("=" * 60)

        if failure_count > 0:
            logger.warning(f"Completed with {failure_count} failure(s)")
            return 3
        else:
            logger.info("Done! Topics initialized successfully.")
            return 0

    except Exception as e:
        logger.error(f"Unexpected error: {e}", exc_info=args.verbose)
        return 3
    finally:
        admin_client.close()


if __name__ == "__main__":
    sys.exit(main())

