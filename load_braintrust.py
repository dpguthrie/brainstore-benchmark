"""Load and replay trace data into Braintrust for performance benchmarking.

This script downloads trace data from S3, parses it, and replays it into Braintrust
using their logging API. It supports hierarchical span structures and can optionally
flatten traces for performance comparison.

Requirements:
    - braintrust: Braintrust Python SDK for logging traces
    - python-dotenv: For loading environment variables

Environment Variables:
    - BRAINTRUST_API_KEY: Required for Braintrust authentication

Prerequisites:
    - data/big_traces.jsonl must exist (run prepare_data.py first)

Usage:
    python load_braintrust.py [--flatten] [-n ITERATIONS] [-l LIMIT]

Examples:
    # Load all traces once
    python load_braintrust.py

    # Load first 100 traces with flattened structure
    python load_braintrust.py --flatten -l 100

    # Run 5 iterations for performance testing
    python load_braintrust.py -n 5
"""

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List

import braintrust
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Constants
TRACE_FILE = "data/big_traces.jsonl"
PROJECT_NAME = "Big traces"
ROOT_SPAN_NAME = "Chat Pipeline"
DEFAULT_MODEL = "gpt-4o"


def log_child_span(
    parent: Any,
    node_id: str,
    children: Dict[str, List[str]],
    tree: Dict[str, Dict[str, Any]],
) -> None:
    """Recursively log child spans into Braintrust as a hierarchical trace structure.

    Args:
        parent: Parent span object from Braintrust logger to attach children to
        node_id: ID of the current node to process from the trace tree
        children: Dict mapping parent span IDs to lists of their child span IDs
        tree: Dict mapping node IDs to their full trace data dictionaries
    """
    child_row = tree[node_id]

    with parent.start_span(child_row["span_attributes"]["name"]) as span:
        span.log(
            input=child_row["input"],
            output=child_row["output"],
        )

        for child_id in children.get(child_row["span_id"], []):
            log_child_span(span, child_id, children, tree)


if __name__ == "__main__":
    # Load environment variables from .env file
    load_dotenv()

    # Check if trace data file exists
    if not os.path.exists(TRACE_FILE):
        logger.error(f"Trace file not found at {TRACE_FILE}")
        logger.error("Please run 'python prepare_data.py' first to download the data")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Load and replay trace data into Braintrust for performance benchmarking"
    )
    parser.add_argument(
        "--flatten",
        action="store_true",
        help="Flatten trace hierarchy instead of preserving parent-child relationships",
    )
    parser.add_argument(
        "-n",
        "--iterations",
        type=int,
        default=1,
        help="Number of times to replay the traces (default: 1)",
    )
    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=None,
        help="Limit number of trace rows to load (default: load all)",
    )
    parser.add_argument(
        "-b",
        "--batch-size",
        type=int,
        default=None,
        help="Flush after every N traces (default: flush once at end)",
    )
    args = parser.parse_args()

    # Validate arguments
    if args.iterations < 1:
        parser.error("iterations must be at least 1")
    if args.limit is not None and args.limit < 1:
        parser.error("limit must be at least 1")
    if args.batch_size is not None and args.batch_size < 1:
        parser.error("batch-size must be at least 1")

    # Login once at startup for better performance
    try:
        braintrust.login()
    except Exception as e:
        logger.error(f"Failed to authenticate with Braintrust: {e}")
        logger.error("Ensure BRAINTRUST_API_KEY is set in your environment")
        raise

    # Load and parse trace data from JSONL file
    tree: Dict[str, Dict[str, Any]] = {}
    rows: List[Dict[str, Any]] = []
    try:
        with open(TRACE_FILE, "r") as f:
            for i, line in enumerate(f):
                if args.limit and i >= args.limit:
                    break
                try:
                    rows.append(json.loads(line))
                except json.JSONDecodeError as e:
                    logger.warning(f"Skipping malformed JSON on line {i + 1}: {e}")
                    continue
    except FileNotFoundError:
        logger.error(f"Trace file not found at {TRACE_FILE}")
        raise
    except OSError as e:
        logger.error(f"Failed to read trace file: {e}")
        raise

    # Build lookup tree for efficient trace access
    for row in rows:
        tree[row["id"]] = row

    # Build parent-child relationships for hierarchical traces
    children: Dict[str, List[str]] = {}
    roots: List[str] = []

    for row in rows:
        if not args.flatten and row["span_parents"] and len(row["span_parents"]) > 0:
            for parent in row["span_parents"]:
                if parent not in children:
                    children[parent] = []
                children[parent].append(row["id"])
        else:
            del row["span_parents"]
            roots.append(row["id"])

    # Log traces to Braintrust
    logger.info(f"Logging {len(roots)} root traces")
    start = time.time()

    # Initialize Braintrust logger with async batching for optimal performance
    bt_logger = braintrust.init_logger(PROJECT_NAME)

    # Replay traces for the specified number of iterations
    for i in range(args.iterations):
        iter_start = time.time()
        for idx, root in enumerate(roots):
            row = tree[root]
            with bt_logger.start_span(ROOT_SPAN_NAME) as span:
                # Set model metadata for tracking purposes
                row["metadata"]["model"] = DEFAULT_MODEL
                span.log(
                    input=row["input"], output=row["output"], metadata=row["metadata"]
                )

                # Recursively log all child spans
                for child_id in children.get(row["span_id"], []):
                    log_child_span(span, child_id, children, tree)

            # Flush periodically if batch size is specified
            if args.batch_size and (idx + 1) % args.batch_size == 0:
                bt_logger.flush()

        logger.info(
            f"Iteration {i + 1}/{args.iterations} completed in {time.time() - iter_start:.2f}s"
        )

    # Ensure all logs are sent before exiting
    bt_logger.flush()
    logger.info(f"Total time: {time.time() - start:.2f}s")
