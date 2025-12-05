"""Load and replay trace data into LangSmith for performance benchmarking.

This script parses pre-downloaded trace data and replays it into LangSmith using
their RunTree API. It creates hierarchical run structures and handles batched
flushing to avoid payload timeout issues.

Requirements:
    - langsmith: LangSmith Python SDK for logging traces
    - python-dotenv: For loading environment variables

Environment Variables:
    - LANGCHAIN_API_KEY: Required for LangSmith authentication

Prerequisites:
    - data/big_traces.jsonl must exist (run prepare_data.py first)

Usage:
    python load_langsmith.py [--flatten] [-n ITERATIONS] [-l LIMIT]

Examples:
    # Load all traces once
    python load_langsmith.py

    # Load first 100 traces with flattened structure
    python load_langsmith.py --flatten -l 100

    # Run 5 iterations for performance testing
    python load_langsmith.py -n 5
"""
import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List

import dotenv
from langsmith import RunTree

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# Constants
TRACE_FILE = "data/big_traces.jsonl"
ROOT_RUN_NAME = "Chat Pipeline"
RUN_TYPE = "chain"


def log_child_run(
    parent: RunTree, node_id: str, children: Dict[str, List[str]], tree: Dict[str, Dict[str, Any]]
) -> None:
    """Recursively log child runs into LangSmith as a hierarchical trace structure.

    Args:
        parent: Parent RunTree object from LangSmith to attach children to
        node_id: ID of the current node to process from the trace tree
        children: Dict mapping parent span IDs to lists of their child span IDs
        tree: Dict mapping node IDs to their full trace data dictionaries
    """
    child_row = tree[node_id]

    child = parent.create_child(
        child_row["span_attributes"]["name"],
        run_type=RUN_TYPE,
        inputs=child_row["input"],
        outputs=child_row["output"],
    )

    for child_id in children.get(child_row["span_id"], []):
        log_child_run(child, child_id, children, tree)

    child.end()
    child.post()


if __name__ == "__main__":
    # Load environment variables from .env file
    dotenv.load_dotenv()

    # Check if trace data file exists
    if not os.path.exists(TRACE_FILE):
        logger.error(f"Trace file not found at {TRACE_FILE}")
        logger.error("Please run 'python prepare_data.py' first to download the data")
        sys.exit(1)

    parser = argparse.ArgumentParser(
        description="Load and replay trace data into LangSmith for performance benchmarking"
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

    # Configure LangSmith endpoint and enable tracing
    os.environ["LANGCHAIN_ENDPOINT"] = "https://api.smith.langchain.com"
    os.environ["LANGCHAIN_TRACING_V2"] = "true"

    from langsmith import Client

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
                    logger.warning(f"Skipping malformed JSON on line {i+1}: {e}")
                    continue
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
            if "span_parents" in row:
                del row["span_parents"]
            roots.append(row["id"])

    # Log traces to LangSmith
    logger.info(f"Logging {len(roots)} root traces")
    start = time.time()

    client = Client()

    # Replay traces for the specified number of iterations
    for iteration in range(args.iterations):
        iter_start = time.time()

        for idx, root in enumerate(roots):
            row = tree[root]
            pipeline = RunTree(
                name=ROOT_RUN_NAME,
                run_type=RUN_TYPE,
                inputs=row["input"],
                outputs=row["output"],
                metadata=row["metadata"],
                client=client,
            )

            # Recursively log all child runs
            for child_id in children.get(row["span_id"], []):
                log_child_run(pipeline, child_id, children, tree)

            pipeline.end()
            pipeline.post()

            # Flush periodically if batch size is specified
            if args.batch_size and (idx + 1) % args.batch_size == 0:
                client.flush()

        # Flush once at the end of iteration
        client.flush()

        logger.info(
            f"Iteration {iteration+1}/{args.iterations} completed in {time.time() - iter_start:.2f}s"
        )

    logger.info(f"Total time: {time.time() - start:.2f}s")
