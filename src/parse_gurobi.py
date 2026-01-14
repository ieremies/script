#!/usr/bin/env -S uv run --script
#
# /// script
# dependencies = [
#   "psutil",
#   "pandas",
#   "rich",
# ]
# ///

import csv
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, List, Optional, TypedDict

from rich.console import Console

out = Console()


def parse_gurobi_log(file_path):
    """
    Parses a Gurobi log file and returns a dictionary of statistics.
    """
    data = {}
    FLOAT_PATTERN = r"[+\-]?\d+(?:\.\d+)?(?:[eE][+\-]?\d+)?"

    patterns = {
        "version": r"Gurobi Optimizer version ([^\s]+)",
        "model_size": r"Optimize a model with (\d+) rows, (\d+) columns and (\d+) nonzeros",
        "presolve_removed": r"Presolve removed (\d+) rows and (\d+) columns",
        "presolve_time": r"Presolve time: (" + FLOAT_PATTERN + r")s",
        "root_relaxation": (
            r"Root relaxation: objective ("
            + FLOAT_PATTERN
            + r"), (\d+) iterations, ("
            + FLOAT_PATTERN
            + r") seconds"
        ),
        "explored_nodes": (
            r"Explored (\d+) nodes \((\d+) simplex iterations\) in ("
            + FLOAT_PATTERN
            + r") seconds"
        ),
        "optimal_solution": (
            r"Best objective ("
            + FLOAT_PATTERN
            + r"), best bound ("
            + FLOAT_PATTERN
            + r"), gap ("
            + FLOAT_PATTERN
            + r")%"
        ),
    }

    with open(file_path, "r") as f:
        content = f.read()

    # 1. Model Info
    match = re.search(patterns["version"], content)
    if match:
        data["gurobi_version"] = match.group(1)

    match = re.search(patterns["model_size"], content)
    if match:
        data["original_rows"] = int(match.group(1))
        data["original_columns"] = int(match.group(2))
        data["original_nonzeros"] = int(match.group(3))

    # 2. Presolve Info
    match = re.search(patterns["presolve_removed"], content)
    if match:
        data["presolve_removed_rows"] = int(match.group(1))
        data["presolve_removed_columns"] = int(match.group(2))

    match = re.search(patterns["presolve_time"], content)
    if match:
        data["presolve_time_seconds"] = float(match.group(1))

    # 3. Root Relaxation Info
    match = re.search(patterns["root_relaxation"], content)
    if match:
        data["root_obj"] = float(match.group(1))
        data["root_iterations"] = int(match.group(2))
        data["root_time_seconds"] = float(match.group(3))

    # 4. Search Progress (Nodes & Simplex Iterations)
    match = re.search(patterns["explored_nodes"], content)
    if match:
        data["explored_nodes"] = int(match.group(1))
        data["simplex_iterations"] = int(match.group(2))
        data["gurobi_time_seconds"] = float(match.group(3))

    # 5. Final Solution Info
    match = re.search(patterns["optimal_solution"], content)
    if match:
        data["ub"] = float(match.group(1))
        data["lb"] = float(match.group(2))
        data["gap_percent"] = float(match.group(3))

    return data


def parse_meta_file(file_path):
    """
    Parses meta.json file for additional metadata.
    """
    import json

    try:
        with open(file_path, "r") as f:
            meta = json.load(f)
    except FileNotFoundError:
        meta = {}

    return meta


def parse(directory_path, output_csv):
    # 1. Gather Data
    log_path = directory_path / "stdout.log"
    meta_path = directory_path / "meta.json"

    general = parse_gurobi_log(log_path)
    meta = parse_meta_file(meta_path)

    # 2. Combine data
    csv_row = {}
    csv_row["instance"] = meta.get("instance_name")
    if meta.get("exit_code") == 0:
        csv_row["time"] = meta.get("wall_time_seconds")
    csv_row.update(general)
    csv_row.update(meta)

    out.log(csv_row)

    # 3. Write to CSV
    # Get all keys for the header
    fieldnames = list(csv_row.keys())

    # Write to file
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(csv_row)


# --- Main Execution ---
if __name__ == "__main__":
    file_path = Path(sys.argv[1])
    output_csv = Path(sys.argv[1]) / "res.csv"

    try:
        parse(file_path, output_csv)

        # After parsing, check bounds
        with open(output_csv, "r") as f:
            reader = csv.DictReader(f)

    except FileNotFoundError:
        print(f"Error: {file_path} not found.", file=sys.stderr)
