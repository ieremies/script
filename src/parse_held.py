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


def parse_held_log(file_path: Path) -> dict:
    """
    In the line:
    "Finished initial bounds: LB 10 and UB 14 in  0.000000 seconds."
    We can extract:
    - "root_lb": 10
    - "root_ub": 14
    - "root_time": 0.000000

    In the line:
    "Compute coloring finished: LB 11 and UB 11"
    We can extract:
    - "lb": 11
    - "ub": 11
    If this line does not exist (at the end of processing, "lb" and "ub" are the same as "root_lb" and "root_ub")
    we can extract from the last line in the format of
    "Branching with lb 7 (est. 6.999999) and ub 8 at depth 123 (id = 48480, opt_track = 0, unprocessed nodes = 111)."
    - "lb": 7
    - "ub": 8

    - "lb_improved": # of times "Lower bound improved:" appears in the log
    - "ub_improved": # of times "Upper bound improved:" appears in the log
    - "lp_integral": # of times "LP returned integral solution." appears in the log

    From the last line starting with "Branching with lb" like so
    "Branching with lb 7 (est. 6.999999) and ub 8 at depth 123 (id = 48480, opt_track = 0, unprocessed nodes = 111)."
    We can extract:
    - "branch_and_bound_nodes": 48480

    - "max_depth": maximum value of "depth" in lines starting with "Branching with lb"
    """
    data = {
        "root_lb": None,
        "root_ub": None,
        "root_time": None,
        "lb": None,
        "ub": None,
        "lb_improved": 0,
        "ub_improved": 0,
        "lp_integral": 0,
        "branch_and_bound_nodes": None,
        "max_depth": 0,
    }

    # Pre-compile regex patterns for performance
    init_bounds_re = re.compile(
        r"Finished initial bounds: LB (\d+) and UB (\d+) in\s+([0-9.]+) seconds\."
    )
    finished_re = re.compile(r"Compute coloring finished: LB (\d+) and UB (\d+)")
    branch_re = re.compile(
        r"Branching with lb (\d+).*?and ub (\d+) at depth (\d+) \(id = (\d+)"
    )

    last_branch_lb = None
    last_branch_ub = None
    last_branch_id = None
    found_finished = False
    lines = []

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except FileNotFoundError:
        return data

    for line in lines:
        # 1. Simple text matching for counters
        if "Lower bound improved:" in line:
            data["lb_improved"] += 1
        elif "Upper bound improved:" in line:
            data["ub_improved"] += 1
        elif "LP returned integral solution." in line:
            data["lp_integral"] += 1

        m_init = init_bounds_re.search(line)
        if m_init:
            data["root_lb"] = int(m_init.group(1))
            data["root_ub"] = int(m_init.group(2))
            data["root_time"] = float(m_init.group(3))
            continue

        m_branch = branch_re.search(line)
        if m_branch:
            last_branch_lb = int(m_branch.group(1))
            last_branch_ub = int(m_branch.group(2))
            depth = int(m_branch.group(3))
            last_branch_id = int(m_branch.group(4))

            if depth > data["max_depth"]:
                data["max_depth"] = depth
            continue

        m_finished = finished_re.search(line)
        if m_finished:
            data["lb"] = int(m_finished.group(1))
            data["ub"] = int(m_finished.group(2))
            found_finished = True
            continue

    # 3. Post-processing fallbacks based on docstring rules
    if not found_finished:
        if last_branch_lb is not None:
            data["lb"] = last_branch_lb
            data["ub"] = last_branch_ub
        else:
            data["lb"] = data.get("root_lb")
            data["ub"] = data.get("root_ub")

    if last_branch_id is not None:
        data["branch_and_bound_nodes"] = last_branch_id

    return data


def parse_meta_file(file_path: Path) -> dict:
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


def parse(directory_path: Path, output_csv: Path):
    # 1. Gather Data
    log_path = directory_path / "stdout.log"
    meta_path = directory_path / "meta.json"

    general = parse_held_log(log_path)
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
