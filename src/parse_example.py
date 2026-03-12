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


class Metrics(TypedDict):
    final_sets: int
    final_coloring: List[str]
    coloring_cost: int
    count_type0_child0: int
    count_branch_reduce_total: int
    count_branch_reduce_0_sets: int
    first_clique: Optional[int]
    heuristic_counts: DefaultDict[str, int]
    root_lb: Optional[int]
    root_ub: Optional[int]
    root_time: Optional[float]
    root_sets: Optional[int]
    next_lb: Optional[int]
    next_ub: Optional[int]
    branch_and_bound_nodes: int
    max_depth: int


def parse_graph_log(file_path):
    """
    Parses specific logic (Set counts, Coloring, Specific Flags, Heuristics)
    """
    metrics: Metrics = {
        "final_sets": 0,
        "final_coloring": [],
        "coloring_cost": 0,
        # Specific counters requested previously
        "count_type0_child0": 0,
        "count_branch_reduce_total": 0,
        "count_branch_reduce_0_sets": 0,
        "first_clique": None,
        "heuristic_counts": defaultdict(int),  # New metric
        "root_lb": None,
        "root_ub": None,
        "root_time": None,
        "root_sets": None,
        "next_lb": None,
        "next_ub": None,
        "branch_and_bound_nodes": 0,
        "max_depth": 0,
    }

    # Regex for specific string occurrences
    sets_pattern = re.compile(r"Final:\s+(\d+)\s+sets")
    coloring_start_pattern = re.compile(r"Coloring:\s+(\d+)\s+=>\s+(.*)")
    clique_pattern = re.compile(r"clique\s*->\s*(\d+)")
    root_pattern = re.compile(r"\(\s*([\d\.]+)s\).*Root\s+(\d+)\s+(\d+)")
    next_pattern = re.compile(r"Next:\s*\[\s*(\d+)\s*,\s*(\d+)\s*\]")
    iteration_pattern = re.compile(r"Iteration\s+(\d+)")
    depth_pattern = re.compile(r"with depth\s+(\d+)")

    # Regex for MWISheuristic -> <heuristic_name> = ...
    # Matches "MWISheuristic -> Greedy weighted =" and captures "Greedy weighted"
    heuristic_pattern = re.compile(r"MWISheuristic\s*->\s*([^=]+)\s*=")

    with open(file_path, "r") as f:
        for line in f:
            # --- Error Checking (High Priority) ---
            if "ERR" in line or "FATL" in line and "Signal: SIGTERM" not in line:
                error_content = line.strip()
                print(f"{error_content}", file=sys.stderr)
                exit(1)

            # --- Specific String Counters ---
            if "type: 0, #childrens: 0" in line:
                metrics["count_type0_child0"] += 1

            if "branch_reduce" in line:
                metrics["count_branch_reduce_total"] += 1
                if "-> 0 sets" in line:
                    metrics["count_branch_reduce_0_sets"] += 1

            # --- Heuristic Name Parsing ---
            heuristic_match = heuristic_pattern.search(line)
            if heuristic_match:
                h_name = heuristic_match.group(1).strip()
                metrics["heuristic_counts"][h_name] += 1

            # --- First Clique Parsing ---
            if metrics["first_clique"] is None:
                clique_match = clique_pattern.search(line)
                if clique_match:
                    metrics["first_clique"] = int(clique_match.group(1))

            # --- Root Parsing ---
            root_match = root_pattern.search(line)
            if root_match:
                metrics["root_time"] = float(root_match.group(1))
                metrics["root_lb"] = int(root_match.group(2))
                metrics["root_ub"] = int(root_match.group(3))

            # --- Next Bounds Parsing ---
            next_match = next_pattern.search(line)
            if next_match:
                metrics["next_lb"] = int(next_match.group(1))
                metrics["next_ub"] = int(next_match.group(2))

            # --- Iteration Parsing ---
            iter_match = iteration_pattern.search(line)
            if iter_match:
                metrics["branch_and_bound_nodes"] = int(iter_match.group(1))

            # --- Depth Parsing ---
            depth_match = depth_pattern.search(line)
            if depth_match:
                metrics["max_depth"] = max(
                    metrics["max_depth"], int(depth_match.group(1))
                )

            # --- Sets Parsing ---
            sets_match = sets_pattern.search(line)
            if sets_match:
                sets_count = int(sets_match.group(1))
                if metrics["root_sets"] is None:
                    metrics["root_sets"] = sets_count
                metrics["final_sets"] = sets_count

            # --- Coloring Parsing ---
            col_start = coloring_start_pattern.search(line)
            if col_start:
                metrics["coloring_cost"] = int(col_start.group(1))

    return metrics


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


def aggregate_all_times(file_path):
    """
    Scans for ANY pattern matching '0.000s: FunctionName' and aggregates time.
    Returns a sorted list of tuples (FunctionName, TotalTime, Count).
    """
    time_totals = defaultdict(float)
    call_counts = defaultdict(int)

    # Regex breakdown:
    # (\d+\.\d+) -> Group 1: Float (The time)
    # \s+s:\s+   -> Matches " s: "
    # (\w+)      -> Group 2: Any word characters (The function name)
    #               This stops at the first space/symbol (e.g., ignores "->")
    pattern = re.compile(r"(\d+\.\d+)\s+s:\s+(\w+)")

    try:
        with open(file_path, "r") as f:
            for line in f:
                match = pattern.search(line)
                if match:
                    duration = float(match.group(1))
                    func_name = match.group(2)

                    time_totals[func_name] += duration
                    call_counts[func_name] += 1
    except FileNotFoundError:
        return []

    # Convert to list and sort by Total Time (descending)
    results = []
    for func, total_time in time_totals.items():
        results.append({"name": func, "time": total_time, "count": call_counts[func]})

    return sorted(results, key=lambda x: x["time"], reverse=True)


def build_csv_row(directory_path, meta, general, times):
    """
    Constructs a flat dictionary representing a single CSV row from aggregated data.
    """
    # Base metadata
    row = {
        "filename": str(directory_path),
        "instance": meta.get("instance_name"),
        "time": meta.get("wall_time_seconds"),
        "exit_code": meta.get("exit_code"),
    }

    # Warn on failure
    if row["exit_code"] is not None and row["exit_code"] != 0:
        out.log(
            f"[warning]Non-zero exit code ({row['exit_code']}) detected for instance {row['instance']}[/warning]"
        )

    # Incorporate General Metrics
    row.update(
        {
            "final_sets": general.get("final_sets"),
            "final_cost": general.get("coloring_cost"),
            "first_clique": general.get("first_clique"),
            "root_lb": general.get("root_lb"),
            "root_ub": general.get("root_ub"),
            "root_time": general.get("root_time"),
            "root_sets": general.get("root_sets"),
            "branch_and_bound_nodes": general.get("branch_and_bound_nodes"),
            "max_depth": general.get("max_depth"),
            "count_type0_child0": general.get("count_type0_child0"),
            "count_branch_reduce_total": general.get("count_branch_reduce_total"),
            "count_branch_reduce_0_sets": general.get("count_branch_reduce_0_sets"),
        }
    )

    # Post-processing logic for bounds
    final_cost = row["final_cost"]
    if isinstance(final_cost, int) and final_cost > 0:
        row["lb"] = final_cost
        row["ub"] = final_cost
    else:
        # If no valid cost, invalidate time (as per original logic)
        row["time"] = None
        if row["next_lb"] is not None and row["next_ub"] is not None:
            row["lb"] = row["next_lb"]
            row["ub"] = row["next_ub"]

    # Heuristics
    for h_name, count in general.get("heuristic_counts", {}).items():
        safe_name = h_name.replace(" ", "_")
        row[f"heuristic_count_{safe_name}"] = count

    # Function Times
    for item in times:
        func_name = item["name"]
        row[f"time_{func_name}"] = item["time"]
        row[f"count_{func_name}"] = item["count"]

    if "count_Expand" in row and row["count_Expand"] > 0:
        row["root_lb"] = None
        row["root_ub"] = None
        row["root_time"] = None

    return row


def parse(directory_path, output_csv):
    # 1. Gather Data
    log_path = directory_path / "stderr.log"
    meta_path = directory_path / "meta.json"

    general = parse_graph_log(log_path)
    times = aggregate_all_times(log_path)
    meta = parse_meta_file(meta_path)

    # 2. Build CSV Row
    csv_row = build_csv_row(directory_path, meta, general, times)

    # 3. Write to CSV
    # Get all keys for the header
    fieldnames = list(csv_row.keys())

    # Write to file
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(csv_row)


def check_instance_bounds(inst_name: str, lb: float | None, ub: float | None):
    """
    Placeholder. For now, just assume that there is a
    `~/rasc/inst/metadata.csv` and it has the best know ub and lb for each instance.
    """
    # get the instance line from the metadata.csv
    known_lb = None
    known_ub = None
    with open(Path.home() / "rasc" / "inst" / "metadata.csv", "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["instance"] == inst_name:
                known_lb = float(row["lb"])
                known_ub = float(row["ub"])
    if known_lb is None or known_ub is None:
        print(f"Missing information about {inst_name}", file=sys.stderr)
        exit(1)

    if lb is not None and known_ub is not None and lb > known_ub:
        print(
            f"{inst_name}: computed lower {lb} >= known upper {known_ub}",
            file=sys.stderr,
        )
        exit(1)

    if ub is not None and known_lb is not None and ub < known_lb:
        print(
            f"{inst_name}: computed upper {ub} <= known lower {known_lb}",
            file=sys.stderr,
        )
        exit(1)

    if lb is None or ub is None:
        return

    if lb > ub:
        print(
            f"{inst_name}: computed lower {lb} > computed upper {ub}", file=sys.stderr
        )
        exit(1)

    if lb == ub and known_lb != known_ub:
        print(
            f"{inst_name}: computed optimal {lb}, but known bounds are {known_lb}-{known_ub}",
        )
        return

    print(f"{inst_name}: bounds look good.")


# --- Main Execution ---
if __name__ == "__main__":
    file_path = Path(sys.argv[1])
    output_csv = Path(sys.argv[1]) / "res.csv"

    try:
        parse(file_path, output_csv)

        # After parsing, check bounds
        with open(output_csv, "r") as f:
            reader = csv.DictReader(f)
            if (
                reader.fieldnames
                and "lb" in reader.fieldnames
                and "ub" in reader.fieldnames
            ):
                for row in reader:
                    inst_name = row["instance"]
                    lb = float(row["lb"]) if row["lb"] else None
                    ub = float(row["ub"]) if row["ub"] else None
                    check_instance_bounds(inst_name, lb, ub)

    except FileNotFoundError:
        print(f"Error: {file_path} not found.", file=sys.stderr)
