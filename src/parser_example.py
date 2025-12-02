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

from rich.console import Console

out = Console()


def parse_graph_log(file_path):
    """
    Parses specific logic (Set counts, Coloring, Specific Flags, Heuristics)
    """
    metrics = {
        "final_sets": 0,
        "final_coloring": [],
        "coloring_cost": 0,
        # Specific counters requested previously
        "count_type0_child0": 0,
        "count_branch_reduce_total": 0,
        "count_branch_reduce_0_sets": 0,
        "first_clique": None,
        "heuristic_counts": defaultdict(int),  # New metric
    }

    # Regex for specific string occurrences
    sets_pattern = re.compile(r"Final:\s+(\d+)\s+sets")
    coloring_start_pattern = re.compile(r"Coloring:\s+(\d+)\s+=>\s+(.*)")
    clique_pattern = re.compile(r"clique\s*->\s*(\d+)")

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

            # --- Sets Parsing ---
            sets_match = sets_pattern.search(line)
            if sets_match:
                metrics["final_sets"] = int(sets_match.group(1))

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


def parse(file_path, output_csv):
    # 1. Get raw data
    general = parse_graph_log(file_path / "stderr.log")
    times = aggregate_all_times(file_path / "stderr.log")
    meta = parse_meta_file(file_path / "meta.json")

    # 2. Flatten data for CSV row
    csv_row = {}

    csv_row["time"] = meta.get("wall_time_seconds", None)
    csv_row["instance"] = meta.get("instance_name", None)
    csv_row["exit_code"] = meta.get("exit_code", None)
    if csv_row["exit_code"] != 0:
        out.log(
            f"[warning]Non-zero exit code ({csv_row['exit_code']}) detected for instance {csv_row['instance']}[/warning]"
        )

    # Add Filename
    csv_row["filename"] = file_path

    # Add General Metrics
    csv_row["final_sets"] = general["final_sets"]
    csv_row["final_cost"] = general["coloring_cost"]
    if csv_row["final_cost"] > 0:
        csv_row["lb"] = csv_row["ub"] = csv_row["final_cost"]
    else:
        csv_row["time"] = None
    csv_row["first_clique"] = general["first_clique"]
    csv_row["count_type0_child0"] = general["count_type0_child0"]
    csv_row["count_branch_reduce_total"] = general["count_branch_reduce_total"]
    csv_row["count_branch_reduce_0_sets"] = general["count_branch_reduce_0_sets"]
    # Convert list to string to fit in one CSV cell
    csv_row["final_coloring"] = str(general["final_coloring"])

    # Add Heuristic Counts (flattened)
    for h_name, count in general["heuristic_counts"].items():
        safe_name = h_name.replace(" ", "_")
        csv_row[f"heuristic_count_{safe_name}"] = count

    # Add Time Metrics (flattened)
    for item in times:
        func_name = item["name"]
        csv_row[f"time_{func_name}"] = item["time"]
        csv_row[f"count_{func_name}"] = item["count"]

    # 3. Write to CSV
    # Get all keys for the header
    fieldnames = list(csv_row.keys())

    # Write to file
    with open(output_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(csv_row)

    # 4. Print CSV to stdout (for immediate viewing)
    # writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    # writer.writeheader()
    # writer.writerow(csv_row)


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

    if lb is not None and lb > known_ub:
        print(
            f"{inst_name}: computed lower {lb} >= known upper {known_ub}",
            file=sys.stderr,
        )
        exit(1)

    if ub is not None and ub < known_lb:
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
            if "lb" in reader.fieldnames and "ub" in reader.fieldnames:
                for row in reader:
                    inst_name = row["instance"]
                    lb = float(row["lb"]) if row["lb"] else None
                    ub = float(row["ub"]) if row["ub"] else None
                    check_instance_bounds(inst_name, lb, ub)

    except FileNotFoundError:
        print(f"Error: {file_path} not found.", file=sys.stderr)
