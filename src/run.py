#!/usr/bin/env -S uv run --script
#
# /// script
# dependencies = [
#   "rich",
#   "pydantic",
#   "psutil",
# ]
# ///
"""
To run:
- install uv (execute: `curl -LsSf https://astral.sh/uv/install.sh | sh` )
- make the script executable (execute: `chmod +x run.py` )
- run: ./run.py <executable_path> <instance_path_glob>
Example:
`./run.py ../builds/my_build ../instances/*`

## Parâmetros adicionais

Imagine que você queira passar uma *depth* diferente para cada execução.

1.  **No `Runner`**, você definiria o template:

    ```python
    run_template = "./{executable} -i {instance_path} --depth {depth_value}"
    ```

2.  **Ao criar `list_of_instances`**, você faria:

    ```python
    list_of_instances = [
        RunInstance(
            executable=executable_path,
            instance_path=inst_path,
            params={"depth_value": 42}  # <-- Passando o parâmetro extra
        )
        for inst_path in instance_paths
    ]
    ```

O script agora formatará corretamente o comando como
`./my_build -i ../instances/inst1.txt --depth 42`.
"""

# TODO gracefully handle KeyboardInterrupt to stop all running instances

import json
import subprocess
from concurrent import futures
from enum import Enum
from pathlib import Path
from typing import Any, Dict

from pydantic import BaseModel, Field
from rich.panel import Panel
from rich.progress import (
    BarColumn,
    Progress,
    TextColumn,
    TimeElapsedColumn,
    TimeRemainingColumn,
)

try:
    from console import out
except ImportError:
    from rich.console import Console

    out = Console()


class RunnerType(str, Enum):
    EXE = "exe"
    CPP = "cpp"
    PYTHON = "python"


class RunInstance(BaseModel):
    """
    Represents a single instance to be run by the Runner.
    {
        "executable": "path/to/executable",
        "instance_path": "path/to/instance",
        "params": {
            "param1": "value1",
            "param2": "value2"
        }
    }
    """

    executable: Path
    instance_path: Path
    params: Dict[str, Any] = Field(default_factory=dict)

    @property
    def name(self) -> str:
        n = self.executable.stem
        n += self.instance_path.stem
        for k, v in self.params.items():
            n += f"_{k}_{v}"
        return n


class Runner:
    name: str
    type: RunnerType
    raw_logs_dir: Path
    time_limit: int = 3600  # seconds
    list_of_instances: list[RunInstance]
    n_workers: int = 1
    run_template: str = "./{executable} {instance_path}"
    class_name: str = None

    def __init__(
        self,
        name: str,
        raw_logs_dir: Path,
        list_of_instances: list[RunInstance],
        time_limit: int = 3600,
        type: RunnerType = RunnerType.CPP,
        n_workers: int = 1,
        run_template: str = "",
        class_name: str = None,
    ):
        self.name = name
        self.type = type
        self.n_workers = n_workers
        self.time_limit = time_limit
        self.class_name = class_name

        # TODO check if raw_logs_dir exists, if not, warn and create
        self.raw_logs_dir = raw_logs_dir

        if run_template:
            self.run_template = run_template
            # TODO check if run_template has ">" and warn user they don't need to handle redirection
        else:
            if self.type == RunnerType.CPP:
                # TODO check if executable is in PATH
                self.run_template = "./{executable} {instance_path}"
            elif self.type == RunnerType.PYTHON:
                # TODO check if python3 is in PATH
                # TODO check if executable is a .py file and it exists
                self.run_template = "python3 {executable} {instance_path}"

        # TODO check if a RunInstance can fulfill the run_template
        self.list_of_instances = list_of_instances

        # ---
        self._print_info()
        self._run_all_instances()

    def _run_all_instances(self) -> None:
        with Progress(
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            "{task.completed}|{task.total}",
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=out,
        ) as progress:
            total_tasks = len(self.list_of_instances)
            task = progress.add_task("Running", total=total_tasks)

            with futures.ThreadPoolExecutor(max_workers=self.n_workers) as executor:
                future_to_instance = {
                    executor.submit(self._run_instance, run_instance): run_instance
                    for run_instance in self.list_of_instances
                }

                for _ in futures.as_completed(future_to_instance):
                    progress.update(task, advance=1)

    def _run_instance(self, run_instance: RunInstance) -> None:
        inst_path = run_instance.instance_path
        log_dir = self.raw_logs_dir / f"{run_instance.name}/"

        # if it exists, skip
        # TODO check in log_dir/meta.json if the exit code is 0, if not, re-run
        if log_dir.exists():
            out.info(
                f"Log for instance {run_instance.name} already exists, skipping..."
            )
            return

        log_dir.mkdir(parents=True, exist_ok=True)

        format_params = {
            "executable": run_instance.executable,
            "instance_path": run_instance.instance_path,
        }
        format_params.update(run_instance.params)
        # BUG quando o format falha, ele só não completa
        command = self.run_template.format(**format_params)

        try:
            with (
                (log_dir / "stdout.log").open("w") as stdout_fd,
                (log_dir / "stderr.log").open("w") as stderr_fd,
            ):
                result = subprocess.run(
                    command,
                    shell=True,
                    timeout=self.time_limit,
                    stdout=stdout_fd,
                    stderr=stderr_fd,
                    text=True,
                )
        except subprocess.TimeoutExpired as _:
            pass
        except Exception as e:
            out.error(f"Error running {run_instance.name}: {e}")
            return

        try:
            # Write meta.json
            # BUG se ocorrer algum erro no meio do caminho, o meta.json fica mal formatado
            time = (
                result.elapsed.total_seconds() if hasattr(result, "elapsed") else None
            )
            meta = {
                "build_name": self.name,
                "instance_name": inst_path.name,
                "instance_path": run_instance.instance_path,
                "command": command,
                "wall_time_seconds": time,
                "exit_code": result.returncode,
            }
            with (log_dir / "meta.json").open("w") as meta_fd:
                json.dump(meta, meta_fd, indent=4)
        except Exception as e:
            out.error(f"Error writing {inst_path.name}/meta.json: {e}")
            return

        # TODO call to parser

    def _print_info(self) -> None:
        info_panel = Panel(
            f"[info]{'Workers':<15}: {self.n_workers}\n"
            f"[info]{'Time Limit':<15}: {self.time_limit}s\n"
            f"[info]{'Raw Logs':<15}: {self.raw_logs_dir}\n"
            f"[info]{'# of Instances':<15}: {len(self.list_of_instances)}",
            title=f"[info]Running {self.name} × {self.class_name}[/info]",
            title_align="left",
            subtitle="Sit back and wait...",
            subtitle_align="right",
            border_style="blue",
            width=74,
        )
        out.print(info_panel)


if __name__ == "__main__":
    import sys

    import psutil

    # Check for minimum arguments (script + executable + at least 1 instance)
    if len(sys.argv) < 3:
        out.error("Usage: ./run.py <executable_path> <instance_path_glob>")
        print("Example: ./run.py ../builds/my_build ../instances/*")
        sys.exit(1)

    # The first argument is the executable path
    executable_path = sys.argv[1]

    # All remaining arguments are instance paths (expanded by the shell's '*')
    instance_paths = sys.argv[2:]

    # Create the list of RunInstance objects
    # Example without extra parameters:
    list_of_instances = [
        RunInstance(executable=Path(executable_path), instance_path=Path(inst_path))
        for inst_path in instance_paths
    ]

    # Example with extra parameters: [sofia]
    # depth_values = [2, 3, 4, 5]
    # list_of_instances = [
    #     RunInstance(
    #         executable=executable_path, instance_path=inst_path, params={"depth": depth}
    #     )
    #     for inst_path in instance_paths
    #     for depth in depth_values
    # ]

    # Set parameters based on your comments
    build_name = Path(executable_path).stem
    raw_logs_dir = Path("./logs/raw/")
    # number of physical cores
    n_workers = psutil.cpu_count(logical=False) or 1
    run_type = RunnerType.EXE

    Path(raw_logs_dir).mkdir(parents=True, exist_ok=True)

    # Initialize and run the Runner
    try:
        runner = Runner(
            name=build_name,
            raw_logs_dir=raw_logs_dir,
            list_of_instances=list_of_instances,
            type=run_type,
            n_workers=n_workers,
            # run_template="python3 {executable} -i {instance_path} --depth {depth}", #[sofia]
        )
    except Exception as e:
        out.error(f"Failed to initialize Runner: {e}")
        sys.exit(1)
