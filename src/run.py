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
import threading
import time
from concurrent import futures
from pathlib import Path
from typing import Any, Dict, Optional
import sys # Added for platform checks
import itertools # Added for cyclic core assignment

import psutil
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
    from src.console import out
except ImportError:
    from rich.console import Console

    out = Console()  # type: ignore

try:
    from src.parse import gather_results, parse_instance
except ImportError:

    def gather_results(raw_logs_dir: Path, parsed_logs_csv: Path) -> None:
        pass

    def parse_instance(parser_cmd: str, inst_path: Path) -> None:
        pass


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
        n = self.executable.name
        n += "_" + self.instance_path.name

        for k, v in self.params.items():
            n += f"_{k}_{v}"
        return n


class Runner:
    name: str
    raw_logs_dir: Path
    time_limit: int = 3600  # seconds
    list_of_instances: list[RunInstance]
    n_workers: int = 1
    run_template: str = "{executable} {instance_path}"
    class_name: Optional[str] = None
    parser_cmd: Optional[str] = None

    def __init__(
        self,
        name: str,
        raw_logs_dir: Path,
        list_of_instances: list[RunInstance],
        time_limit: int = 3600,
        n_workers: int = 1,
        run_template: str = "",
        class_name: Optional[str] = None,
        parser_cmd: Optional[str] = None,
    ):
        self.name = name
        self.n_workers = n_workers
        self.time_limit = time_limit
        self.class_name = class_name
        self.parser_cmd = parser_cmd

        # TODO check if raw_logs_dir exists, if not, warn and create
        self.raw_logs_dir = raw_logs_dir

        if run_template:
            self.run_template = run_template
            # TODO check if run_template has ">" and warn user they don't need to handle redirection

        # TODO check if a RunInstance can fulfill the run_template
        self.list_of_instances = list_of_instances

        # Initialize physical_core_ids based on the operating system
        self.physical_core_ids: list[int] = []
        if sys.platform == "linux":
            num_physical_cores = psutil.cpu_count(logical=False)
            if num_physical_cores:
                self.physical_core_ids = list(range(num_physical_cores))
                out.info(f"Detected {num_physical_cores} physical CPU cores for task pinning.")
            else:
                out.warning("Could not detect physical CPU cores. Task pinning will not be used.")
        else:
            out.info(f"Task pinning is only supported on Linux. Current OS: {sys.platform}")

        # ---
        self._print_info()
        self._run_all_instances()
        # TODO melhorar esse nome
        if self.parser_cmd:
            gather_results(
                raw_logs_dir=self.raw_logs_dir,
                parsed_logs_csv=self.raw_logs_dir.parent / f"{self.name}_results.csv",
            )

    def _monitor_memory(self, stop_event: threading.Event) -> None:
        while not stop_event.is_set():
            mem = psutil.virtual_memory()
            if mem.percent > 80.0:
                out.error(f"High memory usage detected: {mem.percent}% > 80%")
                try:
                    # Get list of processes sorted by memory usage
                    processes = sorted(
                        psutil.process_iter(attrs=["pid", "name", "memory_info", "cmdline"]),
                        key=lambda p: p.info["memory_info"].rss,
                        reverse=True,
                    )

                    out.print("Top 20 processes by memory usage:")
                    for p in processes[:20]:
                        mem_mb = p.info["memory_info"].rss / (1024 * 1024)
                        cmdline = " ".join(p.info["cmdline"]) if p.info["cmdline"] else p.info["name"]
                        out.print(
                            f"PID: {p.info['pid']:<6} "
                            f"Cmd: {cmdline:<50} "
                            f"Memory: {mem_mb:8.2f} MB"
                        )
                except Exception as e:
                    out.error(f"Could not retrieve process list: {e}")

            stop_event.wait(10)

    def _run_all_instances(self) -> None:
        stop_monitor = threading.Event()
        monitor_thread = threading.Thread(
            target=self._monitor_memory, args=(stop_monitor,), daemon=True
        )
        monitor_thread.start()

        try:
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

                # Prepare core_id assignment if physical cores are available
                core_id_iterator = None
                if self.physical_core_ids:
                    core_id_iterator = itertools.cycle(self.physical_core_ids)
                
                with futures.ThreadPoolExecutor(max_workers=self.n_workers) as executor:
                    future_to_instance = {}
                    for run_instance in self.list_of_instances:
                        current_core_id = next(core_id_iterator) if core_id_iterator else None
                        future = executor.submit(self._run_instance, run_instance, current_core_id)
                        future_to_instance[future] = run_instance
                    
                    for _ in futures.as_completed(future_to_instance):
                        progress.update(task, advance=1)
        finally:
            stop_monitor.set()
            monitor_thread.join()

    def _run_instance(self, run_instance: RunInstance, core_id: Optional[int] = None) -> None:
        inst_path = run_instance.instance_path
        log_dir = self.raw_logs_dir / f"{run_instance.name}/"

        if log_dir.exists():
            # out.info(
            #     f"Log for instance {run_instance.name} already exists, skipping..."
            # )
            return

        log_dir.mkdir(parents=True, exist_ok=True)

        format_params = {
            "executable": run_instance.executable,
            "instance_path": run_instance.instance_path,
        }
        format_params.update(run_instance.params)

        # Dica: Use .safe_substitute() se quiser evitar erros de chaves faltando,
        # mas .format() é melhor para garantir que tudo o que é necessário está lá.
        try:
            command = self.run_template.format(**format_params)
        except KeyError as e:
            out.error(f"Failed to format command. Missing key: {e}")
            return

        # Prepend taskset command if core_id is provided and physical cores are detected
        if core_id is not None and self.physical_core_ids:
            command = f"taskset -c {core_id} {command}"
            out.debug(f"Assigned instance {run_instance.name} to CPU core {core_id}")

        command = f"timeout --preserve-status --kill-after={int(self.time_limit * 0.01)} {self.time_limit}s {command}"

        # Inicializamos variáveis de resultado
        exit_code = None
        wall_time = None

        # Marcamos o tempo inicial para calcular a duração manualmente
        start_time = time.perf_counter()

        try:
            with (
                (log_dir / "stdout.log").open("w") as stdout_fd,
                (log_dir / "stderr.log").open("w") as stderr_fd,
            ):
                result = subprocess.run(
                    command,
                    shell=True,
                    timeout=int(self.time_limit),
                    stdout=stdout_fd,
                    stderr=stderr_fd,
                    text=True,
                )

                # SE SUCESSO (o processo terminou, mesmo com erro interno):
                exit_code = result.returncode
                wall_time = time.perf_counter() - start_time

        except subprocess.TimeoutExpired:
            # SE TIMEOUT:
            # out.warning(f"Instance {run_instance.name} timed out.")
            exit_code = 124  # Código padrão de timeout (ou use -1 se preferir)
            wall_time = self.time_limit  # O tempo foi o limite estipulado

        except Exception as e:
            out.error(f"Error running {run_instance.name}: {e}")
            return

        # Bloco de escrita do Meta JSON
        try:
            meta = {
                "build_name": self.name,
                "instance_name": inst_path.name,
                "instance_path": run_instance.instance_path.as_posix(),
                "command": command,
                "wall_time_seconds": wall_time,  # Agora usamos a variável local
                "exit_code": exit_code,  # Agora usamos a variável local
            }

            meta_path = log_dir / "meta.json"
            with meta_path.open("w") as meta_fd:
                json.dump(meta, meta_fd, indent=4)

        except Exception as e:
            out.error(f"Error writing {inst_path.name}/meta.json: {e}")
            return

        if self.parser_cmd:
            try:
                parse_instance(self.parser_cmd, log_dir)
            except Exception as e:
                out.error(f"Error parsing instance {inst_path.name}: {e}")

    def _print_info(self) -> None:
        info_panel = Panel(
            f"{'Workers':<15}: {self.n_workers}\n"
            f"{'Time Limit':<15}: {self.time_limit}s\n"
            f"{'Raw Logs':<15}: {self.raw_logs_dir}\n"
            f"{'# of Instances':<15}: {len(self.list_of_instances)}"
            f"{'' if not self.parser_cmd else f'\nParser':<16}: {self.parser_cmd}",
            title=f"Running {self.name} × {self.class_name}",
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

    Path(raw_logs_dir).mkdir(parents=True, exist_ok=True)

    # Initialize and run the Runner
    try:
        runner = Runner(
            name=build_name,
            raw_logs_dir=raw_logs_dir,
            list_of_instances=list_of_instances,
            n_workers=n_workers,
            # run_template="python3 {executable} -i {instance_path} --depth {depth}", #[sofia]
        )
    except Exception as e:
        out.error(f"Failed to initialize Runner: {e}")
        sys.exit(1)
