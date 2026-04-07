#!/usr/bin/env -S uv run --script
#
# /// script
# dependencies = [
#   "rich",
#   "pydantic",
#   "psutil",
#   "typer",
#   "gitpython",
#   "pandas",
# ]
# ///
from datetime import datetime
from pathlib import Path

import typer
from rich.traceback import install
from typer import Argument as Arg
from typer import Option as Opt

from src.config import load_config
from src.console import out
from src.parse import get_parser_command, parse_and_gather
from src.run import RunInstance, Runner
from src.utils import build_target, get_instances, get_project_root

app = typer.Typer(help="XP CLI Application")


@app.command()
def run(
    config_toml: str = Arg(..., help="Caminho para o arquivo de configuração TOML."),
    tag: str = Opt("", "--tag", help="Tag para identificar esta execução."),
    jobs: int = Opt(1, "--jobs", help="Número de trabalhos paralelos."),
):
    out.rule("Preliminares")
    # 1. Lê o arquivo .toml
    config = load_config(config_toml)
    if config is None:
        out.error("Configuração não carregada.")
        raise typer.Exit(1)

    # 2. Garante que a root do projeto existe (possivelmente clonando o repositório)
    get_project_root(config.project)
    # 4. Garante que as instâncias existem (possivelmente clonando repositórios)
    # e garante que todas as instâncias da(s) classe(s) existem
    get_instances(config.instances)
    # 5. (opcional) Garante que o script de parser.py existe
    # get_parser_script(config.project)
    # 7. Cria, se não existir, o diretório de resultados
    out.rule()

    # if tag is none, tag=datetime.now().strftime("%Y%m%d_%H%M%S")
    tag = tag or datetime.now().strftime("%y%m%d_%H%M%S")

    raw_logs_dir = Path("logs") / "raw"
    parser_script = Path(config.project.parser) if config.project.parser else None
    parser_cmd = get_parser_command(parser_script) if parser_script else None

    for build in config.build:
        build_target(build, config.project)

        if config.instances.instances is None:
            continue

        for inst_class in config.instances.classes:
            build_raw_logs_dir = raw_logs_dir / tag / build.name
            build_raw_logs_dir.mkdir(parents=True, exist_ok=True)

            run_instances = [
                RunInstance(
                    executable=Path(build.executable),
                    instance_path=instance_path,
                    # TODO especificar params adicionais do RunInstance
                )
                for instance_path in config.instances.instances[inst_class]
            ]

            Runner(
                name=build.name,
                raw_logs_dir=build_raw_logs_dir,
                # TODO especificar o TL, talvez dentro da config do build
                time_limit=build.time_limit or 3600,
                list_of_instances=run_instances,
                n_workers=jobs,
                run_template=build.run_template,
                class_name=inst_class,
                parser_cmd=parser_cmd,
            )


@app.command()
def parse(
    input_dir: str = Arg(..., help="Caminho para o diretório de entrada."),
    parser_script: str = Arg(..., help="Caminho para o script do parser."),
):
    raw_logs_dir = Path(input_dir)
    parser_path = Path(parser_script)
    parsed_logs_csv = raw_logs_dir / "parsed_results.csv"
    parse_and_gather(
        raw_logs_dir=raw_logs_dir,
        parser_path=parser_path,
        parsed_logs_csv=parsed_logs_csv,
    )


@app.command()
def plot(
    graph_type: str = Arg(..., help="Tipo de gráfico a ser gerado"),
    results_files: list[str] = Arg(..., help="Arquivos CSV de resultados para plotar."),
    instance_class: str = Opt(
        "", "--instance-class", help="Classe de instância para filtrar."
    ),
    output: str = Opt(
        "show",
        "--output",
        help="Arquivo de saída (o formato é deduzido automaticamente pela extensão).",
    ),
): ...


@app.command()
def summary(
    logs_dir: str = Opt("logs/raw", help="Caminho para a pasta raw de logs."),
):
    import json
    import re
    from datetime import datetime

    from rich import box
    from rich.table import Table

    raw_path = Path(logs_dir)
    if not raw_path.exists() or not raw_path.is_dir():
        out.error(f"Diretório de logs '{raw_path}' não encontrado.")
        raise typer.Exit(1)

    table = Table(title="XP Experiments Summary", show_lines=True, box=box.SIMPLE)
    table.add_column("Date", style="cyan", no_wrap=True)
    table.add_column("Relative", style="dim")
    table.add_column("Build Name", style="magenta")
    table.add_column("Instances", justify="right", style="green")
    table.add_column("Time Limit", justify="right", style="yellow")

    # Iterate through YYMMDD_HHMMSS directories in descending order
    experiments = sorted([d for d in raw_path.iterdir() if d.is_dir()], reverse=True)

    if not experiments:
        out.print("Nenhum experimento encontrado.")
        return

    now = datetime.now()

    def get_relative_time(dt: datetime) -> str:
        diff = now - dt
        days = diff.days
        seconds = diff.seconds
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        if days > 0:
            return f"{days} d atrás"
        if hours > 0:
            return f"{hours} h atrás"
        if minutes > 0:
            return f"{minutes} min atrás"
        return "agora"

    for exp_dir in experiments:
        try:
            exp_date = datetime.strptime(exp_dir.name, "%y%m%d_%H%M%S")
            date_str = exp_date.strftime("%Y-%m-%d %H:%M:%S")
            rel_str = get_relative_time(exp_date)
        except ValueError:
            # Ignore directories that don't match the expected date format
            continue

        builds = sorted([d for d in exp_dir.iterdir() if d.is_dir()])
        if not builds:
            table.add_row(date_str, rel_str, "[dim]vazio[/dim]", "-", "-")
            continue

        for i, build_dir in enumerate(builds):
            # Count valid instances
            instances = [
                d
                for d in build_dir.iterdir()
                if d.is_dir() and (d / "meta.json").exists()
            ]
            n_instances = len(instances)

            # Default time limit if not found
            time_limit = "?"
            if instances:
                try:
                    with (instances[0] / "meta.json").open("r") as f:
                        meta = json.load(f)
                        cmd = meta.get("command", "")
                        # Parse "--kill-after=... 3600s" or similar
                        # The timeout command ends with {self.time_limit}s
                        match = re.search(r"kill-after=\d+\s+(\d+)s", cmd)
                        if match:
                            time_limit = f"{match.group(1)}s"
                except Exception:
                    pass

            # Show date only for the first build of an experiment
            show_date = date_str if i == 0 else ""
            show_rel = rel_str if i == 0 else ""
            table.add_row(
                show_date, show_rel, build_dir.name, str(n_instances), time_limit
            )

    out.print(table)


if __name__ == "__main__":
    install()  # Instala o rich traceback para exibir erros melhores.
    app()
