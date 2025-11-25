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
from src.utils import build_all, get_instances, get_project_root

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
    # 2. Garante que a root do projeto existe (possivelmente clonando o repositório)
    get_project_root(config.project)
    # 3. Para cada build, executa o comando de build
    build_all(config.build, config.project)
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
        for inst_class in config.instances.classes:
            build_raw_logs_dir = raw_logs_dir / tag / build.name
            build_raw_logs_dir.mkdir(parents=True, exist_ok=True)

            run_instances = [
                RunInstance(
                    executable=build.executable,
                    instance_path=instance_path,
                    # TODO especificar params adicionais do RunInstance
                )
                for instance_path in config.instances.instances[inst_class]
            ]

            Runner(
                name=build.name,
                raw_logs_dir=build_raw_logs_dir,
                # TODO especificar o TL, talvez dentro da config do build
                time_limit=10,
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
    parse_and_gather(raw_logs_dir, parser_path, parsed_logs_csv)


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


if __name__ == "__main__":
    install()  # Instala o rich traceback para exibir erros melhores.
    app()
