import os
import subprocess
from pathlib import Path

import pandas as pd  # type: ignore
import psutil
from rich.progress import track

try:
    from src.console import out
except ImportError:
    from rich.console import Console

    out = Console()  # type: ignore


def get_parser_command(parser_path: Path) -> str:
    # Check if parser_path exists
    parser_path = parser_path.expanduser().resolve()
    if not parser_path.exists():
        out.error(f"Script parser não encontrado: {parser_path}")
        exit(1)

    # Check paser_path has the permission to be executed in the file system
    is_executable = parser_path.stat().st_mode & 0o111 != 0
    if is_executable:
        command = f"{parser_path}"
    elif parser_path.suffix == ".py":
        command = f"uv run --script {parser_path}"
    else:
        out.error(f"Não tenho certeza de como executar: {parser_path}")
        exit(1)

    return command


def parse_instance(parser_cmd: str, inst_path: Path) -> None:
    """
    Chamar o parser para coletar as informações da pasta inst_path.
    Ao final, deve gerar um arquivo res.csv dentro de inst_path.

    parser_path: caminho para o script do parser que recebe o diretório da instância
    como primeiro e único argumento.
    Se parser_path for executável, chamar diretamente.
    Se terminar em .py, chamar com `uv run --script`.
    """
    command = f"{parser_cmd} {inst_path}"
    try:
        result = subprocess.run(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if result.stdout and result.returncode != 0:
            out.log(f"> {inst_path}:\n{result.stdout}")
        if result.stderr and result.returncode != 0:
            out.error(f"!> {inst_path}:\n{result.stderr}")
    except Exception as e:
        out.error(f"Erro ao executar o parser em {inst_path}: {e}")
        exit(1)


def gather_results(
    raw_logs_dir: Path,
    parsed_logs_csv: Path,
) -> None:
    """
    Para da instância em raw_logs_dir, lê o res.csv gerado pelo parser e junta em um dataframe.
    Escreve esse dataframe em parsed_logs_csv.
    """
    out.log("Agregando resultados...")
    all_results = []

    for inst_dir in raw_logs_dir.iterdir():
        if not inst_dir.is_dir():
            continue

        res_csv_path = inst_dir / "res.csv"
        if not res_csv_path.exists():
            out.error(f"Arquivo res.csv não encontrado em {inst_dir}, pulando...")
            continue

        try:
            df = pd.read_csv(res_csv_path)
            df["instance_name"] = inst_dir.name
            all_results.append(df)
        except Exception as e:
            out.error(f"Erro ao ler {res_csv_path}: {e}")
            continue

    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        final_df.to_csv(parsed_logs_csv, index=False)
        out.info(f"Resultados agregados escritos em {parsed_logs_csv}")

        # Create or update the symbolic link to the last results CSV
        try:
            symlink_path = Path.home() / "last_results.csv"
            symlink_path.unlink(missing_ok=True)  # Remove existing symlink if it exists

            symlink_path.symlink_to(parsed_logs_csv.resolve())
            out.info(
                f"Link simbólico para os últimos resultados criado em {symlink_path}"
            )
        except Exception as e:
            out.error(f"Erro ao criar link simbólico: {e}")
    else:
        out.warning("Nenhum resultado foi agregado.")


def parse_and_gather(
    raw_logs_dir: Path,
    parsed_logs_csv: Path,
    parser_path: Path,
) -> None:
    """
    Para cada instância em raw_logs_dir, chama o parser e depois agrega os resultados.
    """
    from concurrent import futures

    n_workers = psutil.cpu_count() or 1
    command = get_parser_command(parser_path)
    inst_dirs = [d for d in raw_logs_dir.iterdir() if d.is_dir()]

    with futures.ThreadPoolExecutor(max_workers=n_workers) as executor:
        future_to_inst = {
            executor.submit(parse_instance, command, inst_dir): inst_dir
            for inst_dir in inst_dirs
        }

        for future in track(
            futures.as_completed(future_to_inst),
            description="Parsing instances...",
            total=len(future_to_inst),
            console=out,
        ):
            inst_dir = future_to_inst[future]
            try:
                future.result()
            except Exception as e:
                out.error(f"Erro ao parsear {inst_dir}: {e}")

    gather_results(
        raw_logs_dir=raw_logs_dir,
        parsed_logs_csv=parsed_logs_csv,
    )
