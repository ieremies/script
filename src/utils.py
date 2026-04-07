import os
from contextlib import contextmanager
from pathlib import Path
from typing import List, Optional, Union

from src.config import BuildConfig, InstanceConfig, ProjectConfig
from src.console import out


@contextmanager
def cd(path: Union[Path, str]):
    """Muda o diretório de trabalho atual para o caminho especificado e retorna ao diretório original ao sair do contexto."""
    old_cwd = Path.cwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old_cwd)


def get_path_or_clone(location: Union[str, Path], default_name: Optional[str] = None) -> Path:
    """Dado uma string, que pode ser um caminho local ou um url de repositório git,
    retorna-se um Path local, possivelmente clonando o repositório.
    """

    # Verifica se é um caminho local
    path = Path(location).expanduser()
    if path.exists():
        return path.resolve()

    # Se não existir, tenta clonar como repositório git

    repo_url = str(location)
    if repo_url.startswith("gh:"):
        repo_url = "https://github.com/" + repo_url[3:] + ".git"

    clone_path = Path("./") / (default_name or str(location).split("/")[-1])
    if clone_path.exists():
        # out.print(f"O diretório {clone_path} já existe. Pulando clonagem.")
        return clone_path.resolve()

    out.print(f"Clonando repositório {repo_url} para {clone_path}...")
    try:
        import git  # type: ignore

        git.Repo.clone_from(repo_url, clone_path)
    except Exception as e:
        out.error(f"Erro ao clonar o repositório: {e}")
        exit(1)

    out.print(f"Repositório clonado com sucesso em: {clone_path.resolve()}")
    return clone_path.resolve()


def get_project_root(conf: ProjectConfig) -> None:
    """Garante que a root do projeto existe (possivelmente clonando o repositório)."""

    # se conf.location é um Path, não precisa fazer nada
    if isinstance(conf.location, Path):
        out.print(f"Projeto localizado em: {conf.location}")
        return

    conf.location = get_path_or_clone(conf.location, "project_root")
    out.print(f"Projeto disponível em: {conf.location}")


def build_target(build: BuildConfig, project_config: ProjectConfig) -> None:
    """
    Executa o comando de build para um único alvo.
    Se terminar com sucesso, build.executable será do tipo Path com o caminho para o executável (ou script python)
    """

    import subprocess

    with cd(project_config.location):
        if build.git_ref:
            out.print(f"Checking out {build.git_ref}...")
            try:
                subprocess.run(
                    f"git checkout {build.git_ref}",
                    shell=True,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
            except subprocess.CalledProcessError as e:
                out.error(f"Erro ao fazer checkout de {build.git_ref}:\n{e.stderr.decode()}")
                exit(1)

        from rich.progress import (
            BarColumn,
            Progress,
            SpinnerColumn,
            TextColumn,
            TimeElapsedColumn,
        )
        import re

        out.print(f"> {build.build_command}")
        
        process = subprocess.Popen(
            build.build_command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, # merge stderr for complete logs
            text=True,
            bufsize=1,
            universal_newlines=True,
        )

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            console=out,
        ) as progress:
            task = progress.add_task("Building...", total=100)

            for line in process.stdout:
                line = line.strip()
                if not line:
                    continue
                
                # Procura por progresso do CMake, por exemplo "[ 15%]" ou "[100%]"
                match = re.search(r'\[\s*(\d+)%\]', line)
                if match:
                    percent = int(match.group(1))
                    progress.update(task, completed=percent)
                else:
                    # Atualiza a descrição com a linha atual truncada
                    short_line = line[:60] + "..." if len(line) > 60 else line
                    progress.update(task, description=f"Building... {short_line}")

            process.wait()

            if process.returncode != 0:
                out.error(f"\nErro durante o build (código {process.returncode}). Veja a saída acima.")
                exit(1)
            else:
                progress.update(task, completed=100, description="Build concluído com sucesso!")

        # BUG isso aqui vai dar merda
        build.executable = Path(build.executable).resolve()


def get_instances(conf: InstanceConfig) -> None:
    """
    Garante que as instâncias existem (possivelmente clonando repositórios)
    e garante que todas as instâncias da(s) classe(s) existem.
    """

    conf.location = get_path_or_clone(conf.location, "inst")
    conf.instances = {} if conf.instances is None else conf.instances
    out.print(f"Instâncias disponíveis em: {conf.location}")

    for class_name in conf.classes:
        # {conf.location}/class_name tem que ser ou um dirtório ou um arquivo
        class_path = conf.location / class_name
        if not class_path.exists():
            out.error(
                f"A classe de instância '{class_name}' não existe em {conf.location}"
            )
            exit(1)

        # se é um diretório, as instâncias da classe são os arquivos dentro dele
        if class_path.is_dir():
            conf.instances[class_name] = list(class_path.iterdir())
        # se é um arquivo, cada linha é uma instância, que deve ser um arquivo
        # em {conf.location}/ ou em {conf.location}/all/
        else:
            instance_paths = []
            for line in class_path.read_text().splitlines():
                inst_path = conf.location / line
                all_path = conf.location / "all" / line
                if inst_path.exists():
                    instance_paths.append(inst_path)
                elif all_path.exists():
                    instance_paths.append(all_path)
                else:
                    out.error(
                        f"A instância '{line}' da classe '{class_name}' não existe."
                    )
                    exit(1)
            conf.instances[class_name] = instance_paths

        out.info(
            f"Classe '{class_name}' com {len(conf.instances[class_name])} instâncias"
        )
