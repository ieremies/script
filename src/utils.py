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


def build_all(build_configs: List[BuildConfig], project_config: ProjectConfig) -> None:
    """
    Para cada build, executa o comando de build.
    Se terminar com sucesso, build_configs.executable será do tipo Path com o caminho para o executável (ou script python)
    """

    import subprocess

    with cd(project_config.location):
        for build in build_configs:
            out.print(f"> {build.build_command} ", end="")
            try:
                subprocess.run(
                    build.build_command,
                    shell=True,
                    check=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                )
                out.success(" sucesso!")
            except subprocess.CalledProcessError as e:
                out.error(f"\nErro durante o build:\n{e.stderr.decode()}")
                exit(1)

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
