import typer
from rich.traceback import install
from typer import Argument as Arg
from typer import Option as Opt

from src.config import load_config
from src.console import out

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
    build_all(config.build)
    # 4. Garante que as instâncias existem (possivelmente clonando repositórios)
    # e que garante que todas as instâncias da(s) classe(s) existem
    get_instances(config.instances)
    # 5. (opcional) Garante que o script de parser.py existe
    get_parser_script(config.project)
    # 7. Cria, se não existir, o diretório de resultados
    ...


@app.command()
def parse(
    input_dir: str = Arg(..., help="Caminho para o diretório de entrada."),
    parser_script: str = Arg(..., help="Caminho para o script do parser."),
): ...


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
