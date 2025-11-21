import sys
from pathlib import Path
from typing import Dict, List, Optional

import tomllib
from pydantic import BaseModel, ValidationError


class ProjectConfig(BaseModel):
    """Configurações do [project]"""

    location: str | Path
    parser: Optional[str | Path] = None
    id: Optional[str] = None


class InstanceConfig(BaseModel):
    """Configurações das [instances]"""

    location: str | Path
    classes: List[str]
    # {"class_name": [list, of, instance, paths]}
    instances: Optional[Dict[str, List[Path]]] = None


class BuildConfig(BaseModel):
    """Configurações de um [[build]]"""

    name: str
    type: str
    build_command: str
    executable: str | Path
    run_template: str
    description: Optional[str] = None


class ExperimentConfig(BaseModel):
    """O Modelo Raiz que junta tudo"""

    project: ProjectConfig
    instances: InstanceConfig
    build: List[BuildConfig]


def load_config(config_path: str) -> Optional[ExperimentConfig]:
    """
    Carrega e valida o arquivo de configuração TOML usando Pydantic.
    Retorna um objeto ExperimentConfig em sucesso, ou None em falha.
    """
    try:
        with open(config_path, "rb") as f:
            raw_config_dict = tomllib.load(f)

        # Esta é a mágica: Pydantic analisa o dicionário
        # e o transforma em um objeto Python com tipagem.
        config = ExperimentConfig(**raw_config_dict)
        return config

    except FileNotFoundError:
        print(f"Erro: Arquivo de configuração não encontrado em: {config_path}")
    except ValidationError as e:
        print(f"Erro: Arquivo de configuração '{config_path}' é inválido:")
        print(e)
    except tomllib.TOMLDecodeError as e:
        print(f"Erro: O arquivo TOML está mal formatado: {e}")
    except Exception as e:
        print(f"Erro inesperado ao carregar a configuração: {e}")

    sys.exit(1)  # Encerra o script se a configuração falhar
