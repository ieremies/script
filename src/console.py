from typing import Any

from rich.console import Console
from rich.theme import Theme


class MyConsole(Console):
    """
    Uma classe wrapper para o Rich Console que fornece métodos
    de log semânticos (info, success, warning, error)
    usando o método .log() do Rich.
    """

    # 1. Adicionamos 'info' ao tema para o método .info()
    theme = {"info": "cyan", "success": "green", "warning": "yellow", "error": "red"}

    # 2. Instanciamos os consoles da classe com o tema
    stdout = Console(theme=Theme(theme))
    stderr = Console(stderr=True, theme=Theme(theme))

    def info(self, *args: Any) -> None:
        self.stdout.log(*args, style="info")

    def success(self, *args: Any) -> None:
        self.stdout.log(*args, style="success")

    def warning(self, *args: Any) -> None:
        self.stderr.log(*args, style="warning")

    def error(self, *args: Any) -> None:
        self.stderr.log(*args, style="error")


out = MyConsole()
