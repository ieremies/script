from typing import Any

from rich.console import Console
from rich.theme import Theme


class MyConsole(Console):
    """
    Uma classe wrapper para o Rich Console que fornece métodos
    de log semânticos (info, success, warning, error)
    usando o método .log() do Rich.
    """

    def __init__(self) -> None:
        # 1. Adicionamos 'info' ao tema para o método .info()
        theme = {
            "info": "cyan",
            "success": "green",
            "warning": "yellow",
            "error": "red",
        }
        super().__init__(theme=Theme(theme))

    def info(self, *args: Any) -> None:
        self.print(*args, style="info")

    def success(self, *args: Any) -> None:
        self.print(*args, style="success")

    def warning(self, *args: Any) -> None:
        self.print(*args, style="warning")

    def error(self, *args: Any) -> None:
        # TODO print to stderr
        self.print(*args, style="error", _stack_offset=2)


out = MyConsole()
