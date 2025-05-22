import typer
from rich.console import Console
from athena_mvsh import __version__, __author__, __appname__

app = typer.Typer()
terminal = Console()


@app.command()
def version():
    terminal.print(f'Version: {__version__}, Author: {__author__}, App: {__appname__}')
