import typer
from athena_mvsh import __appname__
from athena_mvsh.app_cli.doc import app as app_doc
from athena_mvsh.app_cli.version import app as app_version

app = typer.Typer()
app.add_typer(app_version)
app.add_typer(app_doc, name='doc', help='Ajuda sobre a sintaxe sql do Athena')


def main():
    app(prog_name=__appname__)
