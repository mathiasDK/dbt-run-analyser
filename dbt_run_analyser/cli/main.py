import click
from dbt_run_analyser.plot import ShowDBTRun
from dbt_run_analyser.dag import DAG
from . import params as p

@click.group()
def cli():
    """CLI for dbt_run_analyser."""
    pass

SINGLE_MODEL_SELECTOR = ('-m', '--model')

@click.command("plot-run-times")
@p.manifest_file
@p.log_file
@p.plot_title
@p.run_time_starting_point
@p.run_time_highlight
@p.run_time_show_model_name
def plot_run_times(manifest_file, log_file, title, run_time_starting_point, run_time_highlight, run_time_show_model_name):
    """Plot the run times of models using the manifest and log files."""
    show_run = ShowDBTRun(manifest_path=manifest_file, log_file=log_file)
    fig = show_run.plot_run_time(title=title, run_time_starting_point=run_time_starting_point, run_time_highlight=run_time_highlight, run_time_show_model_name=run_time_show_model_name)
    fig.show()

@click.command("plot-critical-path")
@p.manifest_file
@p.log_file
@click.option(*SINGLE_MODEL_SELECTOR, type=click.STRING, help = "The models from which the critical path must be found.")
@p.plot_title
@p.run_time_starting_point
@p.run_time_highlight
@p.run_time_show_model_name
def plot_critical_path(manifest_file, log_file, model, title, run_time_starting_point, run_time_highlight, run_time_show_model_name):
    """Plot the critical path of a model using the manifest and log files."""
    show_run = ShowDBTRun(manifest_path=manifest_file, log_file=log_file)
    fig = show_run.plot_critical_path(model, title=title, run_time_starting_point=run_time_starting_point, run_time_highlight=run_time_highlight, run_time_show_model_name=run_time_show_model_name)
    fig.show()

@click.command("get-all-end-nodes")
@p.manifest_file
def get_all_end_nodes(manifest_file):
    """Returning a list of all nodes which have no downstream dependencies."""
    d = DAG(manifest_path=manifest_file)
    return d.get_all_end_nodes()
    

@click.command("help")
def help_command():
    """Show help information for the CLI."""
    click.echo(cli.get_help(click.Context(cli)))

cli.add_command(plot_run_times)
cli.add_command(plot_critical_path)
cli.add_command(get_all_end_nodes)
cli.add_command(help_command)

if __name__ == '__main__':
    cli()
