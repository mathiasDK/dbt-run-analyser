import click
from ..plot import ShowDBTRun

@click.group()
def cli():
    """CLI for dbt_run_analyser."""
    pass

SINGLE_MODEL_SELECTOR = ('-m', '--model')

@click.command("plot-run-times")
@click.argument('manifest_file', type=click.Path(exists=True))
@click.argument('log_file', type=click.Path(exists=True))
@click.option('--title', default='DBT Run Times', help='Title of the plot.')
@click.option('--run_time_starting_point', default=0, help='Starting point of the run time. If there are a lot of models it can take some time to plot. By not plotting models before a specific starting point you can save some time.')
@click.option('--run_time_highlight', default=1e6, help='Threshold to highlight run times. If the model run time is greater than this value, it will be highlighted.')
@click.option('--run_time_show_model_name', default=0, help='Threshold to show model names. If the model run time is greater than this value, the model name will be shown.')
def plot_run_times(manifest_file, log_file, title, run_time_starting_point, run_time_highlight, run_time_show_model_name):
    """Plot the run times of models using the manifest and log files."""
    show_run = ShowDBTRun(manifest_path=manifest_file, log_file=log_file)
    fig = show_run.plot_run_time(title=title, run_time_starting_point=run_time_starting_point, run_time_highlight=run_time_highlight, run_time_show_model_name=run_time_show_model_name)
    fig.show()

@click.command("plot-critical-path")
@click.argument('manifest_file', type=click.Path(exists=True))
@click.argument('log_file', type=click.Path(exists=True))
@click.option(*SINGLE_MODEL_SELECTOR, type=click.STRING)
@click.option('--title', default='DBT Run Times', help='Title of the plot.')
@click.option('--run_time_starting_point', default=0, help='Starting point of the run time. If there are a lot of models it can take some time to plot. By not plotting models before a specific starting point you can save some time.')
@click.option('--run_time_highlight', default=1e6, help='Threshold to highlight run times. If the model run time is greater than this value, it will be highlighted.')
@click.option('--run_time_show_model_name', default=0, help='Threshold to show model names. If the model run time is greater than this value, the model name will be shown.')
def plot_critical_path(manifest_file, log_file, model, title, run_time_starting_point, run_time_highlight, run_time_show_model_name):
    """Plot the critical path of a model using the manifest and log files."""
    show_run = ShowDBTRun(manifest_path=manifest_file, log_file=log_file)
    fig = show_run.plot_critical_path(model, title=title, run_time_starting_point=run_time_starting_point, run_time_highlight=run_time_highlight, run_time_show_model_name=run_time_show_model_name)
    fig.show()

@click.command("help")
def help_command():
    """Show help information for the CLI."""
    click.echo(cli.get_help(click.Context(cli)))

cli.add_command(plot_run_times)
cli.add_command(plot_critical_path)
cli.add_command(help_command)

if __name__ == '__main__':
    cli()
