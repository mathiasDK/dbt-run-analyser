import click


manifest_file = click.argument('manifest_file', type=click.Path(exists=True))
log_file = click.argument('log_file', type=click.Path(exists=True))
plot_title = click.option('--title', default='DBT Run Times', help='Title of the plot.')
run_time_starting_point = click.option('--run_time_starting_point', default=0, help='Starting point of the run time. If there are a lot of models it can take some time to plot. By not plotting models before a specific starting point you can save some time.')
run_time_highlight = click.option('--run_time_highlight', default=1e6, help='Threshold to highlight run times. If the model run time is greater than this value, it will be highlighted.')
run_time_show_model_name = click.option('--run_time_show_model_name', default=0, help='Threshold to show model names. If the model run time is greater than this value, the model name will be shown.')
