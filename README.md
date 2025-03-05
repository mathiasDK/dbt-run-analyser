# dbt Run Analyser

![Coverage Badge](docs/coverage.svg)

This package can help you analyse your dbt runs, and help you ensure you're using your threads most optimal.

## Installation
`pip install dbt-run-analyser`

### Example
See a set of examples in the [plot.ipynb](https://github.com/mathiasDK/dbt-run-analyser/blob/master/examples/plot.ipynb) file.

## Using the CLI

The `dbt-run-analyser` CLI allows you to analyze your dbt run logs and manifest files to gain insights into your dbt runs. Here is a basic example of how to use the CLI:

1. **Plot Run Times:**
    To plot the run times of your dbt models, use the following command:
    ```sh
    dbt-run-analyser plot-run-times "path/to/manifest.json" "path/to/dbt_run.log"
    ```

2. **Plot Critical Path:**
    Show the critical path through the run:
    ```sh
    dbt-run-analyser plot-critical-path "path/to/manifest.json" "path/to/dbt_run.log" --model "order_wide"
    ```

Replace `"path/to/manifest.json"` and `"path/to/dbt_run.log"` with the actual paths to your dbt manifest file and run log file, respectively.

## Functionalities to come
- cli interface.
- Improvements/decrease in run time if threads are added or removed.
- Identification of bottlenecks.