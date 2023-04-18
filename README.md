## `tpch-duckdb-bench`
A small utility to run TPCH benchmarks with DuckDB accompanying my [blog post](https://hussainsultan.com/posts/efficient-duckdb). 

## Example Results
I have an example result set in `results/consolidated.csv` using TPCH data at SF100, SF500 and SF1000 [desktop machine](https://pcpartpicker.com/user/Chemisist/saved/#view=JxrNP6).

### Prepare Data
The following script will download the data and parse it into parquet files

### Run

```
❯ python run.py --help
Usage: run.py [OPTIONS]
Options:
  --powermetrics / --no-powermetrics
                                  Flag to get cpu and power metrics on OSX
                                  [default: no-powermetrics]
  --threads TEXT                  comma seperated list of threads to run e.g.
                                  2,4,8  [default: 8]
  --datadir TEXT                  [default: data]
  --help                          Show this message and exit.
```

