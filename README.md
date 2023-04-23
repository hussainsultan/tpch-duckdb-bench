# `tpch-duckdb-bench`
A small utility to run TPCH benchmarks with DuckDB accompanying my [blog post](https://hussainsultan.com/posts/efficient-duckdb). 

## Example Results
I have an example result set in `results/consolidated.csv` using TPCH data at SF100, SF500 and SF1000 [desktop machine](https://pcpartpicker.com/user/Chemisist/saved/#view=JxrNP6).

## Prepare Data
I used the [tpch docker](https://github.com/databloom-ai/TPCH-Docker) container to prepare the text at appropriate scale factor. For example, for SF100:

```
❯ docker run -v `pwd`/data:/data -it --rm ghcr.io/databloom-ai/tpch-docker:main -vf -s 100 
```
To convert to parquet, I used `convert_to_parquet.py`

## Fetch TPC-H queries
Before we can run the benchmarks, we fetch the queries from the `ibis/tpc-queries` repo:
```
git submodule update --init --recursive
```
## Run
The `run.py` cli will run per scale factor (datadir):
```
❯ python run.py --help
Usage: run.py [OPTIONS]

Options:
  --repeat INTEGER                Number of benchmark runs  [default: 1]
  --comment TEXT                  helpful comments to be added to benchmark
                                  output
  --threads TEXT                  comma seperated list of threads to run to
                                  run  [default: 8]
  --queries TEXT                  comma seperated list of questions to run
                                  [default: h01,h02,h03,h04,h05,h06,h07,h08,h0
                                  9,h10,h11,h12,h13,h14,h15,h16,h17,h18,h19,h2
                                  0,h21,h22]
  --engines TEXT                  comma seperated list of datadirs to run e.g.
                                  duckdb, polars  [default: duckdb]
  --powermetrics / --no-powermetrics
                                  Flag to get cpu and power metrics on OSX
                                  [default: no-powermetrics]
  --datadir TEXT                  comma seperated list of datadirs to run e.g.
                                  2,4,8  [default: data]
  --help                          Show this message and exit.
```
