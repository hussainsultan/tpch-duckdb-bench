import concurrent.futures
import datetime
import itertools
import platform
import sys
import time
import timeit
from pathlib import Path

import click
import pandas as pd
import psutil

# assumes that submodule is present in parent directory
sys.path.append("polars_tpch")

from polars_tpch.polars_queries import (
    q1,
    q2,
    q3,
    q4,
    q5,
    q6,
    q7,
    q8,
    q9,
    q10,
    q11,
    q12,
    q13,
    q14,
    q15,
    q16,
    q17,
    q18,
    q19,
    q20,
    q21,
    q22,
)


QUERIES_TPCH = {
    "h01": q1,
    "h02": q2,
    "h03": q3,
    "h04": q4,
    "h05": q5,
    "h06": q6,
    "h07": q7,
    "h08": q8,
    "h09": q9,
    "h10": q10,
    "h11": q11,
    "h12": q12,
    "h13": q13,
    "h14": q14,
    "h15": q15,
    "h16": q16,
    "h17": q17,
    "h18": q18,
    "h19": q19,
    "h20": q20,
    "h21": q21,
    "h22": q22,
}


def setup_tpch_db(datadir, query):
    path = datadir / "raw" 
    orig_line_item = query.utils.get_line_item_ds 
    def set_new_default(new_default=str(path)):
        return orig_line_item(new_default)
    query.utils.get_line_item_ds = set_new_default
    query.utils.test_results = lambda x,y: y

    orig_orders = query.utils.get_orders_ds 
    def set_new_default(new_default=str(path)):
        return orig_orders(new_default)
    query.utils.get_orders_ds = set_new_default

    orig_customer = query.utils.get_customer_ds 
    def set_new_default(new_default=str(path)):
        return orig_customer(new_default)
    query.utils.get_customer_ds = set_new_default
    orig_region = query.utils.get_region_ds 
    def set_new_default(new_default=str(path)):
        return orig_region(new_default)
    query.utils.get_region_ds = set_new_default
    orig_nation = query.utils.get_nation_ds 
    def set_new_default(new_default=str(path)):
        return orig_nation(new_default)
    query.utils.get_nation_ds = set_new_default
    orig_supplier = query.utils.get_supplier_ds 
    def set_new_default(new_default=str(path)):
        return orig_supplier(new_default)
    query.utils.get_supplier_ds = set_new_default
    orig_part = query.utils.get_part_ds 
    def set_new_default(new_default=str(path)):
        return orig_part(new_default)
    query.utils.get_part_ds = set_new_default
    orig_partsupp = query.utils.get_part_supp_ds 
    def set_new_default(new_default=str(path)):
        return orig_partsupp(new_default)
    query.utils.get_part_supp_ds = set_new_default
    return query


def platform_info():
    return {
        "machine": platform.machine(),
        "version": platform.version(),
        "platform": platform.platform(),
        "system": platform.system(),
        "cpu_count": psutil.cpu_count(),
        "memory": psutil.virtual_memory().total,
        "processor": platform.processor(),
    }


def timed_run(query, datadir, engine, threads):
    query = setup_tpch_db(datadir, QUERIES_TPCH[query])
    start_time_process = timeit.default_timer()
    start_time_cpu = time.process_time()
    query.q()
    total_time_cpu = time.process_time() - start_time_cpu
    total_time_process = timeit.default_timer() - start_time_process
    return total_time_process, total_time_cpu


def execute(query, powermetrics, datadir, engine, threads, comment):
    total_time_process, total_time_cpu = timed_run(query, datadir, engine, threads)
    power_cpu = {}

    run_stats = {
        "name": query,
        "threads": threads,
        "run_date": datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "total_time_process": total_time_process,
        "total_time_cpu": total_time_cpu,
        "comment": comment,
        "success": True,
    }
    run_stats.update(power_cpu)
    return run_stats


def run_method_in_subprocess(f, *args):
    try:
        data = f(*args)
    except Exception as e:
        raise (e)
        data  = {
    "name": args[0],
    "threads": args[1],
    "run_date": datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
    "total_time_process": None,
    "total_time_cpu": None,
    "comment": args[5],
    "success": False,}
    return data


@click.command()
@click.option(
    "--logfile",
    default="log.log",
    show_default=True,
    help="file to log to",
)
@click.option(
    "--repeat",
    default=1,
    show_default=True,
    help="Number of benchmark runs",
)
@click.option(
    "--comment",
    default="",
    show_default=True,
    help="helpful comments to be added to benchmark output",
)
@click.option(
    "--threads",
    default="8",
    show_default=True,
    help="comma seperated list of threads to run to run",
)
@click.option(
    "--queries",
    default="h01,h02,h03,h04,h05,h06,h07,h08,h09,h10,h11,h12,h13,h14,h15,h16,h17,h18,h19,h20,h21,h22",
    show_default=True,
    help="comma seperated list of questions to run",
)
@click.option(
    "--engines",
    default="duckdb",
    show_default=True,
    help="comma seperated list of datadirs to run e.g. duckdb, polars",
)
@click.option(
    "--powermetrics/--no-powermetrics",
    default=False,
    show_default=True,
    help="Flag to get cpu and power metrics on OSX",
)
@click.option(
    "--datadir",
    default="data",
    show_default=True,
    help="comma seperated list of datadirs to run e.g. 2,4,8",
)
def tpch(datadir, powermetrics, engines, queries, threads, comment, repeat, logfile):
    datadirs = [s for s in datadir.split(",")]
    engines = [s for s in engines.split(",")]
    queries = [s for s in queries.split(",")]
    threads = [int(s) for s in threads.split(",")]
    runs = []
    for runno in range(1, repeat + 1):
        for datadir, engine, thread in itertools.product(datadirs, engines, threads):
            datadir = Path(datadir)
            stats = [
                run_method_in_subprocess(
                    execute, query, powermetrics, datadir, engine, thread, comment
                )
                for query in queries
            ]

            data = {
                **platform_info(),
                "runs": stats,
                "datadir": datadir,
                "db": engine,
                "runno": runno,
            }
            runs.append(data)

    df = pd.json_normalize(runs, ["runs"], meta=["datadir", "db", "runno"])
    df.to_csv(logfile, index=False)


if __name__ == "__main__":
    tpch()
