import concurrent.futures
import datetime
import itertools
import os
import platform
import sys
import time
import timeit
import warnings
from multiprocessing import Pipe, Process
from pathlib import Path

import click
import ibis
import pandas as pd
import psutil

warnings.filterwarnings("ignore")  # ibis throws some warnings
# assumes that submodule is present in parent directory
sys.path.append("tpc-queries")

from ibis_tpc import (
    h01,
    h02,
    h03,
    h04,
    h05,
    h06,
    h07,
    h08,
    h09,
    h10,
    h11,
    h12,
    h13,
    h14,
    h15,
    h16,
    h17,
    h18,
    h19,
    h20,
    h21,
    h22,
)

BACKENDS = {"datafusion": ibis.datafusion.connect, "duckdb": ibis.duckdb.connect}

QUERIES_TPCH = {
    "h01": h01.tpc_h01,
    "h02": h02.tpc_h02,
    "h03": h03.tpc_h03,
    "h04": h04.tpc_h04,
    "h05": h05.tpc_h05,
    "h06": h06.tpc_h06,
    "h07": h07.tpc_h07,
    "h08": h08.tpc_h08,
    "h09": h09.tpc_h09,
    "h10": h10.tpc_h10,
    "h11": h11.tpc_h11,
    "h12": h12.tpc_h12,
    "h13": h13.tpc_h13,
    "h14": h14.tpc_h14,
    "h15": h15.tpc_h15,
    "h16": h16.tpc_h16,
    "h17": h17.tpc_h17,
    "h18": h18.tpc_h18,
    "h19": h19.tpc_h19,
    "h20": h20.tpc_h20,
    "h21": h21.tpc_h21,
    "h22": h22.tpc_h22,
}


class PowercapRapl(Process):
    def __init__(self, pipe, *args, **kw):
        self.pipe = pipe
        super(PowercapRapl, self).__init__(*args, **kw)

    def run(self):
        self.pipe.send(0)
        stop = False
        energy_uj = []
        start_time = timeit.default_timer()
        while True:
            with open(
                "/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0/energy_uj"
            ) as rapl:
                energy_uj.append(int(rapl.read()))
            if stop:
                if energy_uj[0] > energy_uj[-1]:
                    energy_uj[-1] = (
                        energy_uj[-1] + 65532610987
                    )  # fix: this needs to read the max buffer value from intel-rapl:0/
                stop_time = timeit.default_timer()
                self.pipe.send((energy_uj[-1] - energy_uj[0], stop_time - start_time))
                break
            stop = self.pipe.poll(0.1)


class PowercapRaplProfiler:
    def __init__(self):
        self.results = []
        self.total_time = None

    def __enter__(self):
        self.child_conn, self.parent_conn = Pipe()
        p = PowercapRapl(self.child_conn)
        p.start()
        self.parent_conn.recv()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.parent_conn.send(0)
        self.results, self.total_time = self.parent_conn.recv()
        return False


def setup_tpch_db(datadir, engine="duckdb", threads=os.cpu_count()):
    db = BACKENDS.get(engine)()
    tables = [
        "customer",
        "lineitem",
        "nation",
        "orders",
        "part",
        "partsupp",
        "region",
        "supplier",
    ]
    for t in tables:
        path = datadir / "raw" / f"{t}.parquet"
        db.register(f"{path}", t)
    #db.raw_sql(f"PRAGMA threads={threads};")
    return db


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


def is_powercap_available():
    if (
        (platform.processor() == "x86_64")
        and platform.system() == "Linux"
        and os.geteuid() == 0
        and os.path.exists("/sys/devices/virtual/powercap/intel-rapl/intel-rapl:0")
    ):
        return True
    else:
        return False


def timed_run(query, datadir, engine, threads):
    db = setup_tpch_db(datadir, engine, threads)
    # create a temporary duckdb database to genrate sql string
    backend = setup_tpch_db(datadir) 
    #result = query.execute()  # TODO validate
    query = QUERIES_TPCH[query](backend)
    sql = str(ibis.to_sql(query))
    start_time_process = timeit.default_timer()
    start_time_cpu = time.process_time()
    db._context.sql(sql).to_pandas()
    total_time_cpu = time.process_time() - start_time_cpu
    total_time_process = timeit.default_timer() - start_time_process
    return total_time_process, total_time_cpu


def execute(query, powermetrics, datadir, engine, threads, comment):
    if powermetrics and is_powercap_available():
        with PowercapRaplProfiler() as power:
            total_time_process, total_time_cpu = timed_run(
                query, datadir, engine, threads
            )
        power_cpu = {
            "cpu_mJ": power.results / 10**3,
            "power_mW": power.results / power.total_time / 10**3,
        }
    else:
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
    with concurrent.futures.ProcessPoolExecutor(max_workers=1) as e:
        f = e.submit(f, *args)
        try:
            concurrent.futures.wait([f])
            data = f.result()
        except Exception as e:
            #raise (e)
            data  = {
        "name": args[0],
        "threads": args[1],
        "run_date": datetime.datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
        "total_time_process": None,
        "total_time_cpu": None,
        "comment": args[5],
        "success": False,
    }
    return data


@click.command()
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
def tpch(datadir, powermetrics, engines, queries, threads, comment, repeat):
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
    click.echo(df.to_csv(index=False))


if __name__ == "__main__":
    tpch()
