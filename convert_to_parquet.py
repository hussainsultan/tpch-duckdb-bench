from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.compute as pac
import pyarrow.csv as pc
import pyarrow.parquet as pq

tables = [
    "part",
    "partsupp",
    "region",
    "supplier",
    "lineitem",
    "orders",
    "customer",
    "nation",
]


PARTITIONS = {"orders": ["o_orderdate"], "lineitem": ["partition_col"]}


def get_tpch_schema_duckdb():
    schema = {
        "part": {
            "p_partkey": "BIGINT",
            "p_name": "VARCHAR",
            "p_mfgr": "VARCHAR",
            "p_brand": "VARCHAR",
            "p_type": "VARCHAR",
            "p_size": "BIGINT",
            "p_container": "VARCHAR",
            "p_retailprice": "DOUBLE",
            "p_comment": "VARCHAR",
        },
        "supplier": {
            "s_suppkey": "BIGINT",
            "s_name": "VARCHAR",
            "s_address": "VARCHAR",
            "s_nationkey": "BIGINT",
            "s_phone": "VARCHAR",
            "s_acctbal": "DOUBLE",
            "s_comment": "VARCHAR",
        },
        "partsupp": {
            "ps_partkey": "BIGINT",
            "ps_suppkey": "BIGINT",
            "ps_availqty": "BIGINT",
            "ps_supplycost": "DOUBLE",
            "ps_comment": "VARCHAR",
        },
        "customer": {
            "c_custkey": "BIGINT",
            "c_name": "VARCHAR",
            "c_address": "VARCHAR",
            "c_nationkey": "BIGINT",
            "c_phone": "VARCHAR",
            "c_acctbal": "DOUBLE",
            "c_mktsegment": "VARCHAR",
            "c_comment": "VARCHAR",
        },
        "orders": {
            "o_orderkey": "BIGINT",
            "o_custkey": "BIGINT",
            "o_orderstatus": "VARCHAR",
            "o_totalprice": "DOUBLE",
            "o_orderdate": "DATE",
            "o_orderpriority": "VARCHAR",
            "o_clerk": "VARCHAR",
            "o_shippriority": "BIGINT",
            "o_comment": "VARCHAR",
            "o_null": "VARCHAR",
        },
        "lineitem": {
            "l_orderkey": "BIGINT",
            "l_partkey": "BIGINT",
            "l_suppkey": "BIGINT",
            "l_linenumber": "BIGINT",
            "l_quantity": "DOUBLE",
            "l_extendedprice": "DOUBLE",
            "l_discount": "DOUBLE",
            "l_tax": "DOUBLE",
            "l_returnflag": "VARCHAR",
            "l_linestatus": "VARCHAR",
            "l_shipdate": "DATE",
            "l_commitdate": "DATE",
            "l_receiptdate": "DATE",
            "l_shipinstruct": "VARCHAR",
            "l_shipmode": "VARCHAR",
            "l_comment": "VARCHAR",
        },
        "nation": {
            "n_nationkey": "BIGINT",
            "n_name": "VARCHAR",
            "n_regionkey": "BIGINT",
            "n_comment": "VARCHAR",
        },
        "region": {
            "r_regionkey": "BIGINT",
            "r_name": "VARCHAR",
            "r_comment": "VARCHAR",
            "r_null": "VARCHAR",
        },
    }
    return schema


def convert_to_parquet():
    schema = get_tpch_schema_duckdb()
    con = duckdb.connect()
    for t in tables:
        f = Path(f"sf500/{t}.tbl")
        con.sql(
            f"create view {t} as select * from read_csv('{f}', delim='|', columns={schema[t]});"
        )
        directory_name = f.name.split(".tbl")[0]
        con.sql(f"COPY {t} to '{f.parent/directory_name}.parquet' (FORMAT PARQUET);")


if __name__ == "__main__":
    convert_to_parquet()
