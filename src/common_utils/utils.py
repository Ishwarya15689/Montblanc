"""Common utility file for rave-to-gemstone and raven-to-gemstone"""

import json
import os
import sys
import logging
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np
import boto3
from pythonjsonlogger import jsonlogger
import yaml
from pydantic import BaseModel, Extra

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Sequence

from .api import Veeva, merge_response

MAX_WORKERS = 2
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Sequence

from .api import Veeva, merge_response

MAX_WORKERS = 2

_state = {"logger": None, "logger_context": {}}


def fetch_aws_secret(secret_name, region_name):
    """fetch AWS secrets from a secret instance"""
    try:
        session = boto3.session.Session()
        client = session.client(service_name="secretsmanager", region_name=region_name)
        response = client.get_secret_value(SecretId=secret_name)
        secret_dict = json.loads(response["SecretString"])
        return secret_dict
    except Exception as error:
        _state["logger"].exception(
            f"Error occurred while fetching PostgreSQL DB credentials from AWS Secrets Manager"
            f" - {error}"
        )
        raise


def change_date_format(value, from_format, to_format):
    """converting date format"""
    if value == "":
        return None
    date = datetime.strptime(value, from_format)
    converted_date = date.strftime(to_format)
    return converted_date


def get_delta_records(df1, df2, cols):
    """Get records which are present in df1 and not in df2"""
    df1 = df1.replace(
        ["", "None", "nan", "NaN", "NaT", pd.NaT, np.NaN, np.nan, None], "NULL"
    )
    df2 = df2.replace(
        ["", "None", "nan", "NaN", "NaT", pd.NaT, np.NaN, np.nan, None], "NULL"
    )
    df_merged = pd.merge(df1, df2, on=cols, how="left", indicator=True)
    df_delta = df_merged[df_merged["_merge"] == "left_only"]
    df_delta = df_delta.drop("_merge", axis=1)
    if not df_delta.empty:
        df_delta = df_delta.replace("NULL", None)
    return df_delta


def build_upsert_query(df, schema, table, pk, method=None) -> str:
    """Prepare postgres INSERT ON CONFLICT query dynamically.
    method parameter can take two values:
    -   "upsert": for creating insert-on conflict-update query
    -   None: for creating insert-on conflict-do nothing query

    Returns
    -------
    returns upsert query string
    """

    cols = ", ".join(df.columns.tolist())  # get comma separated column list as a string
    querystr1 = f"INSERT INTO {schema}.{table} ({cols}) VALUES %s \n"
    querystr2 = f"ON CONFLICT ({pk})\n"
    if method == "upsert":
        querystr3 = "DO UPDATE SET\n"
        querystr4 = ""
        for col in df.columns:
            querystr4 += col + " = " + "EXCLUDED." + col + ", "
        querystr4 = (
            querystr4.rstrip(", ") + ";"
        )  # remove the last comma and space and append semicolon
        query = querystr1 + querystr2 + querystr3 + querystr4
    else:
        querystr3 = "DO NOTHING;"
        query = querystr1 + querystr2 + querystr3
    return query


def _custom_except_hook(exc_type, exc_value, exc_traceback):
    _state["logger"].error(
        "Error occured: ", exc_info=(exc_type, exc_value, exc_traceback)
    )


def get_logger(logger_name: str, _json: bool = True) -> logging.Logger:
    """Get a logger."""
    if not _state["logger"]:
        logging.basicConfig(level=logging.INFO)
        handler = logging.StreamHandler()

        if _json:
            handler.setFormatter(
                jsonlogger.JsonFormatter(
                    fmt="%(asctime)s %(levelname)s %(name)s %(message)s"
                )
            )

        _logger = logging.getLogger(logger_name)
        _logger.addHandler(handler)
        _logger.propagate = False
        _state["logger"] = logging.LoggerAdapter(_logger, _state["logger_context"])

        sys.excepthook = _custom_except_hook

    return _state["logger"]


def get_list_of_dict(keys, list_of_tuples):
    """This function generates a dictionary from a list of tuples

    Args:
        keys (_type_): list of keys
        list_of_tuples (_type_): list of tuples

    Returns:
        _type_: list of dictionaries
    """
    list_of_dict = [dict(zip(keys, values)) for values in list_of_tuples]
    return list_of_dict


def read_yaml_file(file_path: str) -> dict:
    """
    Function to read the sql yaml file

    Parameters
    ----------
        file_path (str): file path where yaml file is present

    Returns
    -------
        returns dict
    """
    with open(
        os.path.join(file_path),
        "r",
        encoding="utf-8",
    ) as f:
        sql = yaml.load(f, Loader=yaml.SafeLoader)

    return sql


class PostgresDbDetails(BaseModel):
    """
    This class is used to store all Postgres db details
    """

    username: str
    password: str
    host: str
    port: str
    dbname: str

    class Config:
        # to allow extra attributes from aws secret
        extra = Extra.allow


def connect_to_db(postgres_db_details: PostgresDbDetails):
    """
    Utility function to connect postgres db

    Parameters
    ----------
        postgres_db_details (PostgresDbDetails): Postgres db details object

    Returns
    -------
        connection to postgres db
    """
    conn = None
    try:
        conn = psycopg2.connect(
            database=postgres_db_details.dbname,
            host=postgres_db_details.host,
            user=postgres_db_details.username,
            password=postgres_db_details.password,
            port=postgres_db_details.port,
        )
        return conn
    except Exception as error:
        _state["logger"].exception(
            f"Error occurred while connecting to CTMS Outbound DB - {error}"
        )
        raise


def execute_upsert_query_on_db_table(
    postgres_db_details: PostgresDbDetails, query, rows
):
    """
    This is generic implementation for executing upsert query in postgres

    Parameters
    ----------
        postgres_db_details (PostgresDbDetails): Postgres db details object
        query : upsert query
        rows : rows of data

    Returns
    -------
        None
    """
    conn = connect_to_db(postgres_db_details)
    try:
        cursor = conn.cursor()
        execute_values(cursor, query, rows)
        conn.commit()
        cursor.close()
    except Exception as error:
        _state["logger"].exception(f"Error occurred while executing a query - {error}")
        raise
    finally:
        if conn:
            conn.close()


def execute_query_on_db_table(
    db_details: PostgresDbDetails, query: str, method="select"
):
    """
    Utility function to execute query on table

    Parameters
    ----------
        db_details (PostgresDbDetails): postgres db details object
        query (str): string to query database
        method (str, optional): different action performed i.e select, update etc.
                                Defaults to "select".

    Returns
    -------
        None or List of Rows
    """
    conn = connect_to_db(db_details)
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        if method.lower() == "select":
            rows = cursor.fetchall()
            cursor.close()
            return rows
        elif method.lower() in ("insert", "update", "delete"):
            conn.commit()
            cursor.close()
    except Exception as error:
        _state["logger"].exception(f"Error occurred while executing a query - {error}")
        raise
    finally:
        if conn:
            conn.close()


def fetch_specific_col_records_from_table(
    db_details: PostgresDbDetails, table: str, cols: list, sql: str, **kwargs
) -> pd.DataFrame:
    """
    Util funtion to fetch records from specific columns from table

    Parameters
    ----------
        db_details (PostgresDbDetails): postgres db details object
        table (str): table name
        cols (list): List of columns required
        sql (str): Sql query string

    Returns
    --------
        pd.DataFrame: specific columns data from db
    """
    schema = kwargs.get("schema", "rave")
    columns = ",".join(cols)
    query = sql.format(columns, schema, table)

    try:
        rows = execute_query_on_db_table(db_details, query)
        df = pd.DataFrame(rows, columns=cols)
        return df
    except Exception as error:
        _state["logger"].exception(
            f"Error occurred while fetching data from Staging DB - {error}"
        )
        raise


def fetch_dataset_config_from_db(
    db_details: PostgresDbDetails, query: str, source: str, dataset_name: str
) -> dict:
    """
    Util function to fetch config data from db
    Args:
        db_details (PostgresDbDetails): postgres db details object
        query (str): Sql query string
        source (str): source interface
        dataset_name (str): dataset_name

    Returns:
        dict: dict of config
    """
    sql = query.format(source=source, dataset_name=dataset_name)
    rows = execute_query_on_db_table(db_details=db_details, query=sql)
    if len(rows) == 0:
        _state["logger"].error(f"Unable to find config for {source} and {dataset_name}")
        raise ValueError
    return {
        "interval_in_days": rows[0][0],
        "fallback_date": rows[0][1].strftime("%Y-%m-%d"),
    }


def fetch_study_country_from_outbound_db(
    db_details: PostgresDbDetails, query_sql: str
) -> pd.DataFrame:
    """
    Util function to fetch all study country details from outbound db

    Parameters
    ----------
        db_details (PostgresDbDetails): postgres db details object
        query_sql (str): Sql query string

    Returns:
        pd.DataFrame: dataframe of data fetched from db
    """
    try:
        rows = execute_query_on_db_table(db_details, query_sql)
        df = pd.DataFrame(rows, columns=["study_country", "site_name", "study_name"])
        return df
    except Exception as error:
        _state["logger"].exception(
            f"Error occurred while fetching study country list from outbound DB - {error}"
        )
        raise


def get_last_modified_date_timestamp():
    """function to get current timestamp"""
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")


def get_veeva_api_connection(veeva_domain: str = None, **kwargs) -> Veeva:
    """Create a Veeva Vault API object."""
    vault = kwargs.get("vault_secret")
    region_name = kwargs.get("region_name")

    if vault and len(vault) > 0:
        secret = fetch_aws_secret(vault, region_name)
        if veeva_domain is None:
            veeva_domain = secret["veeva_domain"]

    veeva_username = secret["veeva_user_id"]
    veeva_password = secret["veeva_password"]

    veeva = Veeva(veeva_domain, **kwargs)
    veeva.authenticate(veeva_username, veeva_password)

    return veeva


def upsert(
    object_type: str,
    data: Sequence[dict],
    id_param: str,
    method: str,
    chunk_size: int = 500,
    veeva: Veeva = None,
    delete_inactive_records: bool = False,
) -> Sequence[dict]:
    """Insert/update data into Veeva and report any failed items."""
    veeva = veeva or _state["veeva"].veeva  # default to global Veeva connection

    in_flight_calls = []
    failed_items = []
    with ThreadPoolExecutor(
        max_workers=MAX_WORKERS, thread_name_prefix=method
    ) as executor:
        # execute calls in parallel
        # upload in chunks of chunk_size records
        for chunk in [
            data[i : i + chunk_size] for i in range(0, len(data), chunk_size)
        ]:
            if method == "upsert":
                _state["logger"].info("Upserting chunk of %d records", len(chunk))
                future = executor.submit(
                    veeva.create_object_records,
                    object_type,
                    json=chunk,
                    id_param=id_param,
                )
                in_flight_calls.append((chunk, future))
            if method == "update":
                _state["logger"].info("Updating chunk of %d records", len(chunk))
                future = executor.submit(
                    veeva.update_object_records,
                    object_type,
                    json=chunk,
                    id_param=id_param,
                )
                in_flight_calls.append((chunk, future))

        # collect results
        for chunk, future in in_flight_calls:
            response = future.result()
            merge_response(chunk, response)
            collect_failed_records(
                response=response,
                veeva=veeva,
                object_type=object_type,
                failed_items=failed_items,
                delete_inactive_records=delete_inactive_records,
            )

    return failed_items


def collect_failed_records(
    response, veeva, object_type, failed_items, delete_inactive_records
):
    for item in response["data"]:
        if "responseStatus" in item:
            if item["responseStatus"] != "SUCCESS":
                _state["logger"].error(json.dumps(item, indent=2))
                failed_items.append(item)
        elif delete_inactive_records and item["data"]["status__v"] == "inactive__v":
            deleted = veeva.delete_object_records(
                object_type, [{"id": item["data"]["id"]}]
            )
            if deleted["responseStatus"] != "SUCCESS":
                failed_items.append(item)


def sync_records(
    data: Sequence[dict],
    object_type: str,
    id_param: str,
    method: str,
    veeva: Veeva = None,
    delete_inactive_records: bool = False,
):
    """Sync a set of records with the specified object type in Veeva Vault."""
    objects_inactivated = 0
    objects_updated = 0
    objects_failed = 0
    active_records = []
    failed_items = []

    _state["logger"].info("syncing %s records", object_type)

    # process table
    for item in data:
        # collect active/inactive records
        if item[c.status] == "active__v":
            del item[c.status]
            # save item for batch processing
            active_records.append(item)

        else:
            # process inactive records indiviually
            failed_items = upsert(
                object_type=object_type,
                data=[item],
                id_param=id_param,
                method=method,
                veeva=veeva,
                delete_inactive_records=delete_inactive_records,
            )

            if failed_items:
                objects_failed += 1
            else:
                objects_inactivated += 1

    # process active records in batches
    if active_records:
        failed_items = upsert(
            object_type=object_type,
            data=active_records,
            id_param=id_param,
            method=method,
            veeva=veeva,
        )
        objects_failed += len(failed_items)
        objects_updated += len(active_records) - len(failed_items)

    _state["logger"].info(
        f"Total objects updated for {object_type} is: {objects_updated}"
    )
    _state["logger"].info(
        f"Total objects inactivated for {object_type} is: {objects_inactivated}"
    )
    _state["logger"].info(
        f"Total objects failed for {object_type} is : {objects_failed}"
    )

    return failed_items


@dataclass
class Columns:
    """Well known Veeva Vault column names."""

    modify_date = "modified_date__v"
    external_id = "external_id__v"
    name = "name__v"
    status = "status__v"


c = Columns()

_state["logger"] = get_logger(os.path.basename(__file__))
