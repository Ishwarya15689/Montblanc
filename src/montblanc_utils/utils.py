import boto3
import json
from common_utils.utils import (
    build_upsert_query,
    PostgresDbDetails,
    execute_upsert_query_on_db_table,
    get_logger,
    execute_query_on_db_table,
    get_delta_records,
    fetch_specific_col_records_from_table,
    get_last_modified_date_timestamp,
    connect_to_db,
)
import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta
from typing import List
import warnings

warning_message = "pandas only support SQLAlchemy connectable"
LOGGER = get_logger(os.path.basename(__file__))


def fetch_aws_secret_dict(secret_name: str) -> dict:
    try:
        session = boto3.session.Session()
        region = session.region_name
        client = session.client(service_name="secretsmanager", region_name=region)
        response = client.get_secret_value(SecretId=secret_name)
        secret_dict = json.loads(response["SecretString"])
        return secret_dict
    except Exception as error:
        print(
            f"Error occurred while fetching secret '{secret_name}' from AWS Secrets Manager - {error}"
        )
        raise


def get_country_abbreviation(outbound_db_details: PostgresDbDetails, query: str):
    """
    Function to get country abbreviations from the refrence schema

    Parameters
    ----------
        outbound_db_details: outbound db details object
        query (str): Postgres SQL query to get country abbreviation dict

    Returns
    -------
        dict : country abbreviation dict

    """
    try:
        country_abbreviation = execute_query_on_db_table(outbound_db_details, query)
    except Exception as error:
        LOGGER.exception(
            f"Error occurred while fetching country abbreviation. Error: {error}"
        )
        raise
    country_abbreviation_dict = dict(country_abbreviation)
    return country_abbreviation_dict


def update_data_extract_time(
    staging_db_details, query_sql: str, last_extract_timestamp
):
    """
    Function to update last data extraction time

    Parameters
    ----------
        staging_db_details: staging db details
        query_sql: sql query string
        last_extract_timestamp: timestamp of last execution

    Returns
    -------
        None
    """
    query_sql = query_sql.format(last_extract_timestamp)
    try:
        execute_query_on_db_table(staging_db_details, query_sql, method="update")
    except Exception as error:
        LOGGER.exception(
            f"Error occurred while updating last data extract timestamp into staging DB."
            f"Error: {error}"
        )
        raise


def fetch_dataset_config_from_db(
    db_details: PostgresDbDetails, query: str, dataset_name: str
) -> dict:
    """
    Util function to fetch config data from db
    Args:
        db_details (PostgresDbDetails): postgres db details object
        query (str): Sql query string
        dataset_name (str): dataset_name

    Returns:
        dict: dict of config
    """
    sql = query.format(dataset_name=dataset_name)
    rows = execute_query_on_db_table(db_details=db_details, query=sql)
    if len(rows) == 0:
        LOGGER.error(f"Unable to find config for {dataset_name}")
        raise ValueError
    return rows[0][0]


def fetch_last_gemstone_extract_time(staging_db_details, query_sql):
    """
    fetch last Raven data extract timestamp from Staging DB

    Parameters
    ----------
        staging_db_details: db detail object
        query_sql: sql query string

    Returns
    -------
        Last execution time
    """
    try:
        rows = execute_query_on_db_table(staging_db_details, query_sql)
        if not rows:
            return None
        return rows[0][0]
    except Exception as error:
        LOGGER.exception(
            f"Error occurred while fetching last raven data extract timestamp - {error}"
        )
        raise


def get_last_extraction_time(
    staging_db_details,
    sql_data,
    **kwargs,
):
    """

    To get last extraction time

    Parameters
    ----------
        staging_db_details: staging db details
        sql_data (dict): Dict of sql queries from yaml
        kwargs:
            full_load_flag (str): Flag for full load and delta load
            dataset_name (str): dataset_name
    Returns:
        datetime: returns last extraction time
    """
    # unpacking kwargs
    full_load_flag = kwargs.get("full_load", None)
    dataset_name = kwargs.get("dataset_name")
    interval_in_days = kwargs.get("interval_in_days")
    last_extract_time = None
    if full_load_flag is None or full_load_flag.lower() != "yes":
        last_extract_time = fetch_last_gemstone_extract_time(
            staging_db_details,
            sql_data["fetch_data_extract_log_query"].format(dataset_name),
        )
        LOGGER.info(f"Started delta load for {dataset_name}")
    else:
        if interval_in_days is not None and interval_in_days != 0:
            last_extract_time = (
                datetime.utcnow() - timedelta(days=interval_in_days)
            ).replace(hour=0, minute=0, second=0, microsecond=0)
            LOGGER.info(
                f"Started full load for {dataset_name} from interval of {interval_in_days} days"
            )
    if last_extract_time is not None:
        last_extract_time = last_extract_time.strftime("%Y-%m-%d %H:%M:%S")
        LOGGER.info(f"Extracting {dataset_name} data from {last_extract_time}")
    else:
        LOGGER.info(
            f"Fetching entire load from montblanc outbounddb for {dataset_name}"
        )
    return last_extract_time


def fetch_data_from_db(db_details: PostgresDbDetails, query: str) -> pd.DataFrame:
    conn = connect_to_db(db_details)
    try:
        with warnings.catch_warnings(record=True) as w:
            data = pd.read_sql(query, conn)
            # suppressing below warning so that it does not appear frequently when extracting data
            for warning in w:
                if warning_message in str(warning.message):
                    warnings.simplefilter("ignore")
    except Exception as error:
        LOGGER.exception(
            f"Error occurred while extracting data using query-{query}"
            f"Error: {error}"
        )
        raise
    return data


def load_montblanc_data_to_staging_db(
    db_details: PostgresDbDetails,
    transformed_records,
    table_name,
    schema,
    sql,
    primary_key,
):
    montblanc_df = pd.DataFrame(transformed_records)
    if montblanc_df.empty:
        return len(montblanc_df)
    montblanc_df = montblanc_df.dropna(subset=[primary_key])
    montblanc_df = montblanc_df.drop_duplicates(subset=[primary_key])
    montblanc_df = montblanc_df.replace([np.NaN, np.nan], None)

    cols = montblanc_df.columns.tolist()
    # get staging db data in dataframe
    stagingdb_df = fetch_specific_col_records_from_table(
        db_details,
        table_name,
        cols,
        sql["fetch_data_from_staging_table"],
        schema=schema,
    )
    montblanc_df = montblanc_df.astype(str)
    stagingdb_df = stagingdb_df.astype(str)
    if table_name == "study__v" and not stagingdb_df.empty:
        stagingdb_df = stagingdb_df.replace("False", "false")
        stagingdb_df = stagingdb_df.replace("True", "true")
    df_delta = get_delta_records(montblanc_df, stagingdb_df, cols)
    df_delta["delta_flag"] = "Y"
    df_delta["last_modified_date"] = get_last_modified_date_timestamp()

    query = build_upsert_query(
        df_delta, schema=schema, table=table_name, pk=primary_key, method="upsert"
    )
    data = df_delta.to_records(index=False).tolist()

    try:
        execute_upsert_query_on_db_table(db_details, query, data)

    except Exception as error:
        LOGGER.exception(
            f"Error occurred while loading data to montblanc staging db table {table_name} - {error}"
        )
        raise
    return len(data)


def update_found_study_flag(
    montblanc_staging_details, outbound_db_details, sql, table, schema, primary_key
):
    outbound_data = fetch_data_from_db(
        outbound_db_details, query=sql["fetch_study_from_gemstone_outbound"]
    )

    if table == "study_country__v":
        montblanc_staging_data = fetch_data_from_db(
            montblanc_staging_details, sql["fetch_study_from_study_country_staging"]
        )
    if table == "study_person__clin":
        montblanc_staging_data = fetch_data_from_db(
            montblanc_staging_details, sql["fetch_study_from_study_person_staging"]
        )

    data_df = pd.merge(
        montblanc_staging_data,
        outbound_data,
        on=["study__v"],
        how="inner",
        validate=None,
    )
    data_df = data_df.drop(columns=["study__v"])
    if not data_df.empty:
        data_df.loc[:, "found_study__v"] = "Y"

    LOGGER.info(f"updating found_study__v flag for {len(data_df)} {table} records")

    if not data_df.empty:
        load_montblanc_data_to_staging_db(
            montblanc_staging_details, data_df, table, schema, sql, primary_key
        )


def update_country_confirmed_date(
    montblanc_staging_details, outbound_db_details, sql, table, schema, primary_key
):
    montblanc_staging_data = fetch_data_from_db(
        montblanc_staging_details, sql["fetch_external_id_from_mb_staging"]
    )
    outbound_data = fetch_data_from_db(
        outbound_db_details, query=sql["fetch_external_id_from_outbound"]
    )

    new_study_data = montblanc_staging_data[
        montblanc_staging_data.external_id__v.isin(outbound_data.external_id__v)
        == False
    ]

    current_date = datetime.today().strftime("%Y-%m-%d")
    new_study_data.loc[:, "country_selected_date__v"] = current_date

    montblanc_staging_data = montblanc_staging_data.drop(
        columns=["country_selected_date__v"]
    )
    existing_study_data = pd.merge(
        montblanc_staging_data,
        outbound_data,
        how="inner",
        on="external_id__v",
        validate=None,
    )

    LOGGER.info(
        f"updating country confirmed date for {len(new_study_data)} new study country records"
    )

    if not new_study_data.empty:
        load_montblanc_data_to_staging_db(
            montblanc_staging_details, new_study_data, table, schema, sql, primary_key
        )

    if not existing_study_data.empty:
        load_montblanc_data_to_staging_db(
            montblanc_staging_details,
            existing_study_data,
            table,
            schema,
            sql,
            primary_key,
        )


def update_data(gemstone_outbound_db, transformed_records, sql, **kwargs):
    filter_condition = kwargs.get("filter_condition", None)
    gemstone_data = fetch_data_from_db(
        gemstone_outbound_db, query=sql["get_study_person_data_from_gemstone_ob"]
    )

    filtered_list = [
        record for record in transformed_records if "external_id_2__c" in record
    ]
    transformed_data = pd.DataFrame(filtered_list)

    existing_data = transformed_data[
        transformed_data["external_id_2__c"].isin(gemstone_data.external_id_2__c)
    ]
    existing_data = existing_data.drop("start_date__clin", axis=1, errors="ignore")

    new_data = transformed_data[
        ~transformed_data["external_id_2__c"].isin(gemstone_data.external_id_2__c)
    ]

    merged_data = pd.merge(
        gemstone_data,
        new_data,
        on=filter_condition,
        how="inner",
        validate=None,
    )

    if not merged_data.empty:
        merged_data = merged_data.drop(
            [
                "study__clin",
                "external_id_2__c_y",
                "team_role__v",
                "study_country__clin",
                "create_urs__v",
                "person__clin",
                "start_date__clin",
                "study_country__abbreviation",
                "state__v",
                "contact_information__clin",
            ],
            axis=1,
            errors="ignore",
        )

        merged_data["end_date__clin"] = datetime.today().strftime("%Y-%m-%d")
        merged_data = merged_data.rename(
            columns={"external_id_2__c_x": "external_id_2__c"}
        )
    return (
        existing_data.to_dict(orient="records"),
        new_data.to_dict(orient="records"),
        merged_data.to_dict(orient="records"),
    )


def get_org_ext_id_from_gemstone(outbound_db_details, query_sql):
    try:
        rows = execute_query_on_db_table(outbound_db_details, query_sql)
        if not rows:
            return None
        return rows[0][0]
    except Exception as error:
        LOGGER.exception(
            f"Error occurred while fetching 'Investigator / Institute' Organization External ID 2 from Gemstone - {error}"
        )
        raise


def get_milestone_external_id_from_gs(
    outbound_db_details: PostgresDbDetails, query: str
):
    try:
        external_ids = execute_query_on_db_table(outbound_db_details, query)
        external_ids_df = pd.DataFrame(
            external_ids, columns=["external_id", "bayer_event_num"]
        )
    except Exception as error:
        LOGGER.exception(
            f"Error occurred while fetching country level milestone external id from Gemstone. Error: {error}"
        )
        raise
    return external_ids_df
