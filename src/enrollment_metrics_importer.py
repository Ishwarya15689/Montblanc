"""Extract Montblanc objects from Staging DB tables and load them into Veeva Vault."""

import os
import pandas as pd
from datetime import datetime
import numpy as np
from common_utils.utils import (
    get_logger,
    execute_query_on_db_table,
    sync_records,
    connect_to_db,
    build_upsert_query,
    execute_upsert_query_on_db_table,
)
import warnings
import os

warning_message = "pandas only support SQLAlchemy connectable"

LOGGER = get_logger(os.path.basename(__file__))
logger_name = os.path.basename(__file__)
LOGGER = get_logger(logger_name)
table = "metrics__ctms"
pk = "external_id__c"


class MontblancVeevaImporterMetrics:
    def __init__(
        self,
        staging_db_secret,
        outbound_db_secret,
        region_name,
        vault_secret,
        dynamodb,
        api,
        sql,
    ):
        self.staging_db_secret = staging_db_secret
        self.outbound_db_secret = outbound_db_secret
        self.region_name = region_name
        self.vault_secret = vault_secret
        self.dynamodb = dynamodb
        self.api = api
        self.sql = sql
        self.main()

    def extract_data_from_staging_db(self, staging_db_details, sql, table):
        """_summary_

        Args:
            staging_db_details (_type_): _description_
            sql (_type_): _description_
        """
        conn = connect_to_db(staging_db_details)
        query = sql["fetch_data_from_staging_db"].format(table_name=table)
        try:
            with warnings.catch_warnings(record=True) as w:
                rows = pd.read_sql(sql=query, con=conn)
                # suppressing below warning so that it does not appear frequently when extracting data
                for warning in w:
                    if warning_message in str(warning.message):
                        warnings.simplefilter("ignore")
        except Exception as error:
            LOGGER.exception(
                f"Error occurred while extracting data from staging db table {table} using query-{query}"
                f"Error: {error}"
            )
            raise
        if conn:
            conn.close()
        rows["status__v"] = "active__v"
        rows = rows.drop(
            columns=[
                "metric_type__ctms",
                "study__ctms",
                "study_country__ctms",
                "abbreviation__ctms",
                "study_type__c",
                "delta_flag",
                "vault_error_response",
                "created_date",
                "last_modified_date",
            ],
            errors="ignore",
        )
        rows = rows.dropna(axis=1, how="all")
        rows = rows.replace([np.NaN, np.nan], None)
        rows = rows.to_dict(orient="records")
        # removing actual__ctms field from payload for study level metrics
        for item in rows:
            if "actual__ctms" in item.keys() and item.get("actual__ctms") is None:
                del item["actual__ctms"]
        return rows

    def load_records_to_veeva(self, data, object_type_name, join_key, api):
        id_param = join_key
        failed_items = sync_records(
            data, object_type_name, id_param, method="update", veeva=api
        )
        return failed_items

    def prepare_failed_records_data(self, failed_items, primary_key):
        last_modified_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        rows = []
        for item in failed_items:
            error_response_object = ""
            error_response_veeva = ""
            error_response_object_flag = False
            pk_value = item["data"][primary_key]
            for error in item["errors"]:
                if error["type"] in (
                    "INVALID_DATA",
                    "PARAMETER_REQUIRED",
                    "OPERATION_NOT_ALLOWED",
                    "UNEXPECTED_ERROR",
                    "ATTRIBUTE_NOT_SUPPORTED",
                    "ITEM_NAME_EXISTS",
                    "RECORD_VALIDATION_FAILURE",
                ):
                    error_response_object += error["message"] + ";"
                    error_response_object_flag = True
                else:
                    error_response_veeva += error["message"] + ";"

            if error_response_object_flag:
                error_response = error_response_object[0:1500].replace("'", "''")
                delta_flag = "FAILED"
            else:
                error_response = error_response_veeva[0:1500].replace("'", "''")
                delta_flag = "Y"
            row = (pk_value, delta_flag, error_response, last_modified_date)
            rows.append(row)
        return rows

    def update_flag_and_response_in_staging_db(
        self, staging_db_details, object, failed_items, primary_key, sql, **kwargs
    ):
        """Update the delta_flag and vault_error_response in Staging DB"""
        schema = kwargs.get("schema", "montblanc")

        query = sql["update_flag_for_montblanc_table"].format(table=object)
        rows = []
        if failed_items:
            rows = self.prepare_failed_records_data(
                failed_items=failed_items, primary_key=primary_key
            )
        df = pd.DataFrame(
            rows,
            columns=[
                primary_key,
                "delta_flag",
                "vault_error_response",
                "last_modified_date",
            ],
        )
        bulk_update_query = build_upsert_query(
            df, schema, object, primary_key, "upsert"
        )
        try:
            execute_query_on_db_table(staging_db_details, query, method="update")
            execute_upsert_query_on_db_table(
                staging_db_details, bulk_update_query, rows
            )
        except Exception as error:
            LOGGER.exception(
                f"Error occurred while executing this query in Staging Databse - {query}. Error {error}"
            )
            raise

        LOGGER.info("delta_flag and vaut_error_response updated in Staging db")

    def main(self):
        LOGGER.info("Loading metrics__ctms objects to vault")
        data = self.extract_data_from_staging_db(
            self.staging_db_secret, self.sql, table
        )
        failed_items = self.load_records_to_veeva(
            data,
            object_type_name=table,
            join_key=pk,
            api=self.api,
        )
        self.update_flag_and_response_in_staging_db(
            self.staging_db_secret, table, failed_items, pk, self.sql
        )
