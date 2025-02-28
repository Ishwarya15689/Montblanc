import pandas as pd
import os
from typing import List, Dict
from datetime import datetime
import numpy as np
import warnings
from common_utils.utils import (
    connect_to_db,
    read_yaml_file,
    PostgresDbDetails,
    fetch_aws_secret,
    get_logger,
    get_veeva_api_connection,
    sync_records,
    build_upsert_query,
    execute_query_on_db_table,
    execute_upsert_query_on_db_table,
)
from common_utils.api import Veeva
from typing import List, Dict, Optional
from pydantic import BaseModel

warning_message = "pandas only support SQLAlchemy connectable"
logger_name = os.path.basename(__file__)
LOGGER = get_logger(logger_name)


class MontblancVeevaImporterStudyCountry:
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

    def fetch_data_from_db(
        self, db_details: PostgresDbDetails, query: str
    ) -> pd.DataFrame:
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

    def get_date_columns(self, staging_db_details, sql, table):
        try:
            data = execute_query_on_db_table(
                staging_db_details,
                sql["extract_date_columns_query"].format(table_name=table),
            )
        except Exception as error:
            LOGGER.exception(
                f"Error occurred while extracting date columns. Error: {error}"
            )
            raise
        date_columns = [date for col in data for date in col]

        return date_columns

    def extract_from_staging_db(self, staging_db_details, sql, table):
        """_summary_

        Args:
            staging_db_details (_type_): _description_
            sql (_type_): _description_
        """
        conn = connect_to_db(staging_db_details)
        date_columns = self.get_date_columns(staging_db_details, sql, table)

        if table == "study_country__v":
            try:
                with warnings.catch_warnings(record=True) as w:
                    rows = pd.read_sql(
                        sql=sql["extract_records_query"].format(table_name=table),
                        con=conn,
                        parse_dates=date_columns,
                        dtype={"planned_number_of_sites__c": int},
                    )
                    # suppressing below warning so that it does not appear frequently when extracting data
                    for warning in w:
                        if warning_message in str(warning.message):
                            warnings.simplefilter("ignore")
            except Exception as error:
                LOGGER.exception(
                    f"Error occurred while extracting data from staging db table"
                    f"Error: {error}"
                )
                raise
        if table == "study_person__clin":
            try:
                with warnings.catch_warnings(record=True) as w:
                    rows = pd.read_sql(
                        sql=sql["extract_records_persons_query"].format(
                            table_name=table
                        ),
                        con=conn,
                        parse_dates=date_columns,
                    )
                    # suppressing below warning so that it does not appear frequently when extracting data
                    for warning in w:
                        if warning_message in str(warning.message):
                            warnings.simplefilter("ignore")
            except Exception as error:
                LOGGER.exception(
                    f"Error occurred while extracting data from staging db table"
                    f"Error: {error}"
                )
                raise

        rows["status__v"] = "active__v"

        for date_col in date_columns:
            rows[date_col] = rows[date_col].apply(
                lambda x: x.strftime("%Y-%m-%d") if not pd.isnull(x) else None
            )

        rows = rows.drop(
            columns=[
                "created_date",
                "last_modified_date",
                "delta_flag",
                "state__v",
                "object_type__v",
                "found_study__v",
                "vault_error_response",
                "cancelled_api_timestamp",
                "cancelled_api_flag",
                "closed_api_flag",
                "closed_api_timestamp",
                "reason_for_stoppingcancelling__c",
                "study_country__abbreviation",
            ],
            errors="ignore",
        )
        rows = rows.dropna(axis=1, how="all")
        rows = rows.replace([np.NaN, np.nan], None)
        return rows.to_dict(orient="records")

    def update_field_names(self, records: List[Dict]):
        fields_list_name__v = (
            "country__v",
            "team_role__v",
            "contact_information__clin",
            "study_country__clin",
        )
        fields_list_external_id__v = ("study__v", "study__clin", "person__clin")
        for record in records:
            for field in fields_list_name__v:
                if field in record:
                    record[field + ".name__v"] = record.pop(field)
            for field in fields_list_external_id__v:
                if field in record:
                    record[field + ".external_id_2__c"] = record.pop(field)
        return records

    def load_records_to_veeva(self, data, object_type_name, join_key, api):
        id_param = join_key
        failed_items = sync_records(
            data, object_type_name, id_param, method="upsert", veeva=api
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
                    "SDK_ERROR",
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

        if object == "study_person__clin":
            query = sql["update_flag_for_study_country_person_montblanc_table"].format(
                table=object
            )
        else:
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
                f"Error occurred while executing this query in Staging Database - {query}. Error: {error}"
            )
            raise

        LOGGER.info("delta_flag and vault_error_response updated in Staging db")

    def update_responsible_person(
        self, montblanc_db_details, outbound_db_details, sql, api
    ):
        montblanc_data = self.fetch_data_from_db(
            montblanc_db_details, sql["fetch_responsible_person_country"]
        )
        montblanc_data["external_id__v"] = (
            montblanc_data["study__clin"]
            + "-"
            + montblanc_data["study_country__abbreviation"]
        )
        outbound_data = self.fetch_data_from_db(
            outbound_db_details, query=sql["fetch_person_from_outbound_db_country"]
        )
        data_merge = pd.merge(
            montblanc_data,
            outbound_data,
            how="inner",
            left_on="person__clin",
            right_on="external_id_2__c",
            validate=None,
        )
        data_merge = data_merge.drop(
            columns=[
                "study__clin",
                "study_country__abbreviation",
                "person__clin",
                "external_id_2__c",
            ],
        )
        data_merge["status__v"] = "active__v"

        data_records = data_merge.to_dict(orient="records")
        LOGGER.info(
            f"Updating responsible person for {len(data_records)} study country"
        )
        self.load_records_to_veeva(
            data=data_records,
            object_type_name="study_country__v",
            join_key="external_id__v",
            api=api,
        )

    def update_api_flag(self, postgress_db_details, studycountry_list, column, flag):
        LOGGER.info(f"Updating api flag for column {column}")
        if len(studycountry_list) == 0:
            LOGGER.info(f"No study country to update api flag for {column}")
            return
        dataframe = pd.DataFrame(studycountry_list, columns=["external_id__v"])
        dataframe[column + "_api_flag"] = flag
        dataframe[column + "_api_timestamp"] = datetime.utcnow().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        query = build_upsert_query(
            dataframe,
            schema="montblanc",
            table="study_country__v",
            method="upsert",
            pk="external_id__v",
        )
        rows = dataframe.to_records(index=False).tolist()
        execute_upsert_query_on_db_table(
            postgres_db_details=postgress_db_details, query=query, rows=rows
        )

    def update_any_to_cancelled(
        self, staging_db_details, gemstone_outbound_db_details, sql, api
    ):
        LOGGER.info("Updating life cycle change for any to cancelled records")
        last_extraction_time = self.get_last_extraction_time(staging_db_details, sql)

        staging_db_data = self.fetch_data_from_db(
            staging_db_details, sql["fetch_cancelled_study_country_from_staging_db"]
        )
        staging_db_data = staging_db_data[
            (staging_db_data["cancelled_api_flag"].isna())
            | (
                (staging_db_data["cancelled_api_flag"] != "Requested")
                & (staging_db_data["last_modified_date"] > last_extraction_time)
            )
        ]

        study_staging_data = self.fetch_data_from_db(
            staging_db_details, sql["fetch_studies_from_staging_db_for_study_country"]
        )

        merging_study_country_data = pd.merge(
            staging_db_data,
            study_staging_data,
            on="study__v",
            how="left",
            validate=None,
        )

        ignored_countries = merging_study_country_data[
            (merging_study_country_data["study_state"] == "cancelled_state__v")            
        ]

        countries_to_be_cancelled = merging_study_country_data[
            (merging_study_country_data["study_state"] != "cancelled_state__v")            
        ]

        gemstone_outbound_db_data = self.fetch_data_from_db(
            gemstone_outbound_db_details,
            sql["fetch_all_study_countries_from_gemstone_outbound_db"],
        )

        if staging_db_data.empty:
            LOGGER.info("No study to update to cancelled")
            return
        new_records_df = pd.merge(
            countries_to_be_cancelled,
            gemstone_outbound_db_data,
            on="external_id__v",
            how="inner",
            validate=None,
        )

        ignored_records_df = pd.merge(
            ignored_countries,
            gemstone_outbound_db_data,
            on="external_id__v",
            how="inner",
            validate=None,
        )
        
        self.apply_any_to_cancelled(
            staging_db_details, new_records_df.to_dict(orient="records"), api
        )

    def apply_any_to_cancelled(self, db_details, data_records, api):
        success_list = []
        url_mapping = {
            "active_no_subjects_recruited_state__c": "active_no_subjects_recruited_state__c.cancel_study_country_useraction5__c",
            "active_on_hold_state__c": "active_on_hold_state__c.request_study_country_cancellation_usera__c",
            "candidate_state__v": "candidate_state__v.cancel_study_country_useraction1__c",
            "assessing_state__c": "assessing_state__c.cancel_study_country_useraction2__c",
            "initiating_state__v": "initiating_state__v.cancel_study_country_useraction3__c",
            "planning_on_hold_state__c": "planning_on_hold_state__c.cancel_study_country_useraction4__c",
        }

        current_date = datetime.today().strftime("%Y-%m-%d")

        for data_record in data_records:
            data = {
                "study_country__v.cancellation_date__c": current_date,
                "study_country__v.reason_for_stoppingcancelling__c": data_record[
                    "reason_for_stoppingcancelling__c"
                ],
            }

            try:
                response = api.execute_user_action(
                    object_type="study_country__v",
                    record_id=data_record["id"],
                    action_name=f"Objectlifecyclestateuseraction.study_country__v.{url_mapping[data_record['state__v']]}",
                    data=data,
                )
                if response["responseStatus"] == "SUCCESS":
                    success_list.append(data_record["external_id__v"])
            except Exception as e:
                LOGGER.error(
                    f"Any to Cancelled user action api failed for {data_record['external_id__v']}. Payload-{data}. Error-{e}"
                )
        self.update_api_flag(db_details, success_list, "cancelled", "Requested")

    def update_closing_to_closed(
        self,
        staging_db_details,
        gemstone_outbound_db_details,
        sql,
        api,
    ):
        LOGGER.info("Updating life cycle change for closing to closed")
        staging_db_data = self.fetch_data_from_db(
            staging_db_details, sql["fetch_not_closed_study_country_from_staging_db"]
        )

        gemstone_outbound_db_data = self.fetch_data_from_db(
            gemstone_outbound_db_details,
            sql["fetch_closed_study_country_from_gemstone_outbound_db"],
        )

        new_records_df = gemstone_outbound_db_data.merge(
            staging_db_data, on="external_id__v", how="inner"
        )

        self.apply_closing_to_closed(
            staging_db_details, new_records_df.to_dict(orient="records"), api
        )

    def apply_closing_to_closed(self, db_details, data_records, api):
        success_list = []

        for data_record in data_records:
            try:
                response = api.execute_user_action(
                    object_type="study_country__v",
                    record_id=data_record["id"],
                    action_name="Objectlifecyclestateuseraction.study_country__v.closing_state__v.cov_complete_useraction__c",
                )
                if response["responseStatus"] == "SUCCESS":
                    success_list.append(data_record["external_id__v"])
            except Exception as e:
                LOGGER.error(
                    f"Closing to closed user action api failed for {data_record['external_id__v']}. Error-{e}"
                )
        self.update_api_flag(db_details, success_list, "closed", "Closed")

    def update_last_extraction_log(self, db_details, sql):
        LOGGER.info("Updating Last extraction time")
        last_extraction_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        query = sql["update_data_extract_log"].format(
            last_extract_timestamp=last_extraction_date, dataset_name="study_country"
        )
        execute_query_on_db_table(db_details, query, method="update")

    class RejectedStudyCountry(BaseModel):
        object_record_id__sys: str
        verdict__sys: str
        verdict_reason__sys: Optional[str]
        modified_date__sys: str

    def get_study_country_reject_objects(
        self, response, staging_db_details, outbound_db_details, sql, column
    ):
        rejected_study_country_list: List[self.RejectedStudyCountry] = []
        for rejected_study_countries in response["data"]:
            modified_date__sys = rejected_study_countries[
                "inactive_workflow__sysr.modified_date__sys"
            ]
            for rejected_study_country in rejected_study_countries[
                "inactive_workflow_task_items__sysr"
            ]["data"]:
                try:
            
                    if rejected_study_country.get("verdict__sys") is None:
                     raise ValueError(f"Missing 'verdict__sys' for record {rejected_study_country.get('object_record_id__sys')}")

           
                    rejected_study_country_list.append(rejected_study_country)
        
                except Exception as error:
            
                     LOGGER.exception(f"Error occurred while processing record {rejected_study_country.get('object_record_id__sys')}. Error: {error}")
                     raise
        if len(rejected_study_country_list) == 0:
            LOGGER.info(f"No reject records found for {column}")
            return pd.DataFrame()
        rejected_study_country_df = pd.DataFrame(
            [study.model_dump() for study in rejected_study_country_list]
        ).sort_values(by="modified_date__sys", ascending=False)
        rejected_study_country_df = rejected_study_country_df.drop_duplicates(
            subset=["object_record_id__sys"], keep="first"
        )
        rejected_study_country_df = rejected_study_country_df[
            rejected_study_country_df["verdict__sys"].str.contains("Reject")
        ]

        outbound_study_df = self.fetch_data_from_db(
            outbound_db_details, sql["fetch_study_country_from_outbound_db"]
        )

        staging_study_df = self.fetch_data_from_db(
            staging_db_details,
            sql["fetch_all_data_from_staging_db"].format(table_name="study_country__v"),
        )

        staging_study_df = staging_study_df[
            staging_study_df[f"{column}_api_flag"] == "Requested"
        ]

        merged_df = pd.merge(
            left=pd.merge(
                rejected_study_country_df,
                outbound_study_df,
                left_on="object_record_id__sys",
                right_on="id",
                how="inner",
                suffixes=("", "_y"),
                validate=None,
            ),
            right=staging_study_df,
            on="external_id__v",
            suffixes=("_z", ""),
            how="inner",
            validate=None,
        )
        merged_df.loc[:, f"{column}_api_timestamp"] = merged_df.loc[
            :, "modified_date__sys"
        ]
        merged_df = merged_df[staging_study_df.columns.to_list()]
        merged_df[f"{column}_api_flag"] = "Rejected"

        return merged_df

    def get_last_extraction_time(self, db_details, sql):
        conn = connect_to_db(db_details)
        datetime_format = "%Y-%m-%dT%H:%M:%S.%f"
        with warnings.catch_warnings(record=True) as w:
            dataframe = pd.read_sql(
                sql=sql["fetch_last_extraction_timestamp"].format(
                    dataset_name="study_country"
                ),
                con=conn,
            )
            # suppressing below warning so that it does not appear frequently when extracting data
            for warning in w:
                if warning_message in str(warning.message):
                    warnings.simplefilter("ignore")
        if not dataframe["last_extract_timestamp"][0]:
            return datetime.utcnow().strftime(datetime_format)
        return dataframe["last_extract_timestamp"][0].strftime(datetime_format)

    def update_rejected_records(
        self, api: Veeva, staging_db_details, outbound_db_details, sql
    ):
        last_extract_time = (
            self.get_last_extraction_time(staging_db_details, sql)[:-3] + "Z"
        )
        LOGGER.info(f"Last extract timestamp is {last_extract_time}")

        reject_workflows = {
            "cancelled": "cancel_study_country__c",
        }
        for workflow, workflow_value in reject_workflows.items():
            LOGGER.info(f"Fetching rejcted records for {workflow}")
            vql = sql["fetch_reject_records_country"].format(
                last_extract_time=last_extract_time, workflow=workflow_value
            )
            response = api.query(vql=vql)

            update_df = self.get_study_country_reject_objects(
                response, staging_db_details, outbound_db_details, sql, workflow
            )

            if update_df.empty:
                LOGGER.info("No rejected records found")
            else:
                update_df = update_df[
                    [
                        "external_id__v",
                        f"{workflow}_api_flag",
                        f"{workflow}_api_timestamp",
                    ]
                ]
                LOGGER.info(
                    f"Updating {workflow} api flag for {len(update_df)} records"
                )

                upsert_query = build_upsert_query(
                    update_df,
                    "montblanc",
                    "study_country__v",
                    pk="external_id__v",
                    method="upsert",
                )

                execute_upsert_query_on_db_table(
                    staging_db_details,
                    upsert_query,
                    update_df.to_records(index=False).tolist(),
                )
                LOGGER.info(
                    f"Successfully updated {workflow} api flag for {len(update_df)} records"
                )

    def main(self):
        LOGGER.info("Loading study_country__v objects to vault")
        self.update_rejected_records(
            self.api, self.staging_db_secret, self.outbound_db_secret, self.sql
        )
        interfaces = self.sql["interfaces"]

        for table in interfaces.keys():
            LOGGER.info(f"Fetching data from {table}")

            data = self.extract_from_staging_db(self.staging_db_secret, self.sql, table)
            updated_data = self.update_field_names(data)

            failed_items = self.load_records_to_veeva(
                updated_data,
                object_type_name=table,
                join_key=interfaces[table]["pk"],
                api=self.api,
            )
            self.update_flag_and_response_in_staging_db(
                self.staging_db_secret,
                table,
                failed_items,
                interfaces[table]["pk"],
                self.sql,
            )

        try:
            self.update_responsible_person(
                self.staging_db_secret, self.outbound_db_secret, self.sql, self.api
            )
        except Exception as e:
            LOGGER.error(
                f"Error occurred while updating study country responsible person. Error: {e}"
            )
        try:
            self.update_any_to_cancelled(
                self.staging_db_secret, self.outbound_db_secret, self.sql, self.api
            )
        except Exception as e:
            LOGGER.error(
                f"Error occurred while changing study country Any to Cancelled lifecycle state. Error: {e}"
            )
        try:
            self.update_closing_to_closed(
                self.staging_db_secret,
                self.outbound_db_secret,
                self.sql,
                self.api,
            )
        except Exception as e:
            LOGGER.error(
                f"Error occurred while changing study country Closing to Closed lifecycle state. Error: {e}"
            )
        self.update_last_extraction_log(self.staging_db_secret, self.sql)
