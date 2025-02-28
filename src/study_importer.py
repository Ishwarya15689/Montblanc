import os
import pandas as pd
from datetime import datetime
from common_utils.utils import (
    read_yaml_file,
    get_logger,
    PostgresDbDetails,
    fetch_aws_secret,
    execute_query_on_db_table,
    sync_records,
    connect_to_db,
    get_veeva_api_connection,
    build_upsert_query,
    execute_upsert_query_on_db_table,
)
from common_utils.api import Veeva
import numpy as np
from typing import List, Dict, Optional
from pydantic import BaseModel
import warnings

warning_message = "pandas only support SQLAlchemy connectable"
logger_name = os.path.basename(__file__)
LOGGER = get_logger(logger_name)


class RejectedStudy(BaseModel):
    object_record_id__sys: str
    verdict__sys: str
    verdict_reason__sys: Optional[str]
    modified_date__sys: str


class StudyVeevaImporter:
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

    def extract_data_from_staging_db(self, staging_db_details, sql, table):
        """_summary_

        Args:
            staging_db_details (_type_): _description_
            sql (_type_): _description_
        """
        conn = connect_to_db(staging_db_details)
        date_columns = self.get_date_columns(staging_db_details, sql, table)
        try:
            with warnings.catch_warnings(record=True) as w:
                rows = pd.read_sql(
                    sql=(
                        sql["fetch_data_from_staging_db"].format(table_name=table)
                        if table in ("study__v", "study_product__v")
                        else sql["fetch_data_from_study_person"]
                    ),
                    con=conn,
                    parse_dates=date_columns,
                    dtype=(
                        {"planned_sites__c": int, "planned_countries__c": float}
                        if table == "study__v"
                        else None
                    ),
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
        if table == "study__v":
            rows["planned_countries__c"] = rows["planned_countries__c"].fillna(1)
            rows["planned_countries__c"] = rows["planned_countries__c"].astype(
                dtype=int
            )

        for date_col in date_columns:
            rows[date_col] = rows[date_col].apply(
                lambda x: x.strftime("%Y-%m-%d") if not pd.isnull(x) else None
            )

        rows = rows.drop(
            columns=[
                "lifecycle__v",
                "responsible_person__c",
                "state__v",
                "study_country__clin",
                "name__v",
                "milestone_master_set__v",
                "planning_api_flag",
                "planning_api_timestamp",
                "cancelled_api_flag",
                "cancelled_api_timestamp",
                "cancellation_date__c",
                "reason_for_stoppingcancelling_study__c",
                "delta_flag",
                "vault_error_response",
                "closed_api_flag",
                "closed_api_timestamp",
                "created_date",
                "last_modified_date",
                "pk_id_study_product",
                "study_ready_to_be_closed_in_gemstone__c",
            ],
            errors="ignore",
        )

        rows = rows.dropna(axis=1, how="all")
        rows = rows.replace([np.NaN, np.nan], None)
        return rows.to_dict(orient="records")

    def update_field_names(self, records: List[Dict]):
        fields_list_name__v = (
            "assign_study_number__c",
            "study_country__clin",
            "team_role__v",
            "contact_information__clin",
            "product_role__v",
        )
        fields_list_external_id_2__c = (
            "study__clin",
            "university_or_company_if_not_bayer__c",
            "oversight__c",
            "managing_medical_unit__c",
            "coordinating_investigator__c",
            "person__clin",
            "data_management__c",
            "safety_reporting__c",
            "study__v",
        )
        fields_list_project_id__c = ("project__c",)
        fields_list_bay_number__c = ("product__v",)
        for record in records:
            for field in fields_list_name__v:
                if field in record:
                    record[field + ".name__v"] = record.pop(field)
            for field in fields_list_external_id_2__c:
                if field in record:
                    record[field + ".external_id_2__c"] = record.pop(field)
            for field in fields_list_project_id__c:
                if field in record:
                    record[field + ".project_id__c"] = record.pop(field)
            for field in fields_list_bay_number__c:
                if field in record:
                    record[field + ".bay_number__c"] = record.pop(field)
        return records

    def load_records_to_veeva(self, data, object_type_name, join_key, api, method):
        id_param = join_key
        failed_items = sync_records(
            data, object_type_name, id_param, method=method, veeva=api
        )
        return failed_items

    def update_responsible_person(
        self, montblanc_db_details, outbound_db_details, sql, api
    ):
        montblanc_data = self.fetch_data_from_db(
            montblanc_db_details, sql["fetch_responsible_person"]
        )
        outbound_data = self.fetch_data_from_db(
            outbound_db_details, query=sql["fetch_person_from_outbound_db"]
        )
        data_merge = pd.merge(
            montblanc_data,
            outbound_data,
            how="inner",
            left_on="external_id_2__c_x",
            right_on="external_id_2__c_y",
        )
        data_merge = data_merge.drop(
            columns=["external_id_2__c_x", "external_id_2__c_y"]
        )
        data_merge["status__v"] = "active__v"
        data_records = data_merge.to_dict(orient="records")
        self.load_records_to_veeva(
            data=data_records,
            object_type_name="study__v",
            join_key="external_id_2__c",
            api=api,
            method="update",
        )
        LOGGER.info(f"Updating responsible person for {len(data_records)} studies")

    def update_candidate_lifecycle_state(
        self, staging_db_details, outbound_db_details, sql, api
    ):
        last_extraction_time = self.get_last_extraction_time(staging_db_details, sql)
        staging_db_data = self.fetch_data_from_db(
            staging_db_details,
            sql["fetch_successfully_loaded_data_from_staging_db"].format(
                table_name="study__v"
            ),
        )
        outbound_db_data = self.fetch_data_from_db(
            outbound_db_details, sql["fetch_study_from_outbound_db"]
        )
        staging_db_data = staging_db_data[
            (staging_db_data["planning_api_flag"].isna())
            | (
                (staging_db_data["planning_api_flag"] == "Rejected")
                & (staging_db_data["last_modified_date"] > last_extraction_time)
            )
        ]

        if staging_db_data.empty:
            LOGGER.info("No records to update lifecycle")
            return
        new_records_df = pd.merge(
            staging_db_data,
            outbound_db_data,
            on="external_id_2__c",
            how="inner",
            suffixes=("", "_y"),
        )
        if new_records_df.empty:
            LOGGER.info(
                "No records to update lifecycle since no records are matching with gemstone outbound db"
            )
            return
        new_records_df = new_records_df[
            (new_records_df["state__v_y"] == "candidate_state__v")
        ]
        reponsible_person_df = self.fetch_data_from_db(
            outbound_db_details, sql["fetch_person_from_outbound_db"]
        )
        organization_df = self.fetch_data_from_db(
            outbound_db_details, sql["fetch_organization_from_outbound_db"]
        )
        new_records_df = pd.merge(
            new_records_df,
            reponsible_person_df,
            left_on="responsible_person__c",
            right_on="external_id_2__c_y",
            how="inner",
            suffixes=("", "_z"),
        )

        new_records_df = new_records_df.loc[
            :,
            [
                "id",
                "external_id_2__c",
                "responsible_person__c_z",
                "study_type__v",
                "study_subtype__c",
                "planned_countries__c",
                "planned_sites__c",
                "sponsor__c",
                "ct_gov_study_type__c",
                "managing_medical_unit__c",
                "oversight__c",
                "recruitment_planning_level__v",
                "protocol_title__clin",
                "post_authorisation_study__c",
                "eu_application_process__c",
                "dct_sites_in_study__c",
                "internal_external__c",
                "active_for_rave_integration__c",
                "route_of_administration__c",
                "masking__v",
                "randomization__c",
                "indications__c",
            ],
        ]

        new_records_df = new_records_df.rename(
            columns={
                "id": "study_id",
                "responsible_person__c_z": "responsible_person__c",
            },
        )
        new_records_df = new_records_df.astype(
            {"planned_countries__c": int, "planned_sites__c": int}
        )
        # mapping organization id for medical unit and oversight below

        new_records_df["managing_medical_unit__c"] = new_records_df[
            "managing_medical_unit__c"
        ].map(organization_df.set_index("external_id_2__c")["id"])
        new_records_df["oversight__c"] = new_records_df["oversight__c"].map(
            organization_df.set_index("external_id_2__c")["id"]
        )
        new_records_df["active_for_rave_integration__c"] = new_records_df[
            "active_for_rave_integration__c"
        ].apply(lambda x: "true" if x else "false")
        LOGGER.info(
            f"Changing lifecycle state from candidate to early planning and planning for {len(new_records_df)} records"
        )
        if new_records_df.empty:
            LOGGER.info(
                "No records found to change from candidate to early planning and planning lifecycle state"
            )
            return
        self.apply_user_action_util(
            staging_db_details, new_records_df.to_dict(orient="records"), api
        )

    def apply_closing_to_closed(self, db_details, data_records, api):
        success_list = []
        for data_record in data_records:
            try:
                response = api.execute_user_action(
                    object_type="study__v",
                    record_id=data_record["id"],
                    action_name="Objectlifecyclestateuseraction.study__v.closing_state__v.change_state_to_closed_useraction6__c",
                )
                if response["responseStatus"] == "SUCCESS":
                    success_list.append(data_record["study_id"])
            except Exception as e:
                LOGGER.error(
                    f"Closing to closed user action api failed for {data_record['study_id']}. Error-{e}"
                )
        self.update_api_flag(db_details, success_list, "closed", "Closed")

    def update_closing_to_closed(
        self,
        staging_db_details,
        gemstone_outbound_db_details,
        sql,
        api,
    ):
        staging_db_data = self.fetch_data_from_db(
            staging_db_details, sql["fetch_not_closed_studies_from_staging_db"]
        )

        gemstone_outbound_db_data = self.fetch_data_from_db(
            gemstone_outbound_db_details,
            sql["fetch_closing_studies_from_gemstone_outbound_db"],
        )

        new_records_df = gemstone_outbound_db_data.merge(
            staging_db_data, on="study_id", how="inner"
        )

        if new_records_df.empty:
            LOGGER.info("No study to update from closing to closed lifecycle state")
            return

        self.apply_closing_to_closed(
            staging_db_details, new_records_df.to_dict(orient="records"), api
        )

    def apply_user_action_util(self, db_details, data_records, api):
        success_list = []
        for data_record in data_records:
            data_dict = {}
            study_id = None
            external_id = None
            for key, value in data_record.items():
                if key == "external_id_2__c":
                    external_id = value
                    continue
                if key == "study_id":
                    study_id = value
                    continue
                data_dict[f"study__v.{key}"] = value
            try:
                response = api.execute_user_action(
                    object_type="study__v",
                    record_id=study_id,
                    action_name="Objectlifecyclestateuseraction.study__v.candidate_state__v.request_move_to_planning_useraction__c",
                    params=data_dict,
                )
                if response["responseStatus"] == "SUCCESS":
                    success_list.append(external_id)

            except Exception as e:
                LOGGER.error(
                    f"Candidate to planning user action api is failed for {external_id}. Payload-{data_dict}. Error-{e}"
                )
        self.update_api_flag(db_details, success_list, "planning", "Requested")

    def update_any_to_cancelled(
        self, staging_db_details, gemstone_outbound_db_details, sql, api
    ):
        LOGGER.info("Updating lifecycle state for any to cancelled records")
        last_extraction_time = self.get_last_extraction_time(staging_db_details, sql)
        staging_db_data = self.fetch_data_from_db(
            staging_db_details, sql["fetch_cancelled_studies_from_staging_db"]
        )
        staging_db_data = staging_db_data[
            (staging_db_data["cancelled_api_flag"].isna())
            | (
                (staging_db_data["cancelled_api_flag"] != "Requested")
                & (staging_db_data["last_modified_date"] > last_extraction_time)
            )
        ]
        gemstone_outbound_db_data = self.fetch_data_from_db(
            gemstone_outbound_db_details,
            sql["fetch_all_studies_from_gemstone_outbound_db"],
        )
        if staging_db_data.empty:
            LOGGER.info("No study to update to cancelled state")
            return
        new_records_df = pd.merge(
            staging_db_data, gemstone_outbound_db_data, on="study_id", how="inner"
        )
        self.apply_any_to_cancelled(
            staging_db_details, new_records_df.to_dict(orient="records"), api
        )

    def apply_any_to_cancelled(self, db_details, data_records, api):
        success_list = []
        url_mapping = {
            "active_no_subjects_recruited_state__c": "active_no_subjects_recruited_state__c.cancel_study_useraction__c",
            "active_on_hold_state__c": "active_on_hold_state__c.request_study_cancellation_useraction__c",
            "candidate_state__v": "candidate_state__v.request_study_cancellation_useraction1__c",
            "early_planning_state__c": "early_planning_state__c.cancel_study_useraction7__c",
            "in_migration_state__v": "in_migration_state__v.change_state_to_cancelled_useraction5__c",
            "planning_on_hold_state__c": "planning_on_hold_state__c.cancel_study_useraction6__c",
            "planning_state__v": "planning_state__v.cancel_study_useraction5__c",
        }
        for data_record in data_records:
            data = {
                "study__v.cancellation_date__c": (
                    data_record["cancellation_date__c"].strftime("%Y-%m-%d")
                    if data_record["cancellation_date__c"] is not None
                    else datetime.now().strftime("%Y-%m-%d")
                ),
                "study__v.reason_for_stoppingcancelling_study__c": data_record[
                    "reason_for_stoppingcancelling_study__c"
                ],
            }
            try:
                response = api.execute_user_action(
                    object_type="study__v",
                    record_id=data_record["id"],
                    action_name=f"Objectlifecyclestateuseraction.study__v.{url_mapping[data_record['state__v']]}",
                    data=data,
                )
                if response["responseStatus"] == "SUCCESS":
                    success_list.append(data_record["study_id"])
            except Exception as e:
                LOGGER.error(
                    f"Any to Cancelled user action api failed for {data_record['study_id']}. Payload-{data}. Error-{e}"
                )
        self.update_api_flag(db_details, success_list, "cancelled", "Requested")

    def get_study_reject_objects(
        self, response, staging_db_details, outbound_db_details, sql, column
    ):
        rejected_studies_list: List[RejectedStudy] = []
        for rejected_studies in response["data"]:
            modified_date__sys = rejected_studies[
                "inactive_workflow__sysr.modified_date__sys"
            ]
            for rejected_study in rejected_studies[
                "inactive_workflow_task_items__sysr"
            ]["data"]:
                try:
            
                    if rejected_study.get("verdict__sys") is None:
                     raise ValueError(f"Missing 'verdict__sys' for record {rejected_study.get('object_record_id__sys')}")

           
                    rejected_studies_list.append(rejected_study)
        
                except Exception as error:
            
                     LOGGER.exception(f"Error occurred while processing record {rejected_study.get('object_record_id__sys')}. Error: {error}")
                     raise
        if len(rejected_studies_list) == 0:
            LOGGER.info(f"No reject records found for {column}")
            return pd.DataFrame()
        rejected_study_df = pd.DataFrame(
            [study.model_dump() for study in rejected_studies_list]
        ).sort_values(by="modified_date__sys", ascending=False)
        rejected_study_df = rejected_study_df.drop_duplicates(
            subset=["object_record_id__sys"], keep="first"
        )
        rejected_study_df = rejected_study_df[
            rejected_study_df["verdict__sys"].str.contains("Reject")
        ]
        outbound_study_df = self.fetch_data_from_db(
            outbound_db_details, sql["fetch_study_from_outbound_db"]
        )
        staging_study_df = self.fetch_data_from_db(
            staging_db_details,
            sql["fetch_all_data_from_staging_db"].format(table_name="study__v"),
        )
        staging_study_df = staging_study_df[
            staging_study_df[f"{column}_api_flag"] == "Requested"
        ]
        merged_df = pd.merge(
            left=pd.merge(
                rejected_study_df,
                outbound_study_df,
                left_on="object_record_id__sys",
                right_on="id",
                how="inner",
                suffixes=("", "_y"),
            ),
            right=staging_study_df,
            on="external_id_2__c",
            suffixes=("_z", ""),
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
                sql=sql["fetch_last_extraction_timestamp"].format(dataset_name="study"),
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
            "planning": "request_chmb_to_early_planning_and_plan__c",
            "cancelled": "cancel_study__c",
        }
        for workflow, workflow_value in reject_workflows.items():
            LOGGER.info(f"Fetching rejcted records for {workflow}")
            vql = sql["fetch_reject_records"].format(
                last_extract_time=last_extract_time, workflow=workflow_value
            )
            response = api.query(vql=vql)
            update_df = self.get_study_reject_objects(
                response, staging_db_details, outbound_db_details, sql, workflow
            )
            if update_df.empty:
                LOGGER.info("No rejected records found")
            else:
                update_df = update_df[
                    [
                        "external_id_2__c",
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
                    "study__v",
                    pk="external_id_2__c",
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

    def update_api_flag(self, postgress_db_details, study_list, column, flag):
        LOGGER.info(f"Updating api flag for column {column}")
        if len(study_list) == 0:
            LOGGER.info(f"No study to update api flag for {column}")
            return
        dataframe = pd.DataFrame(study_list, columns=["external_id_2__c"])
        dataframe[column + "_api_flag"] = flag
        dataframe[column + "_api_timestamp"] = datetime.utcnow().strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        query = build_upsert_query(
            dataframe,
            schema="montblanc",
            table="study__v",
            method="upsert",
            pk="external_id_2__c",
        )
        rows = dataframe.to_records(index=False).tolist()
        execute_upsert_query_on_db_table(
            postgres_db_details=postgress_db_details, query=query, rows=rows
        )

    def prepare_failed_records_data(self, object, failed_items, primary_key):
        last_modified_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S.%f")
        rows = []
        for item in failed_items:
            error_response_object = ""
            error_response_veeva = ""
            error_response_object_flag = False
            person_product_flag = False
            if primary_key == "pk_id_study_product":
                pk_value = (
                    item["data"].get("study__v.external_id_2__c", "")
                    + "-"
                    + item["data"].get("product__v.bay_number__c", "")
                )
            else:
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
                    "SDK_ERROR",
                ):
                    error_response_object += error["message"] + ";"
                    error_response_object_flag = True
                    if (
                        object == "study_person__clin"
                        and "Object field value(s) do not resolve to a valid active [study__clin.external_id_2__c]"
                        in error["message"]
                    ) or (
                        object == "study_product__v"
                        and "Object field value(s) do not resolve to a valid active [study__v.external_id_2__c]"
                        in error["message"]
                    ):
                        person_product_flag = True
                else:
                    error_response_veeva += error["message"] + ";"

            if error_response_object_flag:
                error_response = error_response_object[0:1500].replace("'", "''")
                delta_flag = "FAILED" if not person_product_flag else "Y"
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
            query = sql["update_flag_for_study_person_montblanc_table"].format(
                table=object
            )
        else:
            query = sql["update_flag_for_montblanc_table"].format(table=object)
        rows = []
        if failed_items:
            rows = self.prepare_failed_records_data(
                object=object, failed_items=failed_items, primary_key=primary_key
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

    def update_last_extraction_log(self, db_details, sql):
        LOGGER.info("Updating Last extraction time")
        last_extraction_date = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
        query = sql["update_data_extract_log"].format(
            last_extract_timestamp=last_extraction_date, dataset_name="study"
        )
        execute_query_on_db_table(db_details, query, method="update")

    def main(self, api, staging_db_details, outbound_db_details, sql):
        LOGGER.info("Loading study__v objects to vault")
        interfaces = sql["interfaces_study"]
        self.update_rejected_records(api, staging_db_details, outbound_db_details, sql)
        for table in interfaces.keys():
            LOGGER.info(f"Fetching data from {table}")
            data = self.extract_data_from_staging_db(staging_db_details, sql, table)
            updated_data = self.update_field_names(data)
            if table != "study_product__v":
                join_key = interfaces[table]["pk"]
            else:
                join_key = None
            failed_items = self.load_records_to_veeva(
                updated_data,
                object_type_name=table,
                join_key=join_key,
                api=api,
                method="upsert",
            )
            LOGGER.info(f"Fetched {len(data)} rows from database")
            self.update_flag_and_response_in_staging_db(
                staging_db_details, table, failed_items, interfaces[table]["pk"], sql
            )

        try:
            self.update_responsible_person(
                staging_db_details, outbound_db_details, sql, api
            )
        except Exception as e:
            LOGGER.error(
                f"Error occurred while updating study responsible person. Error: {e}"
            )
        try:
            self.update_candidate_lifecycle_state(
                staging_db_details, outbound_db_details, sql, api
            )
        except Exception as e:
            LOGGER.error(
                f"Error occurred while changing study Candidate to Planning lifecycle state. Error: {e}"
            )
        try:
            self.update_any_to_cancelled(
                staging_db_details, outbound_db_details, sql, api
            )
        except Exception as e:
            LOGGER.error(
                f"Error occurred while changing study Any to Cancelled lifecycle state. Error: {e}"
            )
        try:
            self.update_closing_to_closed(
                staging_db_details,
                outbound_db_details,
                sql,
                api,
            )
        except Exception as e:
            LOGGER.error(
                f"Error occurred while changing study Closing to Closed lifecycle state. Error: {e}"
            )
        self.update_last_extraction_log(staging_db_details, sql)
