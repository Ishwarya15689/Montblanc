"""Utilty file for Preload System check"""

import os
import yaml

from common_utils.utils import get_logger, read_yaml_file

LOGGER = get_logger(os.path.basename(__file__))


def validate_secret(secret_name: str, secret_dict: dict) -> str:
    msg = ""
    empty_fields = []
    for k, v in secret_dict.items():
        if len(v) == 0:
            empty_fields.append(k)
    if len(empty_fields) > 0:
        msg = f"The listed fields are blank in secret {secret_name} - {empty_fields}. These fields are required to run the montblanc-to-gemstone integration."
    return msg


def validate_filepath(path: str) -> str:
    msg = ""
    is_file = os.path.isfile(path)
    if not is_file:
        msg = f"The file {path} does not exist. This file is required to run the montblanc-to-gemstone integration."
        return msg
    return msg


def check_query_exists(sql_data: dict, tags: list) -> str:
    empty_tags = []
    msg = ""
    for tag in tags:
        if len(sql_data.get(tag, "")) == 0:
            empty_tags.append(tag)
    if len(empty_tags) > 0:
        msg = f"Unable to fetch data from montblanc DB as the listed queries are not present in the YAML file - {empty_tags}"
    return msg


def system_check(d_secrets: dict, query_list: list, sql_yaml_filepath: str) -> bool:
    """This function checks whether all the environment variables, SQL YAML file, required queries and secrets are present or not

    Args
    ====
        d_secrets (dict): Dictionary of required secrets
        query_list (list): cList of queies to be checked
        sql_yaml_filepath (str): SQL Yaml file path

    Returns
    =======
        bool: True if Preload system check passed else False
    """
    error_flag = False

    # Validate YAML file path and required queries
    validate_file_msg = validate_filepath(sql_yaml_filepath)
    if len(validate_file_msg) > 0:
        LOGGER.error(validate_file_msg)
        error_flag = True
    else:
        # Validate the queries in the YAML file
        try:
            sql_data = read_yaml_file(sql_yaml_filepath)
        except Exception as e:
            LOGGER.error(
                f"An error occured while opening {sql_yaml_filepath} during Pre-load system check - {e}"
            )
            raise
        check_query_msg = check_query_exists(sql_data, query_list)
        if len(check_query_msg) > 0:
            LOGGER.error(check_query_msg)
            error_flag = True

    # validate the secrets
    for secret_name, secret_dict in d_secrets.items():
        validate_secret_msg = validate_secret(secret_name, secret_dict)
        if len(validate_secret_msg) > 0:
            LOGGER.error(validate_secret_msg)
            error_flag = True

    return error_flag
