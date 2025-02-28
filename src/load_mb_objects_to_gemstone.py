import os
from montblanc_utils.montblanc_veeva_cmd_provider import MontBlancVeevaParameter
from milestone_importer import MilestoneImporter
from enrollment_metrics_importer import MontblancVeevaImporterMetrics
from studycountry_objects_to_gemstone import MontblancVeevaImporterStudyCountry
from study_importer import StudyVeevaImporter
from common_utils.utils import (
    get_logger,
    fetch_aws_secret,
    PostgresDbDetails,
    get_veeva_api_connection,
    read_yaml_file,
)

if __name__ == "__main__":
    logger_name = os.path.basename(__file__)
    LOGGER = get_logger(logger_name=logger_name)

    param = MontBlancVeevaParameter()
    staging_db_secret = "ngctms-test1-ec1-secrets-ctms-staging-db"
    outbound_db_secret = "ngctms-test1-ec1-secrets-ctms-outbound-db"
    region_name = "eu-central-1"
    vault_secret = param.get_vault_secret()
    dynamodb = param.get_dynamodb()
    apiversion = param.get_veeva_apiversion()

    staging_db_details = PostgresDbDetails(
        **fetch_aws_secret(staging_db_secret, region_name)
    )
    outbound_db_details = PostgresDbDetails(
        **fetch_aws_secret(outbound_db_secret, region_name)
    )

    api = get_veeva_api_connection(
        vault_secret=vault_secret,
        region_name=region_name,
        apiversion=apiversion,
        dynamodb=dynamodb,
    )
    sql = read_yaml_file(
        os.path.join(
            os.path.dirname(__file__),
            "montblanc_utils",
            "montblanc_sql.yaml",
        )
    )
    study = StudyVeevaImporter()
    study.main(api, staging_db_details, outbound_db_details, sql)
    study_country = MontblancVeevaImporterStudyCountry(
        staging_db_details,
        outbound_db_details,
        region_name,
        vault_secret,
        dynamodb,
        api,
        sql,
    )

    metrics = MontblancVeevaImporterMetrics(
        staging_db_details,
        outbound_db_details,
        region_name,
        vault_secret,
        dynamodb,
        api,
        sql,
    )
    milestone = MilestoneImporter(
        staging_db_details,
        outbound_db_details,
        region_name,
        vault_secret,
        dynamodb,
        api,
        sql,
    )
