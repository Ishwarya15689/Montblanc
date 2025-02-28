"""A module to fetch parameters from command prompt."""
import argparse
from datetime import datetime


class Parameter:
    """Parent class to get command line argumnets"""

    def __init__(self):
        """Parse command line arguments."""
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument(
            "--region_name", type=str, required=True, help="AWS Region"
        )
        self.parser.add_argument(
            "--outbounddb_secret_name",
            type=str,
            required=True,
            help="AWS Secrets Manager Instance for Outbound DB",
        )
        self.parser.add_argument(
            "--stagingdb_secret_name",
            type=str,
            required=True,
            help="AWS Secrets Manager Instance for Staging DB",
        )
        self.parser.add_argument(
            "--full_load",
            type=str,
            help="load deciding factor whether full or delta load",
        )

    def get_region(self) -> str:
        """Get AWS Region name from command line args."""
        arg = self.parser.parse_args().region_name
        return arg

    def get_outbounddb_secret_name(self) -> str:
        """Get Outbound DB Secrets instance name from command line args."""
        arg = self.parser.parse_args().outbounddb_secret_name
        return arg

    def get_stagingdb_secret_name(self) -> str:
        """Get staging DB Secrets instance name from command line args."""
        arg = self.parser.parse_args().stagingdb_secret_name
        return arg

    def full_load(self) -> str:
        """value for load deciding factor from command line args."""
        arg = self.parser.parse_args().full_load
        return arg


class VeevaParameter:
    """Parent class to get command line arguments for Veeva Importer"""

    def __init__(self):
        """Parse command line arguments."""
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument(
            "--region_name", type=str, required=True, help="AWS Region"
        )
        self.parser.add_argument(
            "--vault_secret_name",
            type=str,
            required=True,
            help="AWS Secrets Manager Instance for Vault Credentials",
        )
        self.parser.add_argument(
            "--outbounddb_secret_name",
            type=str,
            required=True,
            help="AWS Secrets Manager Instance for Outbound DB",
        )
        self.parser.add_argument(
            "--veeva_apiversion",
            type=str,
            required=True,
            help="Veeva Vault Rest API Version",
        )
        self.parser.add_argument(
            "--dynamodb", type=str, required=True, help="AWS Dynamo DB Name"
        )
        self.parser.add_argument(
            "--full_load",
            type=str,
            help="Process all records, not only new or updated ones",
        )
        self.parser.add_argument(
            "--cutoff",
            type=datetime.fromisoformat,
            default="1970-01-01",
            help="The earliest date in ISO format to retrieve audit trail entries",
        )
        self.parser.add_argument(
            "--stagingdb_secret_name",
            type=str,
            help="AWS Secrets Manager Instance for Staging DB",
        )

    def get_vault_secret(self) -> str:
        """Get Vault name from command line args."""
        return self.parser.parse_args().vault_secret_name

    def is_full_load(self) -> bool:
        """Check for incremental mode."""
        full_load_flag = False
        arg = self.parser.parse_args()
        if arg.full_load is not None and arg.full_load.lower() == "yes":
            full_load_flag = True
        return full_load_flag

    def get_region(self) -> str:
        """Get AWS Region name from command line args."""
        return self.parser.parse_args().region_name

    def get_outbounddb_secret_name(self) -> str:
        """Get Outbound DB Secrets instance name from command line args."""
        return self.parser.parse_args().outbounddb_secret_name

    def get_veeva_apiversion(self) -> str:
        """Get Veeva REST API version from command line args."""
        return self.parser.parse_args().veeva_apiversion

    def get_dynamodb(self) -> str:
        """Get dynamodb from command line args."""
        return self.parser.parse_args().dynamodb

    def get_audittrail_date(self) -> str:
        """Get the date in ISO format to retrieve audit trail data"""
        return self.parser.parse_args().cutoff

    def get_stagingdb_secret_name(self) -> str:
        """Get Outbound DB Secrets instance name from command line args."""
        return self.parser.parse_args().stagingdb_secret_name
