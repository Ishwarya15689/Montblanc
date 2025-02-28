#!/usr/bin/env python
"""Wrapper module for the Veeva Vault API."""
import hashlib
import json as _json
import ssl
from datetime import datetime, timezone, timedelta
from ftplib import FTP_TLS
from io import IOBase
from threading import RLock
from urllib.parse import quote

import boto3
import requests

from . import throttle as tt

UTC = timezone.utc
MAX_UPLOAD_SIZE = 50 * 1024 * 1024

app_json = "application/json"
app_urlencoded = "application/x-www-form-urlencoded"
text_csv = "text/csv"
vobjects = "vobjects/"


def get_md5(data: bytes) -> str:
    """Calculate the MD5 hash of the data."""
    md5 = hashlib.md5()
    md5.update(data)
    return md5.hexdigest()


def merge_response(data, response):
    """
    Merge the response with the requested data, so that requested data correlates to the response.

    Data is expected to be a list with 1 to 500 dict objects.
    Response should be a dict containing a data key that contains a list of results.
    """
    ######################
    # sanity checks
    ######################

    # check that data is a list or tuple
    if not isinstance(data, (list, tuple)):
        raise VeevaError("data is not a list or tuple")

    # check that data contains 1 to 500 elements
    if len(data) not in range(1, 501):
        raise VeevaError(f"data has invalid size: {len(data)}")

    # check that response contains a list or tuple of data elements
    if "data" not in response:
        raise VeevaError("response does not contain any data")

    other = response["data"]

    if not isinstance(other, (list, tuple)):
        raise VeevaError("response data is not a list or tule")

    # check that both lists or tuples have the same length
    if len(data) != len(other):
        raise VeevaError(
            f'request and response data size mismatch {len(data)} vs. {len(response["data"])}'
        )

    ######################
    # merge list objects
    ######################
    for request_item, response_item in zip(data, other):
        # check that response item contains a data element of type dict
        if not (isinstance(response_item, dict) and isinstance(request_item, dict)):
            raise VeevaError("request or response item is not a dict")

        if "data" in response_item:
            # merge response into request (response may overwrite request data)
            request_item.update(response_item["data"])

        # store merged item in resonse
        response_item["data"] = request_item


class VeevaError(Exception):
    """A custom error for Veeva requests."""

    def __init__(self, message, response_details=None):
        """Create a new Veeva object."""
        Exception.__init__(self)
        self.message = message
        self.response_details = response_details

    def __str__(self):
        """Convert to string."""
        return f"VeevaError:\n{_json.dumps(self.message, indent=2)}"

    def __repr__(self):
        """Convert to string."""
        return self.__str__()


class Veeva:  # pylint: disable=too-many-public-methods
    """The Veeva class encapsulates communication over the REST API of Veeva."""

    def __init__(
        self,
        domain,
        apiversion,
        vault_secret,
        region_name,
        dynamodb,
        namespace="Ruby_Veeva_Exporter",
        tag="Ruby_Veeva_Exporter",
        migration_mode=False,
        boto3session=None,
    ):
        """Initialize the instance with domain name and version."""
        self.domain = domain
        self.apiversion = apiversion
        self.vault_secret = vault_secret
        self.region_name = region_name
        self.dynamodb = dynamodb
        self.username = None
        self.session_id = None
        self.session_start_date = None
        self.migration_mode = migration_mode
        self.awssession = boto3session

        # initialize cloudwatch
        if boto3session:
            self.cloudwatch = boto3session.client("cloudwatch")
        else:
            self.cloudwatch = boto3.client("cloudwatch")

        self.namespace = namespace
        self.tag = tag
        self.metrics = {}
        self.lock = RLock()

    def _make_url(self, url):
        """Add missing parts of the url."""
        prefix = f"https://{self.domain}"

        # check for qualified url - nothing to do
        if url.startswith(prefix):
            return url

        # check for leading slash
        if not url.startswith("/"):
            url = "/" + url

        # check for api version
        if not url.startswith("/api/"):
            prefix += f"/api/{self.apiversion}"

        return prefix + url

    def _put_metrics(self):
        """Send metrics to cloudwatch."""
        with self.lock:
            if self.metrics:
                self.cloudwatch.put_metric_data(
                    Namespace=self.namespace,
                    MetricData=[
                        {
                            "MetricName": name,
                            "Dimensions": [
                                {"Name": "Tag", "Value": self.tag},
                                {"Name": "Vault", "Value": self.domain},
                            ],
                            "Timestamp": datetime.now(UTC),
                            "Value": int(value),
                            "Unit": "Count",
                        }
                        for name, value in self.metrics.items()
                    ],
                )
                self.metrics.clear()

    def _request(
        self, method, url, record_metrics=True, check_response_status=True, **kwargs
    ):
        """
        Send a generic HTTP request.

        Method must be a valid HTTP method verb (i.e. GET, PUT, POST, DELETE, HEAD, PATCH)
        """
        # check current Request
        url = self._make_url(url)

        headers = kwargs.setdefault("headers", {})
        headers.setdefault("Accept", app_json)
        headers.setdefault("Content-Type", app_json)
        headers.setdefault(
            "X-VaultAPI-MigrationMode", "true" if self.migration_mode else "false"
        )

        savelimits = False
        if self.session_id:
            headers.setdefault("Authorization", self.session_id)
            savelimits = True
            # do not throttle if this a login
            tt.check_limits(
                self.domain, self.region_name, self.dynamodb, awssession=self.awssession
            )

        # send request
        response = requests.request(method, url, **kwargs)

        # check HTTP response
        response.raise_for_status()

        # check limits
        with self.lock:
            if "X-VaultAPI-DailyLimit" in response.headers:
                self.metrics["DailyLimit"] = response.headers["X-VaultAPI-DailyLimit"]

            if "X-VaultAPI-DailyLimitRemaining" in response.headers:
                self.metrics["DailyLimitRemaining"] = response.headers[
                    "X-VaultAPI-DailyLimitRemaining"
                ]

            if "X-VaultAPI-BurstLimit" in response.headers:
                self.metrics["BurstLimit"] = response.headers["X-VaultAPI-BurstLimit"]

            if "X-VaultAPI-BurstLimitRemaining" in response.headers:
                self.metrics["BurstLimitRemaining"] = response.headers[
                    "X-VaultAPI-BurstLimitRemaining"
                ]

            if record_metrics:
                self._put_metrics()

        # write values to db
        if savelimits and "X-VaultAPI-BurstLimitRemaining" in response.headers:
            responsedate = datetime.now(UTC)
            if "Date" in response.headers:
                responsedate = response.headers["Date"]

            tt.write_to_db(
                self.domain,
                response.headers["X-VaultAPI-BurstLimitRemaining"],
                responsedate,
                self.region_name,
                self.dynamodb,
                self.awssession,
            )

        # return Veeva response as JSON object
        isjason = True
        result = response
        if "Accept-Encoding" in headers:
            encoding = headers["Accept-Encoding"]
            if app_json not in encoding:
                isjason = False
        if isjason:
            result = response.json()

        # check for errors
        if (
            check_response_status
            and "responseStatus" in result
            and result["responseStatus"] != "SUCCESS"
        ):
            raise VeevaError(result)

        return result

    def get(self, url, **kwargs):
        """Perform a GET request."""
        return self._request("GET", url, **kwargs)

    def post(self, url, **kwargs):
        """Perform a POST request."""
        return self._request("POST", url, **kwargs)

    def put(self, url, **kwargs):
        """Perform a PUT request."""
        return self._request("PUT", url, **kwargs)

    def delete(self, url, **kwargs):
        """Perform a DELETE request."""
        return self._request("DELETE", url, **kwargs)

    def authenticate(self, username, password):
        """Authenticate against Veeva Vault API using username and password."""
        if (
            self.session_id is not None
            and self.session_start_date is not None
            and datetime.utcnow() - self.session_start_date < timedelta(hours=8)
        ):
            return {
                "responseStatus": "SUCCESS",
                "sessionId": self.session_id,
            }  # do not create new sessions unneseccarrily

        response = self.post(
            url="auth",
            record_metrics=False,
            headers={"Content-Type": app_urlencoded},
            data={"username": username, "password": password},
        )

        # verify authenticated vault id against requested vault
        authenticated_url = None
        if response["responseStatus"] == "SUCCESS":
            for vault in response["vaultIds"]:
                if response["vaultId"] == vault["id"]:
                    authenticated_url = vault["url"]
                    break

        if self.domain not in authenticated_url:
            raise VeevaError(
                f"Authentication against {self.domain} not possible, authenticated against {authenticated_url}",
                response,
            )

        # store the session id
        if "sessionId" in response:
            self.username = username
            self.session_id = response["sessionId"]
            self.session_start_date = datetime.utcnow()

        return response

    def get_audit_metadata(self, audit_type=None):
        """Get Veeva Vault audit metadata."""
        url = "metadata/audittrail"

        if audit_type:
            url += "/" + audit_type

        return self.get(url)

    def get_audit_data(self, audit_type, start_date, end_date):
        """Get Veeva Vault audit data."""
        url = "audittrail/" + audit_type

        return self.get(
            url,
            params={
                "start_date": _get_date(start_date),
                "end_date": _get_date(end_date),
            },
        )

    def get_object_types(self):
        """Get a list of all object types and all fields configured on each object type."""
        return self.get(url="configuration/Objecttype")

    def get_user_metadata(self):
        """Get metadata for Veeva Vault users."""
        url = "metadata/objects/users"

        return self.get(url)

    def get_group_metadata(self):
        """Get metadata for Veeva Vault groups."""
        url = "metadata/objects/groups"

        return self.get(url)

    def get_object_metadata(self, object_type=None, field_name=None):
        """Get metadata for Veeva Vault object types."""
        url = "metadata/vobjects"

        if object_type:
            url += "/" + object_type

            if field_name:
                url += "/fields/" + field_name

        return self.get(url)

    def get_users(self, user_id=None, **kwargs):
        """Get Veeva Vault users."""
        url = "objects/users"

        if user_id:
            url += "/" + user_id

        return self.get(url, **kwargs)

    def get_groups(self, group_id=None):
        """Get Veeva Vault groups."""
        url = "objects/groups"

        if group_id:
            url += "/" + group_id

        return self.get(url)

    def get_picklist(self, picklist_id=None):
        """Get Veeva Vault picklists."""
        url = "objects/picklists"

        if picklist_id:
            url += "/" + picklist_id

        return self.get(url)

    def get_object_records(self, object_type, record_id=None):
        """Get Veeva Vault objects."""
        url = vobjects + object_type

        if record_id:
            url += "/" + record_id

        return self.get(url)

    def create_object_records(self, object_type, json, id_param=None):
        """Create new objects."""
        return self.post(
            url=vobjects + object_type, json=json, params={"idParam": id_param}
        )

    def update_object_records(self, object_type, json, id_param=None):
        """Update existing objects."""
        return self.put(
            url=vobjects + object_type, json=json, params={"idParam": id_param}
        )

    def delete_object_records(self, object_type, json, id_param=None):
        """Delete objects."""
        return self.delete(
            url=vobjects + object_type, json=json, params={"idParam": id_param}
        )

    def query(self, vql):
        """Run a VQL query."""
        return self.post(
            url="query",
            headers={
                "Content-Type": app_urlencoded,
                "X-VaultAPI-DescribeQuery": "true",
            },
            data={"q": vql},
        )

    def get_deleted_documents(self, start_date=None, end_date=None):
        """Get a list of deleted object ids."""
        url = "objects/deletions/documents"

        return self.get(
            url,
            params={
                "start_date": _get_date(start_date),
                "end_date": _get_date(end_date),
            },
        )

    def get_deleted_objects(self, object_type, start_date=None, end_date=None):
        """Get a list of deleted object ids."""
        url = "objects/deletions/vobjects/" + object_type

        return self.get(
            url,
            params={
                "start_date": _get_date(start_date),
                "end_date": _get_date(end_date),
            },
        )

    def create_binder(self, json, create_async: bool = False, **kwargs):
        """
        Create a binder.

        Arguments:
        ---------
        json: A dict containing the field data.
        create_async: (default: False) When creating a binder, the binder metadata is indexed
         synchronously by default. To process the indexing asynchronously, include
         a query parameter async set to true (objects/binders?async=true). This helps
         speed up the response time from Vault when processing large amounts of data.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.post(
            f"objects/binders?async={create_async}",
            headers={"Content-Type": None},
            data=json,
            **kwargs,
        )

    def create_binder_version(self, binder_id: str, **kwargs):
        """
        Create a binder version.

        Arguments:
        ---------
        binder_id: The binder id field value.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.post(
            f"objects/binders/{binder_id}", headers={"Content-Type": None}, **kwargs
        )

    def delete_binder(self, binder_id: str, **kwargs):
        """
        Delete a binder.

        Arguments:
        ---------
        binder_id: The binder id field value.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.delete(
            f"objects/binders/{binder_id}", headers={"Content-Type": None}, **kwargs
        )

    def delete_binder_version(
        self, binder_id: str, major_version: str, minor_version: str, **kwargs
    ):
        """
        Delete a binder version.

        Arguments:
        ---------
        binder_id: The binder id field value.
        major_version: The binder major_version_number__v field value.
        minor_version: The binder minor_version_number__v field value.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.delete(
            f"objects/binders/{binder_id}/{major_version}/{minor_version}",
            headers={"Content-Type": None},
            **kwargs,
        )

    def retrieve_binder_relationship(
        self,
        binder_id: str,
        major_version: str,
        minor_version: str,
        relationship_id: str,
        **kwargs,
    ):
        """
        Retrieve a binders version relationship.

        Arguments:
        ---------
        binder_id: The binder id field value.
        major_version: The binder major_version_number__v field value.
        minor_version: The binder minor_version_number__v field value.
        relationship_id: The binder relationship id field value.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.get(
            f"objects/binders/{binder_id}/{major_version}/{minor_version}/relationships/{relationship_id}",
            headers={"Content-Type": None},
            **kwargs,
        )

    def retrieve_binder_sections(self, binder_id: str, section_id: str = "", **kwargs):
        """
        Retrieve a binders section(s).

        Arguments:
        ---------
        binder_id: The binder id field value.
        section_id: Optional: Retrieve all sections (documents and subsections) in a binder’s sub-level node. If not included,
            all sections from the binder’s top-level root node will be returned.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.get(
            f"objects/binders/{binder_id}/sections/{section_id}",
            headers={"Content-Type": None},
            **kwargs,
        )

    def retrieve_binder_version_section(
        self,
        binder_id: str,
        major_version: str,
        minor_version: str,
        section_id: str = "",
        **kwargs,
    ):
        """
        Retrieve a binders version section(s).

        Arguments:
        ---------
        binder_id: The binder id field value.
        major_version: The binder major_version_number__v field value.
        minor_version: The binder minor_version_number__v field value.
        section_id: Retrieve all sections (documents and subsections) in a binder’s sub-level node. If not included,
        all sections from the binder’s top-level root node will be returned.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.get(
            f"objects/binders/{binder_id}/{major_version}/{minor_version}/sections/{section_id}",
            headers={"Content-Type": None},
            **kwargs,
        )

    def create_binder_section(self, binder_id: str, json, **kwargs):
        """
        Create a binder section.

        Arguments:
        ---------
        binder_id: The binder id field value.
        json: A dict containing the field data.
            name__v 	[Required] Specify a name for the new section.
            section_number__v 	[Optional] Enter a numerical value for the new section.
            parent_id__v 	[Optional] If the new section is going to be a subsection,
                enter the Node ID of the parent section. If left blank, the new section
                will become a top-level section in the binder.
            order__v 	[Optional] Enter a number reflecting the position of the section
                within the binder or parent section. By default, new components appear
                below existing components. Note: There is a known issue affecting this
                parameter. The values you enter may not work as expected.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.post(
            f"objects/binders/{binder_id}/sections",
            headers={"Content-Type": None},
            data=json,
            **kwargs,
        )

    def update_binder_section(self, binder_id: str, node_id: str, json, **kwargs):
        """
        Update a binder section.

        Arguments:
        ---------
        binder_id: The binder id field value.
        node_id: The binder node id of the section.
        json: A dict containing the field data.
            name__v 	[Required] Specify a name for the new section.
            section_number__v 	[Optional] Enter a numerical value for the new section.
            parent_id__v 	[Optional] If the new section is going to be a subsection,
                enter the Node ID of the parent section. If left blank, the new section
                will become a top-level section in the binder.
            order__v 	[Optional] Enter a number reflecting the position of the section
                within the binder or parent section. By default, new components appear
                below existing components. Note: There is a known issue affecting this
                parameter. The values you enter may not work as expected.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.put(
            f"objects/binders/{binder_id}/sections/{node_id}",
            headers={"Content-Type": None},
            data=json,
            **kwargs,
        )

    def delete_binder_section(self, binder_id: str, section_id: str, **kwargs):
        """
        Update a binder section.

        Arguments:
        ---------
        binder_id: The binder id field value.
        section_id: The binder node id of the section.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.delete(
            f"objects/binders/{binder_id}/sections/{section_id}",
            headers={"Content-Type": None},
            **kwargs,
        )

    def add_document_to_binder(self, binder_id: str, json, **kwargs):
        """
        Add document to a binder.

        Arguments:
        ---------
        binder_id: The binder id field value.
        json: A dict containing the field data.
            document_id__v 	[Required] ID of the document being added to the binder.
            parent_id__v 	[Required] Section ID of the parent section, if the document
                will be in a section rather than top-level. Note: the section ID is unique
                no matter where it is in the binder hierarchy. Blank means adding the
                document at the top-level binder.
            order__v 	[Optional] Enter a number reflecting the position of the document
                within the binder or section. By default, new components appear below existing
                components. Note: There is a known issue affecting this parameter. The values
                you enter may not work as expected.
            binding_rule__v 	[Optional] binding rule indicating which version of the
                document will be linked to the binder and the ongoing behavior. Options are:
                default (bind to the latest available version (assumed if binding_rule is
                blank)), steady-state (bind to latest version in a steady-state),
                current (bind to current version), or specific (bind to a specific version).
            major_version_number__v 	If the binding_rule is specific, then this is
                required and indicates the major version of the document to be linked.
                Otherwise it is ignored.
            minor_version_number__v 	If the binding_rule is specific, then this is
                required and indicates the minor version of the document to be linked.
                Otherwise it is ignored.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.post(
            f"objects/binders/{binder_id}/documents",
            headers={"Content-Type": None},
            data=json,
            **kwargs,
        )

    def move_document_in_binder(self, binder_id: str, section_id: str, json, **kwargs):
        """
        Move a document to a differente position within a binder.

        Arguments:
        ---------
        binder_id: The binder id field value.
        section_id: The binder node id field value.
        json: A dict containing the field data.
            parent_id__v 	[Option] To move the document to a different section or
                from a section to the binder’s root node,
                enter the value of the new parent node.
            order__v 	[Optional] Enter a number reflecting the position of the document
                within the binder or section. By default, new components appear below existing
                components. Note: There is a known issue affecting this parameter. The values
                you enter may not work as expected.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.put(
            f"objects/binders/{binder_id}/documents/{section_id}",
            headers={"Content-Type": None},
            data=json,
            **kwargs,
        )

    def remove_document_from_binder(self, binder_id: str, section_id: str, **kwargs):
        """
        Remove a document from a binder.

        Arguments:
        ---------
        binder_id: The binder id field value.
        section_id: The binder node id field value.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.delete(
            f"objects/binders/{binder_id}/documents/{section_id}",
            headers={"Content-Type": None},
            **kwargs,
        )

    def create_binder_relationship(
        self, binder_id: str, major_version: str, minor_version: str, json, **kwargs
    ):
        """
        Create a binder relationship.

        Arguments:
        ---------
        binder_id: The binder id field value.
        major_version: The binder major_version_number__v field value.
        minor_version: The binder minor_version_number__v field value.
        json: A dict containing the field data. Required: target_doc_id__v, relationship_type__v, target_major_version__v, target_minor_version__v
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.post(
            f"objects/binders/{binder_id}/{major_version}/{minor_version}/relationships",
            headers={"Content-Type": None},
            data=json,
            **kwargs,
        )

    def delete_binder_relationship(
        self,
        binder_id: str,
        major_version: str,
        minor_version: str,
        relationship_id: str,
        **kwargs,
    ):
        """
        Delete a binder relationship.

        Arguments:
        ---------
        binder_id: The binder id field value.
        major_version: The binder major_version_number__v field value.
        minor_version: The binder minor_version_number__v field value.
        relationship_id: The binder relationship id field value.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.delete(
            f"objects/binders/{binder_id}/{major_version}/{minor_version}/relationships/{relationship_id}",
            headers={"Content-Type": None},
            **kwargs,
        )

    def update_binder(self, binder_id: str, json, **kwargs):
        """
        Update a binder.

        Arguments:
        ---------
        binder_id: The binder id field value.
        json: A dict containing the field data.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.put(
            f"objects/binders/{binder_id}",
            headers={"Content-Type": None},
            data=json,
            **kwargs,
        )

    def binder_update_binding_rule(self, binder_id: str, json, **kwargs):
        """
        Update binding rule.

        Arguments:
        ---------
        binder_id: The binder id field value.
        json: A dict containing the field data.
            binding_rule__v 	[Required] Indicates which binding rule to apply
                (which document versions to link to the section).
                Options are: default (bind to the latest available version (assumed
                if binding_rule is blank)), steady-state (bind to latest version
                in a steady-state), or current (bind to current version).
            binding_rule_override__v 	[Required] Set to true or false to indicate
                if the specified binding rule should override documents or sections
                which already have binding rules set. If set to true, the binding rule
                is applied to all documents and sections within the current section.
                If blank or set to false, the binding rule is applied only to documents
                and sections within the current section that do not have a binding rule
                specified.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.put(
            f"objects/binders/{binder_id}/binding_rule",
            headers={"Content-Type": None},
            data=json,
            **kwargs,
        )

    def binder_update_section_binding_rule(
        self, binder_id: str, section_id: str, json, **kwargs
    ):
        """
        Update binder section binding rule.

        Arguments:
        ---------
        binder_id: The binder id field value.
        section_id: The binder node id field value.
        json: A dict containing the field data.
            binding_rule__v 	[Required] Indicates which binding rule to apply
                (which document versions to link to the section).
                Options are: default (bind to the latest available version (assumed
                if binding_rule is blank)), steady-state (bind to latest version
                in a steady-state), or current (bind to current version).
            binding_rule_override__v 	[Required] Set to true or false to indicate
                if the specified binding rule should override documents or sections
                which already have binding rules set. If set to true, the binding rule
                is applied to all documents and sections within the current section.
                If blank or set to false, the binding rule is applied only to documents
                and sections within the current section that do not have a binding rule
                specified.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.put(
            f"objects/binders/{binder_id}/sections/{section_id}/binding_rule",
            headers={"Content-Type": None},
            data=json,
            **kwargs,
        )

    def binder_update_document_binding_rule(
        self, binder_id: str, section_id: str, json, **kwargs
    ):
        """
        Update binder document binding rule.

        Arguments:
        ---------
        binder_id: The binder id field value.
        section_id: The binder node id field value.
        json: A dict containing the field data.
            binding_rule__v 	[Optional] Indicates which binding rule to apply
                (which document versions to link to the section).
                Options are: default (bind to the latest available version
                (assumed if binding_rule is blank)), steady-state (bind to latest
                version in a steady-state), current (bind to current version),
                or specific (bind to a specific version).
            major_version_number__v 	If the binding_rule is specific, then this
                is required and indicates the major version of the document to be linked.
                Otherwise it is ignored.
            minor_version_number__v 	If the binding_rule is specific, then this
                is required and indicates the major version of the document to be linked.
                Otherwise it is ignored.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.put(
            f"objects/binders/{binder_id}/documents/{section_id}/binding_rule",
            headers={"Content-Type": None},
            data=json,
            **kwargs,
        )

    def update_binder_version(
        self, binder_id: str, major_version: str, minor_version: str, json, **kwargs
    ):
        """
        Update a binder version.

        Arguments:
        ---------
        binder_id: The binder id field value.
        major_version: The binder major_version_number__v field value.
        minor_version: The binder minor_version_number__v field value.
        json: A dict containing the field data.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.put(
            f"objects/binders/{binder_id}/{major_version}/{minor_version}",
            headers={"Content-Type": None},
            data=json,
            **kwargs,
        )

    def reclassify_binder(self, binder_id: str, json, **kwargs):
        """
        Update a binder (latest version only).

        Arguments:
        ---------
        binder_id: The binder id field value.
        json: A dict containing the field data. Required fields: type__v, subtype__v classification__v, liefcycle__v
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.put(
            f"objects/binders/{binder_id}?reclassify=true",
            headers={"Content-Type": None},
            data=json,
            **kwargs,
        )

    def create_document(self, json, file, **kwargs):
        """
        Create a single document.

        Arguments:
        ---------
        json: A dict containing the field data.
        file: A file like object for the main content.
        **kwargs: Any additional arguments to pass to the requests library

        """
        # content type will be set by the library internally and *must* be set to None here to overwrite the default of 'application/json'
        return self.post(
            "objects/documents",
            headers={"Content-Type": None},
            data=json,
            files={"file": file},
            **kwargs,
        )

    def update_document(self, doc_id, property_param, **kwargs):
        """
        Update a single document properties.

        Arguments:
        ---------
        doc_id: The document id field value
        property_param: A string containing property and values in the format eg language__v=English&product__v=1357662840171

        """
        return self.put(
            url="objects/documents/" + str(doc_id),
            headers={"Content-Type": app_urlencoded},
            data=property_param,
            **kwargs,
        )

    def update_document_version(
        self, doc_id, major_version, minor_version, property_param, **kwargs
    ):
        """
        Update editable field values on a specific version of a document.

        Arguments:
        ---------
        doc_id: The document id field value
        major_version: The document major_version_number__v field value.
        minor_version: The document minor_version_number__v field value.
        property_param: A string containing property and values in the format
            eg language__v=English&product__v=1357662840171

        """
        return self.put(
            url="objects/documents/"
            + str(doc_id)
            + "/versions/"
            + str(major_version)
            + "/"
            + str(minor_version),
            headers={"Content-Type": app_urlencoded},
            data=property_param,
            **kwargs,
        )

    def create_documents_batch(self, csvfile, migration_mode: bool = False, **kwargs):
        """
        Create multiple documents.

        The actual documents must be uploaded before via file-staging-api

        - The maximum CSV input file size is 1GB.
        - The values in the input must be UTF-8 encoded.
        - CSVs must follow the standard format.
        - The maximum batch size is 500.

        The csv file must contain: file, name__v, type__v,
        If applicable add also: subtype__v, classification__v, lifecycle__v, suppressRendition, product__v

        The csv file is uploaded as binary text file to allow unicode character processing on Veeva side.
        """
        return self.post(
            "objects/documents/batch",
            headers={
                "Content-Type": text_csv,
                "X-VaultAPI-MigrationMode": migration_mode,
            },
            data=csvfile,
            **kwargs,
        )

    def create_document_version_batch(
        self, id_param, csvfile, migration_mode: bool = False, **kwargs
    ):
        """
        Create multiple documents.

        The actual documents must be uploaded before via file-staging-api
        - The maximum CSV input file size is 1GB.
        - The values in the input must be UTF-8 encoded.
        - CSVs must follow the standard format.
        - The maximum batch size is 500.

        The csv file must contain: file, name__v, type__v, major_version_number__v, minor_version_number__v
        If applicable add also: subtype__v, classification__v, lifecycle__v, suppressRendition, product__v

        The csv file is uploaded as binary text file to allow unicode character processing on Veeva side.
        """
        return self.post(
            f"objects/documents/versions/batch?idParam={id_param}",
            headers={
                "Content-Type": text_csv,
                "X-VaultAPI-MigrationMode": str(migration_mode),
            },
            data=csvfile,
            **kwargs,
        )

    def add_multiple_renditions_batch(
        self, id_param, csvfile, migration_mode: bool = False, **kwargs
    ):
        """
        Add renditions to existing document versions.

        The actual rendition documents must be uploaded before via file-staging-api
        - The maximum CSV input file size is 1GB.
        - The values in the input must be UTF-8 encoded.
        - CSVs must follow the standard format.
        - The maximum batch size is 500.

        The csv file must contain: file, id, external_id__v (optional), rendition_type__v, major_version_number__v, minor_version_number__v
        """
        return self.post(
            f"objects/documents/renditions/batch?idParam={id_param}",
            headers={
                "Content-Type": text_csv,
                "X-VaultAPI-MigrationMode": str(migration_mode),
            },
            data=csvfile,
            **kwargs,
        )

    def delete_single_document(self, doc_id, **kwargs):
        """Delete all versions of a document, including all source files and viewable renditions."""
        return self.delete(f"objects/documents/{doc_id}", **kwargs)

    def delete_single_document_version(
        self, doc_id, major_version, minor_version, **kwargs
    ):
        """
        Delete a specific version of a document, including the version's source file and viewable rendition.

        Other versions of the document remain unchanged.
        """
        return self.delete(
            f"objects/documents/{doc_id}/versions/{major_version}/{minor_version}",
            **kwargs,
        )

    def delete_multiple_documents(self, csv_data, id_param: str = "id", **kwargs):
        """
        Delete all versions of multiple documents, including all source files and viewable renditions.

        - The maximum CSV input file size is 1GB.
        - The values in the input must be UTF-8 encoded.
        - CSVs must follow the standard format.
        - The maximum batch size is 500.

        Create a CSV or JSON input file. Choose one of the following two ways to identify documents for deletion: id, external_id__v
        """
        return self.delete(
            f"objects/documents/batch?idParam={id_param}",
            headers={"Content-Type": text_csv},
            data=csv_data,
            **kwargs,
        )

    def delete_multiple_renditions_batch(
        self, id_param, csvfile, migration_mode: bool = False, **kwargs
    ):
        """
        Delete document renditions in bulk.

        - The maximum CSV input file size is 1GB.
        - The values in the input must be UTF-8 encoded.
        - CSVs must follow the standard format.
        - The maximum batch size is 500.
        The csv file must contain: id, external_id__v (optional), rendition_type__v, major_version_number__v, minor_version_number__v
        """
        return self.post(
            f"objects/documents/renditions/batch?idParam={id_param}",
            headers={
                "Content-Type": text_csv,
                "X-VaultAPI-MigrationMode": str(migration_mode),
            },
            data=csvfile,
            **kwargs,
        )

    def get_staging_area(self):
        """Return a new FTP_TLS object, ready to use for the file staging area of the vault."""
        # connect to staging area
        # Fix - [SSL: DH_KEY_TOO_SMALL] dh key too small (_ssl.c:1123)
        context = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
        context.set_ciphers("HIGH:!DH:!aNULL")

        ftp = FTP_TLS(
            context=context,
            host=self.domain,
            user=f"{self.domain}+{self.username}",
            passwd=self.session_id,
        )

        # setup secure data connection
        ftp.prot_p()

        # go to root
        ftp.cwd("/")

        return ftp

    def file_staging_create(
        self,
        kind: str,
        path: str,
        binary_data=None,
        overwrite: bool = False,
        size: int = None,
        md5: str = None,
        **kwargs,
    ) -> dict:
        """
        Create Folder or File.

        Upload files or folders up to 50MB to the File Staging Server.

        Arguments:
        ---------
        kind:	The kind of item to create. This can be either file or folder.
        path: 	The absolute path, including file or folder name, to place the item in the file staging server.
                This path is specific to the authenticated user. Admin users can access the root directory.
                All other users can only access their own user directory.
        overwrite: 	If set to true, Vault will overwrite any existing files with the same name at the specified destination.
                For folders, this is always false.
        binary_data:   Data to upload. This has to be a byte array or string or a file like object.
        size:   (Optional) The size of the data. Use this for file-like objects, where the size can not be calculated beforehand.
        md5:    (Optional) The MD5 hash for the data. Use this for file-like objects, where the MD5 hash can not be calculated beforehand.

        """
        url = "services/file_staging/items"
        data = {"path": path, "kind": kind, "overwrite": overwrite}
        headers = {"Accept": app_json, "Content-Type": None}

        if kind == "file":
            if md5 is not None:
                data["Content-MD5"] = md5

            if isinstance(binary_data, (bytes, bytearray)):
                # data is a byte array - calculate MD5 if missing
                if md5 is None:
                    data["Content-MD5"] = get_md5(binary_data)

            result = self.post(
                url, data=data, headers=headers, files={"file": binary_data}, **kwargs
            )

            if (
                "Content-MD5" in data
                and result["responseStatus"] == "SUCCESS"
                and result["data"]["file_content_md5"] != data["Content-MD5"]
            ):
                raise VeevaError(
                    f"upload failure: md5 mismatch - expected {data['Content-MD5']}, actual {result['data']['file_content_md5']}",
                    result,
                )

            if size is not None and size != result["data"]["size"]:
                raise VeevaError(
                    f"upload failure: size mismatch - expected {size}, actual {result['data']['size']}",
                    result,
                )

            return result

        if kind == "folder":
            return self.post(url, data=data, headers=headers, **kwargs)

        raise VeevaError(f'kind must be either "file" or "folder", not "{kind}".')

    def file_staging_upload_session(self, path: str, size: int) -> dict:
        """
        Get a resumable upload session from Vault.

        Arguments:
        ---------
        path: 	The absolute path, including file or folder name, to place the item in the file staging server.
                This path is specific to the authenticated user. Admin users can access the root directory.
                All other users can only access their own user directory.
        size:   The total size of the upload.

        """
        result = self.post(
            "services/file_staging/upload",
            data={"path": path, "size": size, "overwrite": True},
            headers={
                "Accept": app_json,
                "Content-Type": app_urlencoded,
            },
        )

        return result

    def file_staging_upload(self, session: str, size: int, data):
        """
        Upload a local file in chunks of 50 MB to Veeva using a resumable session.

        Arguments:
        ---------
        session: A resumable upload session obtained from file_staging_upload_session().
        size:    The total size of the upload.
        data:    Data to upload. This can be a file-like object or a byte array.

        """
        chunk_count = size // MAX_UPLOAD_SIZE + 1

        for chunk_no in range(0, chunk_count):
            if isinstance(data, (bytes, bytearray)):
                offset = chunk_no * MAX_UPLOAD_SIZE
                chunk_data = data[offset : offset + MAX_UPLOAD_SIZE]
            elif isinstance(data, IOBase):
                chunk_data = data.read(MAX_UPLOAD_SIZE)
            else:
                raise VeevaError(
                    f"data must be a file-like object or byte array, not {type(data)}"
                )

            chunk_size = len(chunk_data)
            chunk_md5 = get_md5(chunk_data)

            result = self.put(
                f"services/file_staging/upload/{session}",
                headers={
                    "Accept": app_json,
                    "Content-Type": "application/octet-stream",
                    "Content-Length": str(chunk_size),
                    "Content-MD5": chunk_md5,
                    "X-VaultAPI-FilePartNumber": str(chunk_no + 1),
                },
                data=chunk_data,
            )

            if result["responseStatus"] == "SUCCESS":
                if result["data"]["part_number"] != chunk_no:
                    raise VeevaError(
                        f"upload failure: part number mismatch - expected {chunk_no}, actual {result['data']['part_number']}",
                        result,
                    )

                if result["data"]["size"] != chunk_size:
                    raise VeevaError(
                        f"upload failure: size mismatch - expected {chunk_size}, actual {result['data']['size']}",
                        result,
                    )

                if result["data"]["part_content_md5"] != chunk_md5:
                    raise VeevaError(
                        f"upload failure: md5 mismatch - expected {chunk_md5}, actual {result['data']['part_content_md5']}",
                        result,
                    )

    def file_staging_upload_commit(self, session: str) -> dict:
        """
        Commit an upload session.

        Arguments:
        ---------
        session: A resumable upload session obtained from file_staging_upload_session().

        """
        return self.post(
            f"services/file_staging/upload/{session}",
            headers={
                "Accept": app_json,
                "Content-Type": app_json,
            },
        )

    def file_staging_list_items(
        self, item: str, recursive: bool = False, **kwargs
    ) -> dict:
        """
        Return a list of files and folders for the specified path.

        Paths are different for Admin users (Vault Owners and System Admins) and non-Admin users.

        Arguments:
        ---------
        item: The absolute path to a file or folder.
            This path is specific to the authenticated user.
            Admin users can access the root directory.
            All other users can only access their own user directory.
        recursive: If true, the response will contain the contents of all subfolders.
                If not specified, the default value is false.

        """
        return self.get(
            f"services/file_staging/items/{quote(item)}?recursive={recursive}", **kwargs
        )

    def file_staging_delete(self, item: str, recursive: bool = False, **kwargs) -> dict:
        """
        Delete an individual file or folder from the file staging server.

        Arguments:
        ---------
        item: The absolute path to the file or folder to delete.
            This path is specific to the authenticated user.
            Admin users can access the root directory.
            All other users can only access their own user directory.
        recursive: Applicable to deleting folders only.
            If true, the request will delete the contents of a folder and all subfolders.
            The default is false.

        """
        return self.delete(
            f"services/file_staging/items/{quote(item)}?recursive={recursive}", **kwargs
        )

    def get_link_to_ui(self, object_type, record_id):
        """Get an URL to show the specified object in the Vault UI."""
        return f"https://{self.domain}/ui/#object/{object_type}/{record_id}"

    def get_user_actions(self, object_type, record_id):
        """Get a list of user actions for the specified vault object."""
        return self.get(f"vobjects/{object_type}/{record_id}/actions")

    def execute_user_action(self, object_type, record_id, action_name, **kwargs):
        """Execute a user action on the specified vault object."""
        return self.post(
            f"vobjects/{object_type}/{record_id}/actions/{action_name}",
            headers={"Content-Type": app_urlencoded},
            **kwargs
        )

    def validate_session_user(self, **kwargs):
        """
        Validate Session User.

        Given a valid session ID, this request returns information for the currently
        authenticated user. If the session ID is not valid, this request returns an
        INVALID_SESSION_ID error type. This is similar to a whoami request.
        """
        return self.get("objects/users/me", **kwargs)

    def session_keep_alive(self, **kwargs):
        """
        Session Keep Alive.

        Given an active sessionId, keep the session active by refreshing the session duration.
        A Vault session is considered active as long as some activity
        (either through the UI or API) happens within the maximum inactive session duration.
        This maximum inactive session duration varies by Vault and is configured by your Vault Admin.
        The maximum active session duration is 48 hours, which is not configurable.
        """
        return self.post("keep-alive", **kwargs)

    def loader_load(self, json, **kwargs):
        """
        Create a loader job and load a set of data files.

        Documentation: https://developer.veevavault.com/api/21.3/#multi-file-load
        """
        return self.post("services/loader/load", json=json, **kwargs)

    def retrieve_job_status(self, job_id, **kwargs):
        """
        Retrieve Job Status.

        After submitting a request, you can query your vault to determine the status
        of the request. To do this, you must have a valid job_id for a job previously
        requested through the API.
        """
        return self.get(f"services/jobs/{job_id}", **kwargs)

    def retrieve_job_tasks(self, job_id, **kwargs):
        """Retrieve the tasks associated with an SDK job."""
        return self.get(f"services/jobs/{job_id}/tasks", **kwargs)

    def retrieve_job_histories(
        self, start_date=None, end_date=None, status=None, limit=None, offset=None
    ):
        """
        Retrieve Job Histories.

        Retrieve a history of all completed jobs in the authenticated vault.

        A completed job is any job which has started and finished running,
        including jobs which did not complete successfully.
        In-progress or queued jobs do not appear here.
        Documentation: https://developer.veevavault.com/api/21.3/#retrieve-job-tasks

        Arguments:
        ---------
        start_date:	Sets the date to start retrieving completed jobs, in the format YYYY-MM-DDTHH:MM:SSZ.
            For example, for 7AM on January 15, 2016, use 2016-01-15T07:00:00Z.
            If omitted, defaults to the first completed job.
        end_date: 	Sets the date to end retrieving completed jobs, in the format YYYY-MM-DDTHH:MM:SSZ.
            For example, for 7AM on January 15, 2016, use 2016-01-15T07:00:00Z.
            If omitted, defaults to the current date and time.
        status: 	Filter to only retrieve jobs in a certain status.
            Allowed values are success, errors_encountered, failed_to_run, missed_schedule, cancelled.
            If omitted, retrieves all statuses.
        limit: 	Paginate the results by specifying the maximum number of histories per page in the response.
            This can be any value between 1 and 200. If omitted, defaults to 50.
        offset: 	Paginate the results displayed per page by specifying the amount of offset from the
            first job history returned. If omitted, defaults to 0. If you are viewing the first 50 results (page 1)
            and want to see the next page, set this to offset=51.

        """
        params = _build_job_params(start_date, end_date, status, limit, offset)
        return self.get("services/jobs/histories", params=params)

    def retrieve_job_monitors(
        self, start_date=None, end_date=None, status=None, limit=None, offset=None
    ):
        """
        Retrieve Job Monitors.

        Retrieve monitors for jobs which have not yet completed in the
        authenticated vault. An uncompleted job is any job which has not
        finished running, such as scheduled, queued, or actively running jobs.
        Completed jobs do not appear here.

        Arguments:
        ---------
        start_date:	Sets the date to start retrieving completed jobs, in the format YYYY-MM-DDTHH:MM:SSZ.
            For example, for 7AM on January 15, 2016, use 2016-01-15T07:00:00Z.
            If omitted, defaults to the first completed job.
        end_date: 	Sets the date to end retrieving completed jobs, in the format YYYY-MM-DDTHH:MM:SSZ.
            For example, for 7AM on January 15, 2016, use 2016-01-15T07:00:00Z.
            If omitted, defaults to the current date and time.
        status: 	Filter to only retrieve jobs in a certain status.
            Allowed values are success, errors_encountered, failed_to_run, missed_schedule, cancelled.
            If omitted, retrieves all statuses.
        limit: 	Paginate the results by specifying the maximum number of histories per page in the response.
            This can be any value between 1 and 200. If omitted, defaults to 50.
        offset: 	Paginate the results displayed per page by specifying the amount of offset from the
            first job history returned. If omitted, defaults to 0. If you are viewing the first 50 results (page 1)
            and want to see the next page, set this to offset=51.

        """
        params = _build_job_params(start_date, end_date, status, limit, offset)
        return self.get("services/jobs/monitors", params=params)

    def start_job(self, job_id, **kwargs):
        """
        Start job.

        Moves up a scheduled job instance to start immediately.
        Each time a user calls this API, Vault cancels the next scheduled instance
        of the specified job. For example, calling this API against a scheduled
        daily job three times would cause the job to not run according to its
        schedule for three days. Once the affected Job ID is complete, Vault will
        schedule the next instance.

        This is analogous to the Start Now option in the Vault UI.
        """
        return self.post(f"services/jobs/start_now/{job_id}", **kwargs)


def _get_date(date):
    """Check for time zone in date object and add UTC if missing."""
    if isinstance(date, datetime):
        if not date.tzinfo:
            # assume UTC if no timezone was specified
            date = date.replace(tzinfo=UTC)

        date = date.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")

    return date


def _build_job_params(start_date, end_date, status, limit, offset):
    """Build parmeters for retrieve_job_histories and retrieve_job_monitors."""
    params = {}
    if start_date is not None:
        params["start_date"] = _get_date(start_date)
    if end_date is not None:
        params["end_date"] = _get_date(end_date)
    if status is not None:
        params["status"] = status
    if limit is not None and 0 < limit < 201:
        params["limit"] = limit
    if offset is not None and offset > 0:
        params["offset"] = offset
    return params
