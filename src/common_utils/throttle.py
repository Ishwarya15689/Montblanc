#!/usr/bin/env python
"""Monitor burst API limits for Veeva Vault API and introduce small delays if we are short of remaining calls."""
import os
import json
import logging
import time
from datetime import datetime, timedelta, timezone
from botocore.exceptions import ClientError


import boto3
from pythonjsonlogger import jsonlogger

UTC = timezone.utc

# setup logging - we cannot use veeva_exporter_utils here, because this would create a circular dependency
json_handler = logging.StreamHandler()
formatter = jsonlogger.JsonFormatter(
    fmt="%(asctime)s %(levelname)s %(name)s %(message)s"
)
json_handler.setFormatter(formatter)

logger = logging.getLogger(os.path.basename(__file__))
logger.addHandler(json_handler)
logger.propagate = False
_state = {"counter_waited": 1, "total_waited": 0}


# pylint: disable=too-many-locals, too-many-statements
# pylint: disable=too-many-locals, too-many-branches
def check_limits(domain: str, region_name: str, dynamodb: str, awssession: None):
    """Check API limits and introduce a delay if necessary."""
    if awssession != None:
        paramstore = awssession.client("ssm", region_name)
    else:
        paramstore = boto3.client("ssm", region_name)

    throttleconfig = f"throttle_config_{domain}"
    try:
        param = paramstore.get_parameter(Name=throttleconfig)
    except ClientError as error:
        # Create parameter store if it not exists
        if error.response["Error"]["Code"] == "ParameterNotFound":
            logger.info("Creating parameter store for domain: %s", domain)
            value = """{"config":{"burst_limit":500,"burst_limit_start":650,"max_delay":180,"min_delay":2}}"""
            resp = paramstore.put_parameter(
                Name=throttleconfig,
                Type="String",
                Value=value,
                Tier="Standard",
                DataType="text",
            )
            retval = resp["ResponseMetadata"].get("HTTPStatusCode")
            if retval == 200:
                logger.info("Parameter store for domain %s created", domain)
                param = paramstore.get_parameter(Name=throttleconfig)
            else:
                logger.error(resp["ResponseMetadata"])
        else:
            raise error
    config = json.loads(param["Parameter"]["Value"]).get("config")
    burstlimitstart = int(config.get("burst_limit_start"))
    burstlimit = int(config.get("burst_limit"))
    mindelay = int(config.get("min_delay"))
    maxdelay = int(config.get("max_delay"))
    vals = read_from_db(domain, region_name, dynamodb, awssession)
    burstlimitcurrent = int(vals.get("last_burnlimit"))
    lastupdate = parse_date(vals.get("last_update"))
    logger.debug("checkLimits: %s", config)
    if burstlimitcurrent < burstlimitstart:
        # check lastupdate difference to 5 Minutes
        currentdate = datetime.now(UTC)
        lastminute = lastupdate.minute
        lastsecond = lastupdate.second
        diff = 5 - (lastminute % 5)
        nextreset = lastupdate + timedelta(minutes=diff) - timedelta(seconds=lastsecond)
        if currentdate < nextreset:
            # how long until 5 Minutes are reached
            nextresetdiff = nextreset - currentdate
            nextresetseconds = nextresetdiff.seconds
            logger.debug(
                "checkLimits: current: %s, nextreset: %s - Next reset in %s seconds: check burst: %s - burstlimit: %s - current: %s",
                currentdate,
                nextreset,
                nextresetseconds,
                burstlimitstart,
                burstlimit,
                burstlimitcurrent,
            )
            buffer_time = burstlimitstart - burstlimit
            if burstlimitcurrent > burstlimit:
                divider = burstlimitcurrent - burstlimit
                logger.debug("checkLimits: divider = %s", divider)
                wait = int(nextresetseconds / divider)
            else:
                wait = int(nextresetseconds / buffer_time)
            if wait > maxdelay:
                wait = maxdelay
                logger.debug("checkLimits: Use maxdelay")
            if wait < mindelay:
                wait = mindelay
                logger.debug("checkLimits: Use mindelay")
            logger.info(
                "checkLimits: Sleeping... waited: %s times and %s seconds, next waittime: %s seconds",
                _state["counter_waited"],
                _state["total_waited"],
                wait,
            )
            time.sleep(wait)
            _state["total_waited"] = _state["total_waited"] + wait
            _state["counter_waited"] += 1
        elif _state["counter_waited"] > 1:
            logger.debug(
                "checkLimits: next reset already happended: counter timewaited: %s to 1, totalwaited: %s to 0",
                _state["counter_waited"],
                _state["total_waited"],
            )
            _state["counter_waited"] = 1
            _state["total_waited"] = 0
    else:
        logger.debug(
            "checkLimits: checkLimits: burstlimit not triggered: %s - %s",
            burstlimitstart,
            burstlimitcurrent,
        )
        if _state["counter_waited"] > 1:
            logger.debug(
                "checkLimits: burstlimitcurrent > burstlimitstart: counter timewaited: %s to 1, totalwaited: %s to 0",
                _state["counter_waited"],
                _state["total_waited"],
            )
            _state["counter_waited"] = 1
            _state["total_waited"] = 0


def write_to_db(
    domain, lastburnlimit, timestamp, region_name, dynamodb, awssession: None
):
    """Write last burn limit to DynamoDB."""
    logger.debug("writeToDB: %s - %s", timestamp, lastburnlimit)
    if awssession != None:
        table = awssession.resource("dynamodb", region_name).Table(dynamodb)
    else:
        table = boto3.resource("dynamodb", region_name).Table(dynamodb)

    table.put_item(
        Item={
            "environment": domain,
            "last_burnlimit": lastburnlimit,
            "last_update": timestamp,
        }
    )


def read_from_db(
    domain: str, region_name: str, dynamodb: str, awssession: None
) -> dict:
    """Read throttle config from DynamoDB."""
    if awssession != None:
        client = awssession.client("dynamodb")
    else:
        client = boto3.client("dynamodb")

    response = client.list_tables()
    item = {}

    if dynamodb in response["TableNames"]:
        if awssession != None:
            table = awssession.resource("dynamodb", region_name).Table(dynamodb)
        else:
            table = boto3.resource("dynamodb", region_name).Table(dynamodb)

        response = table.get_item(Key={"environment": domain})
        item = response.get("Item", {})
        logger.debug("readfromDB: %s", item)
    else:
        logger.error("%s: %s", "readfromDB", "Dynamo DB table does not exist")

    if not bool(item):
        # should only occur if table was created and this is the first call to read from the db
        logger.warning("%s: %s", "readFromDB", "Nothing found return default values")
        item = {
            "environment": domain,
            "last_burnlimit": "999",
            "last_update": datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT"),
        }

    return item


def parse_date(strdate: str) -> datetime:
    """Parse date from string."""
    try:
        # Fri, 18 Sep 2020 19:09:44 GMT
        date = datetime.strptime(strdate, "%a, %d %b %Y %H:%M:%S %Z")
        if not date.tzinfo:
            # assume UTC if no timezone was specified
            date = date.replace(tzinfo=UTC)

        date = date.astimezone(UTC)
    except ValueError:
        # handle malformed/missing dates
        logger.warning("invalid data formate %s", strdate)
        date = datetime.utcnow()

    return date
