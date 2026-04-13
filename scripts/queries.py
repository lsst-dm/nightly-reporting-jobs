# This file is part of nightly-reporting-jobs.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

__all__ = [
    "count_alerts",
    "get_day_obs",
    "get_next_visit_events",
    "get_nvfo_groups",
    "get_no_work_count_from_loki",
    "get_status_code_from_loki",
    "get_df_from_loki",
    "get_ignored_event_count",
    "query_exposures",
]
import logging
import json
import re
import requests
import subprocess

import astropy
from astropy.time import Time, TimeDelta
import pandas

from lsst_efd_client import EfdClient

logging.basicConfig(
    format="{levelname} {asctime} {name} - {message}",
    style="{",
)
_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)


def get_start_end(day_obs):
    """Return start time and end time of a day_obs

    Parameters
    ----------
    day_obs : `str`
        day_obs in the format of YYYY-MM-DD.
    """
    start = Time(day_obs, scale="utc", format="isot") + TimeDelta(
        12 * 60 * 60, format="sec"
    )
    end = start + TimeDelta(1, format="jd")
    return start, end


def get_day_obs(time: astropy.time.Time) -> int:
    """Convert a timestamp into a day-obs string.

    The day-obs is defined as the TAI date of an instant 12 hours before
    the timestamp.

    Parameters
    ----------
    time : `astropy.time.Time`
        The timestamp to convert.

    Returns
    -------
    day_obs : `int`
        The day_obs corresponding to ``time``, in YYYYMMDD format.
    """
    day_obs_delta = TimeDelta(-12.0 * astropy.units.hour, scale="tai")
    iso_date = (time + day_obs_delta).tai.to_value("iso", "date")
    return int(iso_date.replace("-", ""))


async def get_next_visit_events(day_obs, instrument, survey=None):
    """Obtain nextVisit events

    Parameters
    ----------
    day_obs : `str`
        day_obs in the format of YYYY-MM-DD.

    instrument : `str`
        The instrument name.

    survey : `str`, optional
        The imaging survey name of interest. If None, get all events regardless
        of the survey.

    Returns
    -------
    df : `pandas.DataFrame`
        All nextVisit events matching the criteria.
    canceled : `pandas.DataFrame`
        Canceled nextVisit events.
    """
    client = EfdClient("usdf_efd")

    topic = "lsst.sal.ScriptQueue.logevent_nextVisit"
    start, end = get_start_end(day_obs)
    df = await client.select_time_series(topic, ["*"], start.utc, end.utc)
    canceled = await client.select_time_series(
        topic + "Canceled", ["*"], start.utc, end.utc
    )

    if df.empty:
        _log.info(f"No events on {day_obs}")
        return pandas.DataFrame(
            columns=["instrument", "survey", "groupId", "filters"]
        ), pandas.DataFrame(columns=["instrument", "survey", "groupId", "filters"])

    if survey:
        # Only select on-sky exposures from the selected survey
        df = df.loc[
            (df["instrument"] == instrument) & (df["survey"] == survey)
        ].set_index("groupId")
        _log.info(f"There were {len(df)} {survey} nextVisit events on {day_obs}")
    else:
        df = df.loc[(df["instrument"] == instrument)].set_index("groupId")
        _log.info(f"There were {len(df)} {instrument} nextVisit events on {day_obs}")

    return df, canceled


async def get_alert_latency(day_obs, instrument):
    """Obtain alert latency from pipelines timing metrics

    Parameters
    ----------
    day_obs : `str`
        day_obs in the format of YYYY-MM-DD.

    instrument : `str`
        The instrument name.

    Returns
    -------
    median : `float`
        Median alert timing since shutter close in seconds.
    count : `int`
        The number of metrics.
    """
    client = EfdClient("usdfdev_efd", db_name="lsst.prompt")
    start, end = get_start_end(day_obs)
    df = await client.select_time_series(
        "lsst.prompt.prod.associationTimingMetrics",
        ["alert_timing_since_shutter_close"],
        start.utc,
        end.utc,
    )
    if df.empty:
        _log.info(f"No timing metrics on {day_obs}")
        return None, 0
    else:
        median = df["alert_timing_since_shutter_close"].median()
        return median, len(df)


def query_loki(day_obs, container_name, search_string):
    """Query Grafana Loki for log records.

    Parameters
    ----------
    day_obs : `str`
        day_obs in the format of YYYY-MM-DD.
    """
    start, end = get_start_end(day_obs)
    command = [
        "logcli",
        "query",
        "--output=jsonl",
        "--tls-skip-verify",
        "--addr=http://sdfloki.slac.stanford.edu:80",
        "--timezone=UTC",
        "-q",
        "--limit=200000",
        "--proxy-url=http://sdfproxy.sdf.slac.stanford.edu:3128",
        f'--from={start.strftime("%Y-%m-%dT%H:%M:%SZ")}',
        f'--to={end.strftime("%Y-%m-%dT%H:%M:%SZ")}',
        f'{{namespace="vcluster--usdf-prompt-processing",container="{container_name}"}} {search_string}',
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        error_msg = f"Loki query failed: {result.stderr.strip()}"
        _log.error(error_msg)
        raise RuntimeError(error_msg)

    return result.stdout


def get_nvfo_groups(day_obs, survey):
    """Get the groups next-visit-fan-out deserialized

    Parameters
    ----------
    day_obs : `str`
        day_obs in the format of YYYY-MM-DD.
    survey : `str`
        Science program survey name.

    Returns
    -------
    groups : `list`
        A list of groups that NVFO deserialized.
    """
    results = query_loki(
        day_obs,
        container_name="next-visit-fan-out",
        search_string=f'|="message deserialized" |= "{survey}"',
    )
    pattern = re.compile(r"'groupId':\s*'(?P<group>[^']+)'")
    groups = [
        m1.group("group")
        for line in results.splitlines()
        if (m1 := pattern.search(line))
    ]
    return groups


def get_status_code_from_loki(day_obs):
    """Get status return codes from next-visit-fan-out

    This assumes a Knative platform of the Prompt service.

    Parameters
    ----------
    day_obs : `str`
        day_obs in the format of YYYY-MM-DD.

    Returns
    -------
    df : `pandas.DataFrame`
    """
    results = query_loki(
        day_obs,
        container_name="next-visit-fan-out",
        search_string='|~ "status code" |~ "for initial request"',
    )
    pattern = re.compile(
        r".*nextVisit {'instrument': '(?P<instrument>\w*)', 'groupId': '(?P<group>[^' ]*)', 'detector': (?P<detector>\d*)} status code (?P<code>\d*) for.*timestamp\":\"(?P<timestamp>\S*)\""
    )
    records = []
    for line in results.splitlines():
        m1 = pattern.match(line)
        if m1:
            records.append(
                (
                    m1["instrument"],
                    m1["group"],
                    int(m1["detector"]),
                    int(m1["code"]),
                    m1["timestamp"],
                )
            )
    df = pandas.DataFrame.from_records(
        data=records, columns=["instrument", "group", "detector", "code", "timestamp"]
    )
    return df


def get_df_from_loki(
    day_obs,
    instrument="LSSTCam",
    match_string="",
    match_string2='|= "Processing failed"',
):
    """Query Loki and return matching log entries as a DataFrame.

    Parameters
    ----------
    day_obs : `str`
        day_obs in the format of YYYY-MM-DD.
    instrument : `str`
        Instrument name.
    match_string : `str`
        Loki stream selector for Loki query.
    match_string2 : `str`
        Additional search/filter expression for the Loki query.

    Returns
    -------
    df : `pandas.DataFrame`
    """
    results = query_loki(
        day_obs,
        container_name=instrument.lower(),
        search_string=f"{match_string} {match_string2}",
    )

    if not results:
        return pandas.DataFrame(columns=["instrument", "group", "detector", "ts"])

    parsed_data = []
    for result in results.splitlines():
        try:
            data = json.loads(result)
            parsed_data.append(data)
        except json.JSONDecodeError as e:
            _log.error(f"Failed to parse \n{result}\n JSON decode error: {e}")

    df = pandas.json_normalize(parsed_data)
    df = df.merge(
        pandas.json_normalize(df["line"].apply(json.loads)),
        left_index=True,
        right_index=True,
    ).drop(columns=["line"])

    return df


def get_no_work_count_from_loki(
    day_obs, task_name, survey, instrument="LSSTCam", visit_detector=None
):
    """Count the numbers with no work to do

    Parameters
    ----------
    visit_detector: `set`, optional
        A set of (visit, detector) tuples to filter with. If given,
        only count numbers overlapping this set.
    """
    results = query_loki(
        day_obs,
        container_name=instrument.lower(),
        search_string=f'|= "Nothing to do for task \'{task_name}" | json | survey="{survey}"',
    )
    count1 = len(results.splitlines())
    # These can include images failing at single frame processing after dropping ap tasks
    # Only want those with sfm outputs and also dropping ap task
    results = query_loki(
        day_obs,
        container_name=instrument.lower(),
        search_string=f'|= "Dropping task {task_name} because no quanta remain (1 had no work to do)" | json | survey="{survey}"',
    )
    count2 = len(results.splitlines())
    if visit_detector is not None:
        df = parse_loki_results(results)
        matches = df[
            df[["exposure", "detector"]].apply(tuple, axis=1).isin(visit_detector)
        ]
        count2 = len(matches)
    return count1, count2


def get_handled_surveys_from_loki(day_obs, instrument="LSSTCam"):
    results = query_loki(
        day_obs,
        container_name=instrument.lower(),
        search_string='|= "Preparing Butler for visit FannedOutVisit"',
    )

    pattern = re.compile(
        r".*Preparing Butler for visit FannedOutVisit.*survey='(?P<survey>[-\w\s]*)',"
    )
    surveys = set()
    for line in results.splitlines():
        m = pattern.match(line)
        if m:
            surveys |= {m["survey"]}
    return surveys


def get_skipped_surveys_from_loki(day_obs, instrument="LSSTCam"):
    results = query_loki(
        day_obs,
        container_name=instrument.lower(),
        search_string='|= "Skipping visit: No pipeline configured for"',
    )

    pattern = re.compile(
        r".*Skipping visit: No pipeline configured for.*survey=(?P<survey>[-\w\s]*),"
    )
    skipped_surveys = set()
    for line in results.splitlines():
        m = pattern.match(line)
        if m:
            skipped_surveys |= {m["survey"]}
    return skipped_surveys


def get_unsupported_surveys_from_loki(day_obs, instrument="LSSTCam"):
    results = query_loki(
        day_obs,
        container_name=instrument.lower(),
        search_string='|= "No pipelines config matches"',
    )

    pattern = re.compile(
        r".*RuntimeError: No pipelines config matches \([^,]+, survey=(?P<survey>[^,]+),"
    )
    unsupported_surveys = set()
    for line in results.splitlines():
        m = pattern.match(line)
        if m:
            unsupported_surveys |= {m["survey"]}
    return unsupported_surveys


def get_ignored_event_count(day_obs, instrument="LSSTCam"):
    """Get a count of nextVisit messaged ignored."""
    results = query_loki(
        day_obs,
        container_name=instrument.lower(),
        search_string='|= "Message published" |= "old, ignoring"',
    )
    return len(results.splitlines())


def parse_loki_results(results):
    """Make Loki results into a DataFrame

    Parameters
    ----------
    results : `str`

    Returns
    -------
    df : `pandas.DataFrame`
    """
    rows = []
    if not results:
        return pandas.DataFrame(columns=["group", "detector", "exposure"])
    for line in results.splitlines():
        outer = json.loads(line)
        inner = json.loads(outer["line"])
        rows.append(inner)
    df = pandas.DataFrame(rows)
    df["exposure"] = df["exposures"].apply(lambda x: int(x.strip("{}")))
    df["detector"] = df["detector"].astype("int64")
    return df[["group", "detector", "exposure"]]


def count_alerts(day_obs_string):
    """Query alert stream increase over a day_obs"""
    url = "https://prometheus.slac.stanford.edu/api/v1/query_range"

    start, end = get_start_end(day_obs_string)
    topic = "lsst-alerts-v10.0"

    params = {
        "query": f"sum by (topic) (kafka_topic_partition_current_offset{{"
        f'namespace=~"vcluster--usdf-alert-stream-broker.dev", topic="{topic}"}})',
        "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "step": "24h",
    }

    # Make a request to the Prometheus API
    response = requests.get(url, params=params)

    if response.status_code == 200:
        data = response.json()
        values = data.get("data", {}).get("result", [])

        if values:
            topic_values = values[0].get("values", [])
            if topic != values[0].get("metric", []).get("topic", []):
                _log.error(f"Alert topic {topic} not found.")
                return None
            if len(topic_values) == 2:
                first_value_timestamp, first_value_offset = topic_values[0]
                last_value_timestamp, last_value_offset = topic_values[1]
                difference = int(last_value_offset) - int(first_value_offset)
                _log.debug(f"{day_obs_string}: {difference} alerts from {topic}")
                return difference
            else:
                _log.error(f"Unexpected results: {json.dumps(data, indent=4)}")
                return None
        else:
            _log.error("No results found.")
            return None
    else:
        _log.error(f"Error: {response.status_code} - {response.text}")
        return None


def query_exposures(
    butler,
    survey,
    day_obs=None,
    time_start_tai=None,
    time_end_tai=None,
    collections=None,
):
    """Query exposures without visit_geometry from butler for a given day_obs and survey.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler instance for querying.
    survey : `str`
        Survey/science program name (e.g., "BLOCK-407").
    day_obs : `int`, optional
        Day of observation in YYYYMMDD format.
    time_start_tai : `str`, optional
        Start time in TAI format (e.g., "2026-03-31T00:08:02.994000").
        If not provided, calculated from day_obs.
    time_end_tai : `str`, optional
        End time in TAI format (e.g., "2026-03-31T05:48:02.994000").
        If not provided, calculated from day_obs.
    collections : `list` of `str`
        Collections to search for existing visit_geometry.

    Returns
    -------
    results : `list`
        A list of exposure IDs without visit_geometry.
    """

    # Step 1: Parse times as astropy Time objects, ensure TAI scale
    t_start = Time(time_start_tai, scale="tai") if time_start_tai else None
    t_end = Time(time_end_tai, scale="tai") if time_end_tai else None

    # Step 2: Determine day_obs if not given, using time_start_tai
    if day_obs is None:
        if t_start is None:
            raise ValueError("Either day_obs or time_start_tai must be provided")
        day_obs = get_day_obs(t_start)

    # Convert day_obs from integer YYYYMMDD to YYYY-MM-DD string format
    day_obs_str = f"{day_obs//10000:04d}-{(day_obs//100)%100:02d}-{day_obs%100:02d}"
    # Step 3: Get the nominal interval for the given day_obs
    day_obs_start_utc, day_obs_end_utc = get_start_end(day_obs_str)
    day_obs_start_tai = day_obs_start_utc.tai
    day_obs_end_tai = day_obs_end_utc.tai

    # Step 4: Fill in missing time_start_tai, time_end_tai from day_obs
    if t_start is None:
        t_start = day_obs_start_tai
        time_start_tai = t_start.isot
    if t_end is None:
        t_end = day_obs_end_tai
        time_end_tai = t_end.isot

    # Step 5: Consistency check -- do day_obs's interval and [t_start, t_end] overlap?
    # If not, short-circuit and return empty list.
    latest_start = max(t_start, day_obs_start_tai)
    earliest_end = min(t_end, day_obs_end_tai)
    if latest_start >= earliest_end:
        _log.warning("No overlap in time window & day_obs")
        return []  # No overlap in time window & day_obs: no exposures possible

    results = butler.query_dimension_records(
        "exposure",
        where="exposure.science_program IN (survey) "
        "and instrument=instrument_name and day_obs=day_obs "
        f"and exposure.timespan.end > T'{time_start_tai}' "
        f"and exposure.timespan.end < T'{time_end_tai}'",
        bind={"day_obs": day_obs, "instrument_name": "LSSTCam", "survey": survey},
        explain=False,
        limit=None,
    )

    exposures = {exposure.id for exposure in results}
    exposure_exists = set()
    if collections:
        exists = butler.query_datasets(
            "visit_geometry",
            collections=collections,
            find_first=False,
            where="instrument='LSSTCam' and exposure in (exposures)",
            bind={"exposures": exposures},
            explain=False,
            limit=None,
        )
        exposure_exists = {_.dataId["visit"] for _ in exists}
    return list(exposures - exposure_exists)
