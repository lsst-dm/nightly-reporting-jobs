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
    "get_next_visit_events",
    "get_no_work_count_from_loki",
    "get_status_code_from_loki",
    "get_df_from_loki",
]
import logging
import json
import re
import subprocess

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
        return pandas.DataFrame()

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
        _log.error("Loki query failed")
        _log.error(result.stderr)
        return

    return result.stdout


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
    day_obs, task_name, instrument="LSSTCam", visit_detector=None
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
        search_string=f'|= "Nothing to do for task \'{task_name}"',
    )
    count1 = len(results.splitlines())
    # These can include images failing at single frame processing after dropping ap tasks
    # Only want those with sfm outputs and also dropping ap task
    results = query_loki(
        day_obs,
        container_name=instrument.lower(),
        search_string=f'|= "Dropping task {task_name} because no quanta remain (1 had no work to do)"',
    )
    count2 = len(results.splitlines())
    if visit_detector is not None:
        df = parse_loki_results(results)
        matches = df[
            df[["exposure", "detector"]].apply(tuple, axis=1).isin(visit_detector)
        ]
        count2 = len(matches)
    return count1, count2


def get_skipped_surveys_from_loki(day_obs, instrument="LSSTCam"):
    results = query_loki(
        day_obs,
        container_name=instrument.lower(),
        search_string='|= "Skipping visit: No pipeline configured for"',
    )

    pattern = re.compile(
        r".*Skipping visit: No pipeline configured for.*survey=(?P<survey>[-\w]*),"
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
        search_string='|= "Unsupported survey"',
    )

    pattern = re.compile(r".*RuntimeError: Unsupported survey: (?P<survey>[-\w]*)")
    unsupported_surveys = set()
    for line in results.splitlines():
        m = pattern.match(line)
        if m:
            unsupported_surveys |= {m["survey"]}
    return unsupported_surveys


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
    df["exposure"] = df["exposures"].apply(
        lambda x: x[0] if isinstance(x, list) and x else None
    )
    return df[["group", "detector", "exposure"]]
