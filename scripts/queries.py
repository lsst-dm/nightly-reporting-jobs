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
    "get_status_code_from_loki",
    "get_timeout_from_loki",
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


async def get_next_visit_events(day_obs, sal_index, survey):
    """Obtain uncanceled nextVisit events

    Parameters
    ----------
    day_obs : `str`
        day_obs in the format of YYYY-MM-DD.

    sal_index : `int`
        Index of Script SAL component. Use this as a proxy of the instrument.
        TODO: just use instrument.

    survey : `str`
        The imaging survey name of interest.
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

    # Only select on-sky exposures from the selected survey
    df = df.loc[
        (df["coordinateSystem"] == 2)
        & (df["salIndex"] == sal_index)
        & (df["survey"] == survey)
    ].set_index("groupId")
    _log.info(f"There were {len(df)} {survey} nextVisit events on {day_obs}")

    # Ignore the explicitly canceled groups
    if not canceled.empty:
        canceled = df.index.intersection(canceled.set_index("groupId").index).tolist()
        if canceled:
            _log.info(f"{len(canceled)} events were canceled {canceled}")
            df = df.drop(canceled)

    return df


def query_loki(day_obs, pod_name, search_string):
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
        "--limit=10000",
        "--proxy-url=http://sdfproxy.sdf.slac.stanford.edu:3128",
        f'--from={start.strftime("%Y-%m-%dT%H:%M:%SZ")}',
        f'--to={end.strftime("%Y-%m-%dT%H:%M:%SZ")}',
        f'{{app="vcluster--usdf-prompt-processing",pod=~"{pod_name}-.+"}} {search_string}',
    ]

    result = subprocess.run(command, capture_output=True, text=True)
    if result.returncode != 0:
        _log.error("Loki query failed")
        _log.error(results.stderr)
        return

    return result.stdout


def get_status_code_from_loki(day_obs):
    """Get status return codes from next-visit-fan-out

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
        pod_name="next-visit-fan-out",
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


def get_timeout_from_loki(day_obs):
    """Get the IDs of the timed out cases.

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
        pod_name="prompt-proto-service",
        search_string='|~ "Timed out waiting for image after receiving exposures"',
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
