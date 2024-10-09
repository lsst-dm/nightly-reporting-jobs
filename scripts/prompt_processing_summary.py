import asyncio
import sys
import os
import lsst.daf.butler as dafButler
from dataclasses import dataclass
from datetime import date, timedelta
import requests

from queries import (
    get_next_visit_events,
    get_status_code_from_loki,
    get_timeout_from_loki,
)


def make_summary_message(day_obs):
    """Make Prompt Processing summary message for a night

    Parameters
    ----------
    day_obs : `str`
        day_obs in the format of YYYY-MM-DD.
    """

    output_lines = []

    day_obs_int = int(day_obs.replace("-", ""))

    survey = "BLOCK-306"
    next_visits = asyncio.run(get_next_visit_events(day_obs, 2, survey))

    butler_nocollection = dafButler.Butler("/repo/embargo")
    raw_exposures = butler_nocollection.query_dimension_records(
        "exposure",
        instrument="LATISS",
        where=f"day_obs={day_obs_int} AND exposure.can_see_sky AND exposure.observation_type='science'",
        explain=False,
    )

    # Do not send message if there are no on-sky exposures.
    if len(raw_exposures) == 0:
        sys.exit(0)

    output_lines.append("Number of on-sky exposures: {:d}".format(len(raw_exposures)))

    raw_exposures = butler_nocollection.query_dimension_records(
        "exposure",
        instrument="LATISS",
        where=f"day_obs=day_obs_int AND exposure.science_program IN (survey)",
        bind={"day_obs_int": day_obs_int, "survey": survey},
        explain=False,
    )

    output_lines.append(
        f"Number for {survey}: {len(next_visits)} uncanceled nextVisit, {len(raw_exposures):d} raws"
    )

    if len(raw_exposures) == 0:
        return "\n".join(output_lines)

    try:
        collections = butler_nocollection.collections.query(
            f"LATISS/prompt/output-{day_obs:s}"
        )
        collection = list(collections)[0]
    except dafButler.MissingCollectionError:
        output_lines.append(f"No output collection was found for {day_obs:s}")
        return "\n".join(output_lines)

    sfm_counts = len(
        butler_nocollection.query_datasets(
            "isr_log",
            collections=f"LATISS/prompt/output-{day_obs:s}/SingleFrame*",
            where=f"exposure.science_program IN (survey)",
            bind={"survey": survey},
            find_first=False,
            explain=False,
        )
    )
    dia_counts = len(
        butler_nocollection.query_datasets(
            "isr_log",
            collections=f"LATISS/prompt/output-{day_obs:s}/ApPipe*",
            where=f"exposure.science_program IN (survey)",
            bind={"survey": survey},
            find_first=False,
            explain=False,
        )
    )

    b = dafButler.Butler("/repo/embargo", collections=[collection, "LATISS/defaults"])

    log_visit_detector = set(
        [
            (x.dataId["exposure"], x.dataId["detector"])
            for x in b.query_datasets(
                "isr_log",
                where=f"exposure.science_program IN (survey)",
                bind={"survey": survey},
            )
        ]
    )
    output_lines.append(
        "Number of main pipeline runs: {:d} total, {:d} SingleFrame, {:d} ApPipe".format(
            len(log_visit_detector), sfm_counts, dia_counts
        )
    )

    sfm_outputs = len(
        b.query_datasets(
            "initial_photometry_match_detector",
            where=f"exposure.science_program IN (survey)",
            bind={"survey": survey},
            explain=False,
        )
    )
    output_lines.append(
        "- ProcessCcd: {:d} attempts, {:d} succeeded, {:d} failed.".format(
            sfm_counts + dia_counts, sfm_outputs, sfm_counts + dia_counts - sfm_outputs
        )
    )

    dia_visit_detector = set(
        [
            (x.dataId["visit"], x.dataId["detector"])
            for x in b.query_datasets(
                "apdb_marker",
                where=f"exposure.science_program IN (survey)",
                bind={"survey": survey},
                explain=False,
            )
        ]
    )
    output_lines.append(
        "- ApPipe: {:d} attempts, {:d} succeeded, {:d} failed.".format(
            dia_counts, len(dia_visit_detector), dia_counts - len(dia_visit_detector)
        )
    )

    output_lines.append(
        f"<https://usdf-rsp-dev.slac.stanford.edu/times-square/github/lsst-dm/vv-team-notebooks/PREOPS-prompt-error-msgs?day_obs={day_obs}&instrument=LATISS&ts_hide_code=1|Full Error Log>"
    )

    raws = {r.id: r.group for r in raw_exposures}
    log_group_detector = {
        (raws[visit], detector) for visit, detector in log_visit_detector
    }
    df = get_status_code_from_loki(day_obs)
    df = df[(df["instrument"] == "LATISS") & (df["group"].isin(raws.values()))]

    status_groups = df.set_index(["group", "detector"]).groupby("code").groups
    for code in status_groups:
        counts = len(status_groups[code])
        output_lines.append(f"- {counts} counts have status code {code}.")

        indices = status_groups[code].intersection(log_group_detector)
        if not indices.empty and code != 200:
            output_lines.append(f"  - {len(indices)} have outputs.")
            counts -= len(indices)

        match code:
            case 500:
                df = get_timeout_from_loki(day_obs)
                df = df[
                    (df["instrument"] == "LATISS") & (df["group"].isin(raws.values()))
                ].set_index(["group", "detector"])
                indices = status_groups[code].intersection(df.index)
                if not indices.empty:
                    output_lines.append(f"  - {len(indices)} timed out.")
                    counts -= len(indices)
                if counts > 0:
                    output_lines.append(f"  - {counts} to be investigated.")

    output_lines.append(
        f"<https://usdf-rsp-dev.slac.stanford.edu/times-square/github/lsst-sqre/times-square-usdf/prompt-processing/groups?date={day_obs}&instrument=LATISS&survey={survey}&mode=DEBUG&ts_hide_code=1|Timing plots>"
    )

    return "\n".join(output_lines)


if __name__ == "__main__":
    url = os.getenv("SLACK_WEBHOOK_URL")

    day_obs = date.today() - timedelta(days=1)
    day_obs_string = day_obs.strftime("%Y-%m-%d")
    summary = make_summary_message(day_obs_string)
    output_message = (
        f":clamps: *LATISS {day_obs.strftime('%A %Y-%m-%d')}* :clamps: \n" + summary
    )

    if not url:
        print("Must set environment variable SLACK_WEBHOOK_URL in order to post")
        print("Message: ")
        print(output_message)
        sys.exit(1)

    res = requests.post(
        url, headers={"Content-Type": "application/json"}, json={"text": output_message}
    )

    if res.status_code != 200:
        print("Failed to send message")
        print(res)
