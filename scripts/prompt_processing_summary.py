import sys
import os
import lsst.daf.butler as dafButler
from dataclasses import dataclass
from datetime import date, timedelta
import requests


def make_summary_message(day_obs):
    """Make Prompt Processing summary message for a night

    Parameters
    ----------
    day_obs : `str`
        day_obs in the format of YYYY-MM-DD.
    """

    output_lines = []

    butler_nocollection = dafButler.Butler("/repo/embargo")
    try:
        collections = butler_nocollection.registry.queryCollections(
            f"LATISS/prompt/output-{day_obs:s}"
        )
        collection = list(collections)[0]
    except dafButler.registry.MissingCollectionError:
        output_lines.append(f"No output collection was found for {day_obs:s}")
        return "\n".join(output_lines)

    b = dafButler.Butler("/repo/embargo", collections=[collection, "LATISS/defaults"])

    day_obs_int = int(day_obs.replace('-',''))
    raw_visit_detector = set([(x.dataId['exposure'], x.dataId['detector']) for x in b.registry.queryDatasets("raw", where=f"exposure.day_obs={day_obs_int} AND exposure.observation_type='science'")])
    output_lines.append("Number of science raws: {:d}".format(len(raw_visit_detector)))

    if len(raw_visit_detector) == 0:
        sys.exit(0)

    log_visit_detector = set([(x.dataId['exposure'], x.dataId['detector']) for x in b.registry.queryDatasets("isr_log")])
    output_lines.append("Number of ISRs attempted: {:d}".format(len(log_visit_detector)))

    pvi_visit_detector = set([(x.dataId['visit'], x.dataId['detector']) for x in b.registry.queryDatasets("initial_pvi")])
    output_lines.append("Number of successful initial_pvi results: {:d}".format(len(pvi_visit_detector)))

    missing_pvis = set(log_visit_detector - pvi_visit_detector)
    missing_visits = [x[0] for x in missing_pvis]
    output_lines.append("Number of unsuccessful processCcd attempts (no resulting initial_pvi): {:d}".format(len(missing_pvis)))

    dia_visit_detector = set([(x.dataId['visit'], x.dataId['detector']) for x in b.registry.queryDatasets("apdb_marker")])
    output_lines.append("Number of successful DIA attempted: {:d}".format(len(dia_visit_detector)))

    missing_dias = set(log_visit_detector - dia_visit_detector)
    missing_visits = [x[0] for x in missing_dias]
    output_lines.append("Number of unsuccessful DIA attempts (no resulting apdb_marker): {:d}".format(len(missing_dias)))

    output_lines.append(f"<https://usdf-rsp-dev.slac.stanford.edu/times-square/github/lsst-dm/vv-team-notebooks/PREOPS-prompt-error-msgs?day_obs={day_obs}&instrument=LATISS&ts_hide_code=1|Full Error Log>")

    return "\n".join(output_lines)


if __name__ == "__main__":

    url = os.getenv("SLACK_WEBHOOK_URL")

    day_obs = date.today() - timedelta(days=1)
    day_obs_string = day_obs.strftime("%Y-%m-%d")
    summary = make_summary_message(day_obs_string)
    output_message = f"*LATISS {day_obs.strftime('%A %Y-%m-%d')}*\n" + summary

    if not url:
        print("Must set environment variable SLACK_WEBHOOK_URL in order to post")
        print("Message: ")
        print(output_message)
        sys.exit(1)

    res = requests.post(
            url, headers={"Content-Type": "application/json"},
            json={"text": output_message}
            )

    if(res.status_code != 200):
        print("Failed to send message")
        print(res)
