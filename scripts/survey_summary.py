import asyncio
import requests
import sys
import os
from lsst.daf.butler import Butler
from datetime import date, timedelta

from queries import (
    get_handled_surveys_from_loki,
    get_next_visit_events,
    get_skipped_surveys_from_loki,
    get_unsupported_surveys_from_loki,
)


if __name__ == "__main__":
    instrument = "LSSTCam"
    webhook = "SLACK_WEBHOOK_URL_" + instrument.upper()
    url = os.getenv(webhook)

    day_obs = date.today() - timedelta(days=1)
    day_obs_string = day_obs.strftime("%Y-%m-%d")
    day_obs_int = int(day_obs_string.replace("-", ""))

    df, canceled = asyncio.run(get_next_visit_events(day_obs_string, instrument))
    df = df[df["survey"] != ""]

    output_lines = []

    butler_alias = "embargo"
    butler_nocollection = Butler(butler_alias)
    raw_exposures = butler_nocollection.query_dimension_records(
        "exposure",
        instrument=instrument,
        where=f"day_obs={day_obs_int} AND (exposure.can_see_sky or exposure.can_see_sky=NULL) AND exposure.observation_type='science'",
        explain=False,
    )
    output_lines.append(
        "Number of on-sky science exposures: {:d}".format(len(raw_exposures))
    )

    for block in df["survey"].unique():
        canceled_list = (
            df[df["survey"] == block]
            .index.intersection(canceled.set_index("groupId").index)
            .tolist()
        )
        count_canceled = len(canceled_list)
        df = df.drop(canceled_list)
        count_events = len(df[df["survey"] == block])
        filters = df[df["survey"] == block]["filters"].unique()

        raw_exposures = butler_nocollection.query_dimension_records(
            "exposure",
            instrument=instrument,
            where=f"day_obs=day_obs_int AND exposure.science_program IN (survey)",
            bind={"day_obs_int": day_obs_int, "survey": block},
            explain=False,
        )
        output_lines.append(
            f"{block}: {count_events} uncanceled nextVisit events ({count_canceled} canceled) with filters {filters}. {len(raw_exposures)} raw exposures."
        )

    unsupported_surveys = get_unsupported_surveys_from_loki(day_obs_string)
    if unsupported_surveys:
        output_lines.append(f"Unsupported survey: {', '.join(unsupported_surveys)}")

    skipped_surveys = get_skipped_surveys_from_loki(day_obs_string)
    if skipped_surveys:
        output_lines.append(f"Skipped survey: {', '.join(skipped_surveys)}")

    handled_surveys = get_handled_surveys_from_loki(day_obs_string)
    if handled_surveys:
        output_lines.append(f"Handled survey: {', '.join(handled_surveys)}")

    unknown_surveys = set(df["survey"].unique()) - skipped_surveys - handled_surveys
    if unknown_surveys:
        output_lines.append(f"Unknown survey: {', '.join(unknown_surveys)}")

    output_message = (
        f":clamps: *{instrument} {day_obs.strftime('%A %Y-%m-%d')}* :clamps: \n"
        + "\n".join(output_lines)
    )

    if not url:
        print(f"Must set environment variable {webhook} in order to post")
        print("Message: ")
        print(output_message)
        sys.exit(1)

    res = requests.post(
        url, headers={"Content-Type": "application/json"}, json={"text": output_message}
    )

    if res.status_code != 200:
        print("Failed to send message")
        print(res)
