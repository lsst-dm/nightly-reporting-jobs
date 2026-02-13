import os
import requests
import sys
from datetime import date, timedelta

from lsst.daf.butler import Butler

from gather_provenance import generate_task_report, get_package_tag


if __name__ == "__main__":
    instrument = os.getenv("INSTRUMENT")
    if not instrument:
        instrument = "LSSTCam"
    webhook = "SLACK_WEBHOOK_URL_" + instrument.upper()
    url = os.getenv(webhook)

    day_obs = date.today() - timedelta(days=1)
    day_obs_string = day_obs.strftime("%Y-%m-%d")
    output_message = (
        f":clamps: {day_obs.strftime('%A %Y-%m-%d')} :clamps: \n"
        f"Task report from ApPipe run provenance \n"
    )

    butler_nocollection = Butler("embargo")
    collections = butler_nocollection.collections.query(
        f"{instrument}/prompt/output-{day_obs_string}/Ap*"
    )
    tag = get_package_tag(butler_nocollection, collections[0])
    if tag:
        table = generate_task_report(butler_nocollection, collections[0])
        output_message += f"- Stack tag :{tag}\n"
        output_lines = []
        output_lines.extend(table.pformat())
        output_message += "\n\n```\n" + "\n".join(output_lines) + "\n```"
    else:
        sys.exit(0)

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
