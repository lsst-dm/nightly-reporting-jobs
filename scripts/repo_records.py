import datetime
import os
import requests
import sys

from lsst.daf.butler import CollectionType
from lsst.daf.butler.script.queryCollections import queryCollections


if __name__ == "__main__":
    url = os.getenv("SLACK_WEBHOOK_URL_TEST")
    instrument = os.getenv("INSTRUMENT")

    now = datetime.datetime.now()
    table1 = queryCollections(
        "embargo", f"{instrument}/defaults", [CollectionType.CHAINED], chains="TREE"
    )
    table2 = queryCollections(
        "embargo", f"{instrument}/templates", [CollectionType.CHAINED], chains="TREE"
    )
    output_message = (
        f":rice_ball: *{now.strftime('%A %Y-%m-%dT%H:%M')}* :rice_ball: \n"
        + "```"
        + "\n".join(table2.pformat(align="<", max_lines=-1))
        + "```"
        + "\n"
        + "```"
        + "\n".join(table1.pformat(align="<", max_lines=-1))
        + "```"
    )

    if not url:
        print("Must set environment variable SLACK_WEBHOOK_URL_TEST in order to post")
        print("Message: ")
        print(output_message)
        sys.exit(1)

    res = requests.post(
        url, headers={"Content-Type": "application/json"}, json={"text": output_message}
    )

    if res.status_code != 200:
        print("Failed to send message")
        print(res)
