import requests
import sys
import os
from lsst.daf.butler import Butler, MissingCollectionError
from datetime import date

if __name__ == "__main__":
    instrument = "LSSTCam"
    webhook = "SLACK_WEBHOOK_URL_" + instrument.upper()
    url = os.getenv(webhook)

    day_obs = date.today()
    day_obs_string = day_obs.strftime("%Y-%m-%d")

    output_lines = []

    butler = Butler("embargo_readonly")
    day_chain_collection = f"{instrument}/prompt/output-{day_obs_string:s}"
    try:
        collections = butler.collections.query(day_chain_collection)
        collection = list(collections)[0]
        print(f"collection {collection} exists")
    except MissingCollectionError:
        output_lines.append(
            f"Collection {day_chain_collection} was not found in embargo_readonly."
        )

    output_message = ":postgresq: :fire: " + "\n".join(output_lines)

    if not output_lines:
        print("All good.")
        sys.exit(1)

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
