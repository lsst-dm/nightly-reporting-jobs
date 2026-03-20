__all__ = ["count_redis_messages_for_visit", "count_detectors_with_outputs"]
import redis
from redis.exceptions import RedisError

from lsst.daf.butler import Butler, EmptyQueryResultError
from lsst.daf.butler.registry import DataIdError, MissingDatasetTypeError


def count_detectors_with_outputs(butler, visit_id, dataset_type="isr_log"):
    """Count how many detectors have data outputs for a given visit.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler instance to query.
    visit_id : `int`
        Visit ID to check.
    dataset_type : `str`, optional
        Dataset type to check for. Default is "isr_log".

    Returns
    -------
    count : `int`
        Number of detectors that have outputs for this visit.
    """
    try:
        exp_record = list(
            butler.query_dimension_records(
                "exposure", instrument="LSSTCam", visit=visit_id
            )
        )[0]
    except EmptyQueryResultError:
        return 0

    day_obs_raw = str(exp_record.day_obs)
    day_obs_str = f"{day_obs_raw[:4]}-{day_obs_raw[4:6]}-{day_obs_raw[6:8]}"
    collection = f"LSSTCam/prompt/output-{day_obs_str}"

    try:
        outs = butler.query_datasets(
            dataset_type,
            collections=collection,
            data_id={
                "instrument": exp_record.instrument,
                "exposure": exp_record.id,
            },
            find_first=True,
        )
        # Count unique detectors in the results
        return len(outs)
    except (EmptyQueryResultError, MissingDatasetTypeError):
        return 0

    return count


def count_redis_messages_for_visit(
    butler,
    visit_id,
    redis_host="prompt-redis.prompt-redis",
    stream_name="instrument:lsstcam",
    time_window=3600,
):
    """Count Redis Stream messages for a given visit_id.

    This counts fanned-out messages sent to workers, which corresponds to
    the number of detectors that were assigned for processing.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler instance to query for exposure records.
    visit_id : `int`
        Visit ID to check.
    redis_host : `str`, optional
        Redis host address. Default is "prompt-redis.prompt-redis".
    stream_name : `str`, optional
        Redis stream name. Default is "instrument:lsstcam".
    time_window : `int`, optional
        Time window in seconds to search around the visit start time.
        Default is 3600 (1 hour).

    Returns
    -------
    count : `int`
        Number of messages found for this visit_id.
    """
    # Get exposure record to find groupId and timing info
    try:
        exp_record = list(
            butler.query_dimension_records(
                "exposure", instrument="LSSTCam", visit=visit_id
            )
        )[0]
    except EmptyQueryResultError:
        return 0

    group_id = exp_record.group

    # Use exposure timespan to narrow search window
    # Convert to milliseconds for Redis Stream message IDs
    if exp_record.timespan and exp_record.timespan.begin:
        start_tai_sec = exp_record.timespan.begin.tai.unix
        # Search from a bit before the exposure started
        search_start_ms = int((start_tai_sec - time_window) * 1000)
        search_end_ms = int((start_tai_sec + time_window) * 1000)
        min_id = f"{search_start_ms}-0"
        max_id = f"{search_end_ms}-0"
    else:
        # Fall back to searching everything if no timespan
        min_id = "-"
        max_id = "+"

    client = redis.Redis(host=redis_host)
    count = 0

    try:
        # Read messages in the time window
        cursor = min_id
        batch_size = 1000

        while True:
            messages = client.xrange(
                stream_name, min=cursor, max=max_id, count=batch_size
            )

            if not messages:
                break

            for msg_id, msg_data in messages:
                try:
                    # Decode message
                    decoded = {
                        k.decode("utf-8"): v.decode("utf-8")
                        for k, v in msg_data.items()
                    }

                    # Check if this message matches our groupId
                    if decoded.get("groupId") == group_id:
                        count += 1

                except (UnicodeDecodeError, AttributeError):
                    # Skip malformed messages
                    continue

                # Update cursor for next batch (exclusive)
                cursor = f"({msg_id.decode('utf-8')}"

            # If we got fewer than requested, we're done
            if len(messages) < batch_size:
                break

    except RedisError as e:
        print(f"Redis error: {e}")
        return 0
    finally:
        client.close()

    return count
