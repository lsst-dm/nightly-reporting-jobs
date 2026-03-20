"""estimate_from_logs.py - Extract visit processing stats from log aggregation."""

__all__ = ["count_detectors_from_logs"]

import json
import re
import requests
from datetime import datetime, timezone
from lsst.daf.butler import Butler, EmptyQueryResultError


# Configurable search phrases
START_PHRASE = "Unpacked message as"
FINISH_PHRASE = "Request took"
FINISH_PHRASE = "Main pipeline successfully run"
MIDDLE_PHRASES = {
    "ready": "Waiting for snap",
    "ingest": "Ingesting image",
    # Add more checkpoints here as needed:
    # "calibrate": "Calibrating detector",
    # "diff_image": "Generating difference image",
}

# Set which checkpoint to use as time reference
# Options: None, "start", "finish", "exposure_end", or any key from MIDDLE_PHRASES
TIME_REFERENCE = "exposure_end"  # Set to None or "start" to use start time as reference

# Pipelines to exclude from counting
EXCLUDED_PIPELINES = {"Preprocessing"}

# Pattern to extract duration from "Request took X.XXX s." messages
DURATION_PATTERN = r"Request took\s+([\d.]+)\s+s\."
# Pattern to extract result from "Result: Success" messages
RESULT_PATTERN = r"Result:\s+(\w+)"
# Pattern to extract pipeline name from "Running '/app/pipelines/LSSTCam/ApPipe.yaml'" messages
PIPELINE_PATTERN = r"Running\s+'/app/pipelines/LSSTCam/(?!Preprocessing)(\w+)\.yaml'"


def loki_query(
    group_id,
    time_range,
    namespace="vcluster--usdf-prompt-processing",
    container="lsstcam",
):
    """Query Grafana Loki for log records matching a groupId.

    Parameters
    ----------
    group_id : `str`
        The groupId to filter on.
    time_range : `tuple` of `float`, optional
        (start_timestamp, end_timestamp) in Unix seconds.
    namespace : `str`, optional
        Kubernetes namespace for the logs.
    container : `str`, optional
        Container name for the logs.

    Returns
    -------
    records : `list` of `dict`
        List of log records with 'message', 'group', 'detector', 'timestamp' keys.
    """
    loki_url = "http://sdfloki.slac.stanford.edu:80"

    # Build LogQL query - line filter BEFORE json parsing
    # Combine all search phrases with OR, be more specific with Running pattern
    all_phrases = [START_PHRASE, FINISH_PHRASE, "Running '/app/pipelines"] + list(
        MIDDLE_PHRASES.values()
    )
    search_pattern = "|".join(all_phrases)
    logql = (
        f'{{namespace="{namespace}", container="{container}"}} |~ "{search_pattern}"'
    )

    params = {
        "query": logql,
        "limit": 50000,  # Adjust if you expect more than 10k log lines
    }

    if time_range:
        params["start"] = int(time_range[0] * 1e9)  # Convert to nanoseconds
        params["end"] = int(time_range[1] * 1e9)

    try:
        response = requests.get(
            f"{loki_url}/loki/api/v1/query_range", params=params, timeout=60
        )
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        raise RuntimeError(f"Loki query failed: {e}")

    records = []
    if data.get("status") == "success":
        for stream in data.get("data", {}).get("result", []):
            for value in stream.get("values", []):
                timestamp_ns, log_line = value

                try:
                    # Parse JSON log line
                    log_json = json.loads(log_line)
                    message = log_json.get("message", "")

                    # Extract group and detector from message context
                    group = log_json.get("group")
                    detector = None

                    # Try to get detector from structured field
                    detector_field = log_json.get("detector")
                    if detector_field is not None:
                        try:
                            detector = int(detector_field)
                        except (ValueError, TypeError):
                            pass

                    # If not in structured fields, try to parse from message
                    if detector is None and "detector=" in message:
                        try:
                            detector_str = (
                                message.split("detector=")[1]
                                .split(",")[0]
                                .split(")")[0]
                            )
                            detector = int(detector_str)
                        except (IndexError, ValueError):
                            pass

                    # Only include messages for the requested group_id
                    if group == group_id or group_id in message:
                        records.append(
                            {
                                "message": message,
                                "group": group,
                                "detector": detector,  # Always int or None
                                "timestamp": int(timestamp_ns) / 1e9,
                                "asctime": log_json.get("asctime"),
                            }
                        )

                except json.JSONDecodeError:
                    # Skip non-JSON lines
                    continue

    return records


def _parse_asctime(asctime_str):
    """Parse asctime string to timestamp.

    Parameters
    ----------
    asctime_str : `str`
        Timestamp string in format "2026-03-16T19:16:11.217+0000"

    Returns
    -------
    timestamp : `float` or `None`
        Unix timestamp in seconds, or None if parsing failed.
    """
    if not asctime_str:
        return None
    try:
        # Parse ISO format with timezone
        dt = datetime.fromisoformat(asctime_str.replace("+0000", "+00:00"))
        return dt.timestamp()
    except (ValueError, AttributeError):
        return None


def _empty_stats():
    """Return empty statistics dictionary."""
    stats = {
        "started": 0,
        "finished": 0,
        "max_duration_reported": None,
        "min_duration_reported": None,
        "avg_duration_reported": None,
        "max_duration_actual": None,
        "min_duration_actual": None,
        "avg_duration_actual": None,
        "results": {},
        "time_reference": TIME_REFERENCE if TIME_REFERENCE else "start",
        "pipelines": {},
    }

    # Add relative timing stats for start
    stats["start_count"] = 0
    stats["max_relative_time_start"] = None
    stats["min_relative_time_start"] = None
    stats["avg_relative_time_start"] = None

    # Add relative timing stats for finish
    stats["finish_relative_count"] = 0
    stats["max_relative_time_finish"] = None
    stats["min_relative_time_finish"] = None
    stats["avg_relative_time_finish"] = None

    # Add middle checkpoint stats
    for checkpoint_name in MIDDLE_PHRASES.keys():
        stats[f"{checkpoint_name}_count"] = 0
        stats[f"max_relative_time_{checkpoint_name}"] = None
        stats[f"min_relative_time_{checkpoint_name}"] = None
        stats[f"avg_relative_time_{checkpoint_name}"] = None

    return stats


def count_detectors_from_logs(
    butler, visit_id, namespace="vcluster--usdf-prompt-processing", container="lsstcam"
):
    """Count detector processing statistics from Loki logs.

    This queries logs for start, middle checkpoint, and finish messages to compile statistics.
    Times are reported relative to the TIME_REFERENCE checkpoint (can be negative).

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler instance to query for exposure records (to get groupId).
    visit_id : `int`
        Visit ID to check.
    namespace : `str`, optional
        Kubernetes namespace for the logs.
    container : `str`, optional
        Container name for the logs.

    Returns
    -------
    stats : `dict`
        Dictionary with keys:
        - 'started': Number of detectors that started processing
        - 'finished': Number of detectors that finished processing
        - 'time_reference': Which checkpoint is used as time zero
        - 'pipelines': Dict mapping pipeline names to counts (e.g., {'ApPipe': 150, 'SingleFrame': 20})
        - 'max_duration_reported': Maximum from "Request took X s." (or None)
        - 'min_duration_reported': Minimum from "Request took X s." (or None)
        - 'avg_duration_reported': Average from "Request took X s." (or None)
        - 'max_duration_actual': Maximum from start->finish timestamps (or None)
        - 'min_duration_actual': Minimum from start->finish timestamps (or None)
        - 'avg_duration_actual': Average from start->finish timestamps (or None)
        - 'start_count': Number with timing relative to reference
        - 'max_relative_time_start': Max time of start relative to reference (or None)
        - 'min_relative_time_start': Min time of start relative to reference (or None)
        - 'avg_relative_time_start': Avg time of start relative to reference (or None)
        - 'finish_relative_count': Number with timing relative to reference
        - 'max_relative_time_finish': Max time of finish relative to reference (or None)
        - 'min_relative_time_finish': Min time of finish relative to reference (or None)
        - 'avg_relative_time_finish': Avg time of finish relative to reference (or None)
        - '<checkpoint>_count': Number of detectors that reached this checkpoint
        - 'max_relative_time_<checkpoint>': Max time relative to reference (or None)
        - 'min_relative_time_<checkpoint>': Min time relative to reference (or None)
        - 'avg_relative_time_<checkpoint>': Avg time relative to reference (or None)
        - 'results': Dict mapping result types to counts (e.g., {'Success': 150, 'Error': 2})
    """
    # Get exposure record to find groupId
    try:
        exp_record = list(
            butler.query_dimension_records(
                "exposure", instrument="LSSTCam", visit=visit_id
            )
        )[0]
    except EmptyQueryResultError:
        return _empty_stats()

    group_id = exp_record.group

    # Get exposure end time if needed for time reference
    exposure_end_timestamp = None
    if exp_record.timespan and exp_record.timespan.end:
        exposure_end_dt = exp_record.timespan.end.utc.datetime

        # The datetime is in UTC but has no timezone info (naive datetime)
        # We need to make it timezone-aware before calling .timestamp()
        if exposure_end_dt.tzinfo is None:
            exposure_end_dt = exposure_end_dt.replace(tzinfo=timezone.utc)

        exposure_end_timestamp = exposure_end_dt.timestamp()

    # Define time range for log query (use exposure timespan if available)
    if exp_record.timespan and exp_record.timespan.begin:
        # Search from before exposure start to well after
        start_time = exp_record.timespan.begin.tai.unix - 3600  # 1 hour before
        end_time = start_time + 7200  # 2 hours window
        time_range = (start_time, end_time)
    else:
        time_range = None

    # Query logs
    try:
        log_records = loki_query(group_id, time_range, namespace, container)
    except Exception as e:
        print(f"Log query error: {e}")
        return _empty_stats()

    # Parse log records
    # Track start, middle checkpoints, and finish times per detector (detector is always int)
    detector_start_times = {}  # int detector -> str asctime
    detector_finish_times = {}  # int detector -> str asctime
    detector_checkpoint_times = {
        name: {} for name in MIDDLE_PHRASES.keys()
    }  # checkpoint -> {detector -> asctime}
    durations_reported = []  # From "Request took X s."
    durations_actual = []  # From timestamp differences (start -> finish)
    results = {}
    pipelines = {}  # Pipeline name -> count

    for record in log_records:
        message = record.get("message", "")
        detector = record.get("detector")  # Always int or None
        asctime = record.get("asctime")

        # Check for start message
        if START_PHRASE in message:
            if detector is not None and asctime:
                detector_start_times[detector] = asctime

        # Check for middle checkpoint messages
        for checkpoint_name, checkpoint_phrase in MIDDLE_PHRASES.items():
            if checkpoint_phrase in message:
                if detector is not None and asctime:
                    detector_checkpoint_times[checkpoint_name][detector] = asctime

        # Check for pipeline execution
        pipeline_match = re.search(PIPELINE_PATTERN, message)
        if pipeline_match:
            pipeline_name = pipeline_match.group(1)
            if pipeline_name not in EXCLUDED_PIPELINES:
                pipelines[pipeline_name] = pipelines.get(pipeline_name, 0) + 1

        # Check for completion message
        if FINISH_PHRASE in message:
            if detector is not None and asctime:
                detector_finish_times[detector] = asctime

            # Extract reported duration if pattern exists
            duration_match = re.search(DURATION_PATTERN, message)
            if duration_match:
                try:
                    duration = float(duration_match.group(1))
                    durations_reported.append(duration)
                except ValueError:
                    pass

            # Extract result if pattern exists
            result_match = re.search(RESULT_PATTERN, message)
            if result_match:
                result = result_match.group(1)
                results[result] = results.get(result, 0) + 1

    # Calculate actual durations from timestamps (start -> finish)
    for detector in detector_finish_times:
        if detector in detector_start_times:
            start_ts = _parse_asctime(detector_start_times[detector])
            finish_ts = _parse_asctime(detector_finish_times[detector])
            if start_ts and finish_ts:
                duration_actual = finish_ts - start_ts
                durations_actual.append(duration_actual)

    # Determine which times to use as reference
    reference = TIME_REFERENCE if TIME_REFERENCE else "start"

    if reference == "exposure_end":
        # Use exposure end time as reference (constant for all detectors)
        if exposure_end_timestamp is None:
            print(
                "Warning: exposure_end requested but timespan.end not available, falling back to start"
            )
            detector_reference_times = detector_start_times
            reference_timestamp = None  # Will use per-detector times
            reference = "start"
        else:
            detector_reference_times = None  # Signal to use constant reference
            reference_timestamp = exposure_end_timestamp
    elif reference in ("start", None):
        detector_reference_times = detector_start_times
        reference_timestamp = None
        reference = "start"
    elif reference == "finish":
        detector_reference_times = detector_finish_times
        reference_timestamp = None
    elif reference in MIDDLE_PHRASES.keys():
        detector_reference_times = detector_checkpoint_times[reference]
        reference_timestamp = None
    else:
        # Fallback to start if invalid reference
        detector_reference_times = detector_start_times
        reference_timestamp = None
        reference = "start"

    # Calculate relative times for all checkpoints
    relative_times_start = []
    relative_times_finish = []
    relative_times_checkpoints = {name: [] for name in MIDDLE_PHRASES.keys()}

    # Helper function to get reference time for a detector
    def get_reference_time(detector):
        if reference_timestamp is not None:
            # Constant reference (e.g., exposure_end)
            return reference_timestamp
        elif detector_reference_times and detector in detector_reference_times:
            # Per-detector reference
            return _parse_asctime(detector_reference_times[detector])
        return None

    # Relative times for start
    for detector in detector_start_times:
        start_ts = _parse_asctime(detector_start_times[detector])
        ref_ts = (
            get_reference_time(detector)
            if reference_timestamp is None
            else reference_timestamp
        )
        if start_ts is not None and ref_ts is not None:
            relative_time = start_ts - ref_ts  # Can be negative
            relative_times_start.append(relative_time)

    # Relative times for finish
    for detector in detector_finish_times:
        finish_ts = _parse_asctime(detector_finish_times[detector])
        ref_ts = (
            get_reference_time(detector)
            if reference_timestamp is None
            else reference_timestamp
        )
        if finish_ts is not None and ref_ts is not None:
            relative_time = finish_ts - ref_ts  # Can be negative
            relative_times_finish.append(relative_time)

    # Relative times for middle checkpoints
    for checkpoint_name in MIDDLE_PHRASES.keys():
        for detector in detector_checkpoint_times[checkpoint_name]:
            checkpoint_ts = _parse_asctime(
                detector_checkpoint_times[checkpoint_name][detector]
            )
            ref_ts = (
                get_reference_time(detector)
                if reference_timestamp is None
                else reference_timestamp
            )
            if checkpoint_ts is not None and ref_ts is not None:
                relative_time = checkpoint_ts - ref_ts  # Can be negative
                relative_times_checkpoints[checkpoint_name].append(relative_time)

    # Build results dictionary
    stats = {
        "started": len(detector_start_times),
        "finished": len(detector_finish_times),
        "time_reference": reference,
        "pipelines": pipelines,  # Pipeline counts
        "max_duration_reported": (
            max(durations_reported) if durations_reported else None
        ),
        "min_duration_reported": (
            min(durations_reported) if durations_reported else None
        ),
        "avg_duration_reported": (
            sum(durations_reported) / len(durations_reported)
            if durations_reported
            else None
        ),
        "max_duration_actual": max(durations_actual) if durations_actual else None,
        "min_duration_actual": min(durations_actual) if durations_actual else None,
        "avg_duration_actual": (
            sum(durations_actual) / len(durations_actual) if durations_actual else None
        ),
        "results": results,
        # Relative times for start
        "start_count": len(relative_times_start),
        "max_relative_time_start": (
            max(relative_times_start) if relative_times_start else None
        ),
        "min_relative_time_start": (
            min(relative_times_start) if relative_times_start else None
        ),
        "avg_relative_time_start": (
            sum(relative_times_start) / len(relative_times_start)
            if relative_times_start
            else None
        ),
        # Relative times for finish
        "finish_relative_count": len(relative_times_finish),
        "max_relative_time_finish": (
            max(relative_times_finish) if relative_times_finish else None
        ),
        "min_relative_time_finish": (
            min(relative_times_finish) if relative_times_finish else None
        ),
        "avg_relative_time_finish": (
            sum(relative_times_finish) / len(relative_times_finish)
            if relative_times_finish
            else None
        ),
    }

    # Add checkpoint statistics (relative times)
    for checkpoint_name in MIDDLE_PHRASES.keys():
        checkpoint_relative_times = relative_times_checkpoints[checkpoint_name]
        stats[f"{checkpoint_name}_count"] = len(checkpoint_relative_times)
        stats[f"max_relative_time_{checkpoint_name}"] = (
            max(checkpoint_relative_times) if checkpoint_relative_times else None
        )
        stats[f"min_relative_time_{checkpoint_name}"] = (
            min(checkpoint_relative_times) if checkpoint_relative_times else None
        )
        stats[f"avg_relative_time_{checkpoint_name}"] = (
            sum(checkpoint_relative_times) / len(checkpoint_relative_times)
            if checkpoint_relative_times
            else None
        )

    return stats
