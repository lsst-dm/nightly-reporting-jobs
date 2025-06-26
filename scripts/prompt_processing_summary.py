import asyncio
import sys
import os
import lsst.daf.butler as dafButler
from dataclasses import dataclass
from datetime import date, timedelta
import requests

from queries import (
    get_next_visit_events,
    get_no_work_count_from_loki,
    get_df_from_loki,
)


def make_summary_message(day_obs, instrument):
    """Make Prompt Processing summary message for a night

    Parameters
    ----------
    day_obs : `str`
        day_obs in the format of YYYY-MM-DD.
    """

    output_lines = []

    day_obs_int = int(day_obs.replace("-", ""))

    butler_alias = "embargo"
    if instrument == "LATISS":
        survey = "BLOCK-306"
    elif instrument == "LSSTComCam":
        survey = "BLOCK-320"
    else:
        survey = "BLOCK-365"
    next_visits, canceled_visits = asyncio.run(
        get_next_visit_events(day_obs, instrument, survey)
    )
    total_visit_count = len(next_visits)
    canceled_list = next_visits.index.intersection(
        canceled_visits.set_index("groupId").index
    ).tolist()
    if canceled_list:
        next_visits = next_visits.drop(canceled_list)
    butler_nocollection = dafButler.Butler(butler_alias)
    raw_exposures = butler_nocollection.query_dimension_records(
        "exposure",
        instrument=instrument,
        where=f"day_obs={day_obs_int} AND (exposure.can_see_sky or exposure.can_see_sky=NULL) AND exposure.observation_type='science'",
        explain=False,
        limit=None,
    )

    # Do not send message if there are no on-sky exposures.
    if len(raw_exposures) == 0:
        sys.exit(0)

    output_lines.append("Number of on-sky exposures: {:d}".format(len(raw_exposures)))

    raw_exposures = butler_nocollection.query_dimension_records(
        "exposure",
        instrument=instrument,
        where=f"day_obs=day_obs_int AND exposure.science_program IN (survey)",
        bind={"day_obs_int": day_obs_int, "survey": survey},
        explain=False,
        limit=None,
    )
    groups = [r.group for r in raw_exposures]
    groups_without_events = set(groups) - set(next_visits.reset_index()["groupId"])

    raw_counts = count_datasets(
        butler_nocollection,
        "raw",
        f"{instrument}/raw/all",
        instrument=instrument,
        where=f"day_obs=day_obs_int AND exposure.science_program IN (survey) AND detector < 189",
        bind={"day_obs_int": day_obs_int, "survey": survey},
    )
    output_lines.append(
        f"Number for {survey}: {len(next_visits)}/{total_visit_count} nextVisit, "
        f"{len(raw_exposures):d} raws ({raw_counts} images)"
    )
    if groups_without_events:
        output_lines.append(
            f"{len(groups_without_events)} raws had no nextVisit: {', '.join(groups_without_events)}"
        )
    if len(raw_exposures) == 0:
        return "\n".join(output_lines)

    try:
        collections = butler_nocollection.collections.query(
            f"{instrument}/prompt/output-{day_obs:s}"
        )
        collection = list(collections)[0]
    except dafButler.MissingCollectionError:
        output_lines.append(f"No output collection was found for {day_obs:s}")
        return "\n".join(output_lines)

    isr_counts = count_datasets(
        butler_nocollection,
        "isr_log",
        f"{instrument}/prompt/output-{day_obs:s}/Isr/*",
        where=f"exposure.science_program IN (survey)",
        bind={"survey": survey},
    )
    sfm_counts = count_datasets(
        butler_nocollection,
        "isr_log",
        f"{instrument}/prompt/output-{day_obs:s}/SingleFrame*",
        where=f"exposure.science_program IN (survey)",
        bind={"survey": survey},
    )
    dia_counts = count_datasets(
        butler_nocollection,
        "isr_log",
        f"{instrument}/prompt/output-{day_obs:s}/ApPipe*",
        where=f"exposure.science_program IN (survey)",
        bind={"survey": survey},
    )

    b = dafButler.Butler(
        butler_alias, collections=[collection, f"{instrument}/defaults"]
    )

    log_visit_detector = set(
        [
            (x.dataId["exposure"], x.dataId["detector"])
            for x in b.query_datasets(
                "isr_log",
                where=f"exposure.science_program IN (survey)",
                bind={"survey": survey},
                limit=None,
                explain=False,
            )
        ]
    )
    missed = 0
    counted = 0
    # LSSTCam number of active detector is hard-coded here.
    if instrument == "LSSTCam":
        off_detector = 18
        df = get_df_from_loki(
            day_obs,
            instrument=instrument,
            match_string='|= "Preprocessing pipeline successfully run."',
            match_string2="",
        )
        output_lines.append(
            f"Number of expected preprocessing: {total_visit_count} nextVisits*(189-{off_detector} detectors)={total_visit_count * (189-off_detector)}. Successful: {len(df)}. "
        )
        expected = (len(raw_exposures) - len(groups_without_events)) * (
            189 - off_detector
        )
        missed = expected - len(log_visit_detector)

    df = get_df_from_loki(
        day_obs, instrument=instrument, match_string='|= "Timed out waiting for image"'
    )
    count_total = len(df)
    df = df[(df["instrument"] == instrument) & (df["group"].isin(groups))].set_index(
        ["group", "detector"]
    )
    if count_total > 0:
        counted += len(df)
        output_lines.append(
            f"- {len(df)} unexpected timeout ({count_total} total including raws not received)."
        )
    df = get_df_from_loki(
        day_obs,
        instrument=instrument,
        match_string='|= "MiddlewareInterface(_get_central_butler()"',
    )
    count_total = len(df)
    df = df[(df["instrument"] == instrument) & (df["group"].isin(groups))].set_index(
        ["group", "detector"]
    )
    if count_total > 0:
        counted += len(df)
        output_lines.append(
            f"- {len(df)} failure in instantiating MWI central butler connection ({count_total} total including raws not received)."
        )
    df = get_df_from_loki(
        day_obs, instrument=instrument, match_string='|= "prep_butler"'
    )
    count_total = len(df)
    df = df[(df["instrument"] == instrument) & (df["group"].isin(groups))].set_index(
        ["group", "detector"]
    )
    if count_total > 0:
        counted += len(df)
        output_lines.append(
            f"- {len(df)} failure in prep_butler ({count_total} total including raws not received)."
        )
        lines = _count_messages(
            df,
            [
                "botocore.exceptions.ClientError",
                "SSL SYSCALL error: EOF detected",
                "SSL connection has been closed unexpectedly",
                "server closed the connection unexpectedly",
            ],
        )
        if lines:
            output_lines.extend(lines)

    df = get_df_from_loki(
        day_obs,
        instrument=instrument,
        match_string='|= "loadDiaCatalogs" |= "cassandra"',
        match_string2='| json | level="ERROR"',
    )
    count_total = len(df)
    df = df[(df["instrument"] == instrument) & (df["group"].isin(groups))].set_index(
        ["group", "detector"]
    )
    if count_total > 0:
        output_lines.append(
            f"- {len(df)} loadDiaCatalogs errors from cassandra ({count_total} total including raws not received)."
        )
        lines = _count_messages(
            df,
            [
                "cassandra.cluster.NoHostAvailable",
                "Error from server",
            ],
        )
        if lines:
            output_lines.extend(lines)

    df = get_df_from_loki(
        day_obs,
        instrument=instrument,
        match_string='|= "Timed out connecting to raw microservice"',
        match_string2='| json | level="ERROR"',
    )
    df = df[(df["instrument"] == instrument) & (df["group"].isin(groups))].set_index(
        ["group", "detector"]
    )
    if len(df) > 0:
        output_lines.append(f"- {len(df)} Timed out connecting to raw microservice.")

    output_lines.append(
        f"Number of expected processing: ({len(raw_exposures)}-{len(groups_without_events)}) raws*(189-{off_detector} detectors)={expected:d}. Missed {missed}"
    )
    df = get_df_from_loki(
        day_obs,
        instrument=instrument,
        match_string='|= "RuntimeError: Unable to retrieve JSON sidecar"',
    )
    df = df[(df["instrument"] == instrument) & (df["group"].isin(groups))].set_index(
        ["group", "detector"]
    )
    if not df.empty:
        counted += len(df)
        output_lines.append(f"- {len(df)} failure in retrieving json sidecar.")

    df = get_df_from_loki(
        day_obs,
        instrument=instrument,
        match_string='|= "NoGoodPipelinesError: No main pipeline graph could be built"',
    )
    df = df[(df["instrument"] == instrument) & (df["group"].isin(groups))].set_index(
        ["group", "detector"]
    )
    if not df.empty:
        counted += len(df)
        output_lines.append(
            f"- {len(df)} NoGoodPipelinesError: {df.reset_index()['group'].unique().tolist()}"
        )

    if missed > 0:
        output_lines.append(f"- {missed - counted} unspecified")

    output_lines.append(
        "Number of main pipeline runs with outputs: {:d} total, {:d} Isr, {:d} SingleFrame, {:d} ApPipe".format(
            len(log_visit_detector), isr_counts, sfm_counts, dia_counts
        )
    )

    isr_outputs = count_datasets(
        b,
        "calibrateImage_log",  # this misses ISR-only
        collection,
        where=f"exposure.science_program IN (survey)",
        bind={"survey": survey},
    )
    output_lines.append(
        "- isr: {:d} attempts with outputs, {:d} passed not including ISR-only attempts.".format(
            isr_counts + sfm_counts + dia_counts, isr_outputs
        )
    )

    sfm_outputs = count_datasets(
        b,
        "analyzePreliminarySummaryStats_log",
        collection,
        where=f"exposure.science_program IN (survey)",
        bind={"survey": survey},
    )
    output_lines.append(
        "- calibrateImage: {:d} attempts with outputs, {:d} passed, {:d} failed.".format(
            sfm_counts + dia_counts, sfm_outputs, sfm_counts + dia_counts - sfm_outputs
        )
    )
    output_lines.extend(
        count_recurrent_pipeline_errors(
            b,
            f"visit.science_program='{survey}'AND instrument='{instrument}'",
            "calibrateImage",
        )
    )

    sfm_output_subset_visit_detector = set(
        [
            (x.dataId["visit"], x.dataId["detector"])
            for x in butler_nocollection.query_datasets(
                "analyzePreliminarySummaryStats_log",
                collections=f"{instrument}/prompt/output-{day_obs:s}/ApPipe*",
                where=f"exposure.science_program IN (survey)",
                bind={"survey": survey},
                find_first=False,
                explain=False,
                limit=None,
            )
        ]
    )

    dia_visit_detector = set(
        [
            (x.dataId["visit"], x.dataId["detector"])
            for x in b.query_datasets(
                "dia_source_apdb",
                where=f"exposure.science_program IN (survey)",
                bind={"survey": survey},
                explain=False,
                limit=None,
            )
        ]
    )
    count_no_work1, count_no_work2 = get_no_work_count_from_loki(
        day_obs, "associateApdb", visit_detector=sfm_output_subset_visit_detector
    )
    count_no_apdb = count_no_work1 + count_no_work2
    output_lines.append(
        "- associateApdb: {:d} attempts with outputs, {:d}+{:d}+{:d}={:d} passed, {:d} failed".format(
            dia_counts,
            len(dia_visit_detector),
            count_no_work1,
            count_no_work2,
            len(dia_visit_detector) + count_no_apdb,
            dia_counts - len(dia_visit_detector) - count_no_apdb,
        )
    )
    if sfm_output_subset_visit_detector:
        output_lines.append(
            f"  - {dia_counts - len(sfm_output_subset_visit_detector)} failed at single frame stage"
        )

    if dia_counts > 0 and (dia_counts - len(dia_visit_detector) - count_no_apdb) > 0:
        output_lines.extend(
            count_recurrent_pipeline_errors(
                b,
                f"visit.science_program='{survey}'AND instrument='{instrument}'",
                "subtractImages",
            )
        )
        output_lines.extend(
            count_recurrent_pipeline_errors(
                b,
                f"visit.science_program='{survey}'AND instrument='{instrument}'",
                "associateApdb",
            )
        )

    output_lines.append(
        f"<https://usdf-rsp.slac.stanford.edu/times-square/github/lsst-dm/vv-team-notebooks/PREOPS-prompt-error-msgs?day_obs={day_obs}&instrument={instrument}&ts_hide_code=1&survey={survey}|Full Error Log>"
    )

    output_lines.append(
        f"<https://usdf-rsp.slac.stanford.edu/times-square/github/lsst-sqre/times-square-usdf/prompt-processing/groups?date={day_obs}&instrument={instrument}&survey={survey}&mode=DEBUG&ts_hide_code=1|Timing plots>"
    )

    df = get_df_from_loki(
        day_obs,
        instrument=instrument,
        match_string='|= "export_outputs"',
        match_string2='|= "Central repo export failed"',
    )
    df = df[(df["instrument"] == instrument) & (df["group"].isin(groups))].set_index(
        ["group", "detector"]
    )
    if not df.empty:
        output_lines.append(f"- {len(df)} failure in export_outputs.")
        output_lines.append(f"  (Partial export may be incorrectly counted as success)")

        lines = _count_messages(
            df,
            [
                "botocore.exceptions.ClientError",
                "SSL SYSCALL error: EOF detected",
                "SSL connection has been closed unexpectedly",
                "server closed the connection unexpectedly",
                "psycopg2.errors.UniqueViolation",
            ],
        )
        if lines:
            output_lines.extend(lines)

    df = get_df_from_loki(
        day_obs,
        instrument=instrument,
        match_string='|= "Signal SIGTERM detected, cleaning up and shutting down."',
        match_string2="",
    )
    count_total = len(df)
    df = df[(df["instrument"] == instrument) & (df["group"].isin(groups))].set_index(
        ["group", "detector"]
    )
    if count_total > 0:
        output_lines.append(
            f"- At least {len(df)} had SIGTERM ({count_total} total including raws not received)."
        )

    return "\n".join(output_lines)


def count_datasets(butler, dataset_type, collection, **kwargs):
    try:
        refs = butler.query_datasets(
            dataset_type,
            collections=collection,
            find_first=False,
            explain=False,
            limit=None,
            **kwargs,
        )
    except dafButler.MissingCollectionError:
        return 0
    return len(refs)


RECURRENT_ERRORS_BY_TASK = {
    "calibrateImage": [
        "Exception AllCentroidsFlaggedError",
        "Exception BadAstrometryFit: Poor quality astrometric fit",
        "Exception IndexError: arrays used as indices must be of integer (or boolean) type",
        "Exception MatcherError",
        "Exception MatcherFailure: No matches found",
        "Exception MatcherFailure: No matched reference stars with valid fluxes",
        "Exception MatcherFailure: No matches to use for photocal",
        "Exception MatcherFailure: Not enough catalog objects",
        "Exception MatcherFailure: Not enough refcat objects",
        "Exception MeasureApCorrError: Unable to measure aperture correction",
        "Exception NonfinitePsfShapeError: Failed to determine PSF",
        "Exception NoPsfStarsToStarsMatchError",
        "Exception NormalizedCalibrationFluxError",
        "Exception ObjectSizeNoGoodSourcesError",
        "Exception ObjectSizeNoSourcesError",
        "Exception PsfexNoGoodStarsError",
        "Exception PsfexTooFewGoodStarsError",
        "Exception TooManyCosmicRays",
        "Exception TooManyMaskedPixelsError",
        "No valid points to fit. Variance is likely zero",
    ],
    "subtractImages": [
        "RuntimeError: Cannot compute PSF matching kernel: too few sources selected",
        "RuntimeError: No good PSF candidates to pass to PSFEx",
        "RuntimeError: No objects passed our cuts for consideration as psf stars",
        "Unable to determine kernel sum; 0 candidates",
        "Could not compute LinearTransform inverse",
    ],
    "associateApdb": [
        "OperationTimedOut",  # cassandra.OperationTimedOut
        "Control connection failed to connect",  # cassandra.cluster.NoHostAvailable
        "Error from server",
    ],
}


def count_recurrent_pipeline_errors(butler, where, task):
    # with open("error_config.yaml") as f:
    #    RECURRENT_ERRORS_BY_TASK = yaml.safe_load(f)
    recurrent_errors = RECURRENT_ERRORS_BY_TASK.get(task, [])
    refs = butler.query_datasets(
        f"{task}_log",
        where=where,
        explain=False,
        limit=None,
    )
    visit_errors = []
    for ref in refs:
        log_messages = butler.get(ref)
        errors = [msg for msg in log_messages if msg.levelno > 30]
        visit_errors.extend(errors)
    lines = []
    total_count = 0
    for err in recurrent_errors:
        count = _count_error(err, visit_errors)
        if count:
            lines.append(f"    - {count} {err}")
            total_count += count
    if lines:
        lines.insert(0, f"    Among {task} errors, {total_count} were")
    return lines


def _count_error(errMsg, visit_errors):
    return len([_.message for _ in visit_errors if errMsg in _.message])


def _count_messages(df, messages):
    lines = []
    for msg in messages:
        sub_df = df[df["message"].str.contains(msg, na=False)]
        if not sub_df.empty:
            lines.append(f"  - {len(sub_df)}: {msg}.")
    return lines


if __name__ == "__main__":
    instrument = os.getenv("INSTRUMENT")
    if not instrument:
        instrument = "LSSTCam"
    webhook = "SLACK_WEBHOOK_URL_" + instrument.upper()
    url = os.getenv(webhook)

    day_obs = date.today() - timedelta(days=1)
    day_obs_string = day_obs.strftime("%Y-%m-%d")
    summary = make_summary_message(day_obs_string, instrument)
    output_message = (
        f":clamps: *{instrument} {day_obs.strftime('%A %Y-%m-%d')}* :clamps: \n"
        + summary
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
