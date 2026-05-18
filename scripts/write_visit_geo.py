#!/usr/bin/env python3

import argparse
from datetime import datetime
import logging

from astropy.time import Time, TimeDelta

from lsst.daf.butler import Butler, CollectionType
from lsst.pipe.base import Pipeline
from lsst.pipe.base.mp_graph_executor import MPGraphExecutorError
from lsst.pipe.base.separable_pipeline_executor import SeparablePipelineExecutor
from lsst.resources import ResourcePath

from queries import get_day_obs, query_exposures_without_visit_geometry

logging.basicConfig(
    format="{levelname} {asctime} {name} - {message}",
    style="{",
)
_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)


def _get_pipeline_yaml():
    ap_pipe_dir = ResourcePath("eups://ap_pipe/pipelines/", forceDirectory=True)
    pipeline_yaml = f"{ap_pipe_dir}/LSSTCam/ApPipe.yaml#consolidateVisitSummary"
    return pipeline_yaml


def run_all_visits(butler_repo, exp_ids, output_run, n_processes=4):
    """Build a single quantum graph for all exposures and execute in parallel.

    Parameters
    ----------
    butler_repo : `str`
        URI or alias to the butler repository.
    exp_ids : `list` of `int`
        List of exposure IDs to process.
    output_run : `str`
        Output run collection name.
    n_processes : `int`, optional
        Number of parallel processes to use. Default is 4.

    Returns
    -------
    None
    """
    # Assumes all exp_ids are from the same night; may break if window spans midnight.
    day_obs = str(exp_ids[0])[:8]
    input_collections = ["LSSTCam/calib", f"LSSTCam/runs/prompt-{day_obs}"]
    butler = Butler(butler_repo, writeable=True, collections=input_collections, run=output_run)
    executor = SeparablePipelineExecutor(butler=butler)

    # Filter to visits that have all detectors present.
    valid_exp_ids = []
    for visit_id in exp_ids:
        where = f"instrument='LSSTCam' and exposure={visit_id}"
        with butler.query() as query:
            count = query.datasets("preliminary_visit_image").where(where).count()
        if count < 172:
            _log.info(f"visit={visit_id} has only {count} pvi; skipping")
        else:
            valid_exp_ids.append(visit_id)

    if not valid_exp_ids:
        _log.info("No valid visits to process.")
        return None

    # Build one quantum graph spanning all valid visits.
    exposure_list = ", ".join(str(v) for v in valid_exp_ids)
    where = f"instrument='LSSTCam' and exposure IN ({exposure_list})"
    _log.debug(f"Building graph for {len(valid_exp_ids)} visits: {where}")

    pipeline = Pipeline.fromFile(_get_pipeline_yaml())
    predicted = executor.build_quantum_graph(pipeline, where=where)

    if len(list(predicted)) == 0:
        _log.info("No work to do for any visit.")
        return None

    _log.info(f"Executing {len(list(predicted))} quanta with {n_processes} processes")

    # TODO: write config and init outputs only once a day
    #executor.pre_execute_qgraph(predicted)
    try:
        executor.run_pipeline(predicted, num_proc=n_processes)
    except MPGraphExecutorError as exc:
        _log.error("One or more quanta failed: %s", exc)


BLOCKS = [
    "BLOCK-365",
    "BLOCK-407",
    "BLOCK-408",
    "BLOCK-416",
    "BLOCK-417",
    "BLOCK-419",
    "BLOCK-421",
    "BLOCK-T698",
    "BLOCK-T703",
    "BLOCK-T704",
    "BLOCK-T706",
]

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "n_hours", nargs="?", default=1, type=float, help="Number of hours (default: 1)"
    )
    args = parser.parse_args()

    now_tai = Time.now().tai
    # Only process visits up to 1 hour ago
    t_end = now_tai + TimeDelta(-60 * 60, format="sec")
    t_start = t_end + TimeDelta(-args.n_hours * 60 * 60, format="sec")

    butler = Butler("embargo", writeable=True)
    day_obs = get_day_obs(t_start)
    chain = f"u/hchiang2/visit_geom/{day_obs}"
    butler.collections.register(chain, CollectionType.CHAINED)

    all_exp_ids = []
    for block in BLOCKS:
        exp_ids = query_exposures_without_visit_geometry(
            butler,
            block,
            day_obs=day_obs,
            time_start_tai=t_start.isot,
            time_end_tai=t_end.isot,
            collections=chain,
        )
        _log.debug(f"Block {block}: found {len(exp_ids)} exposures to process")
        all_exp_ids.extend(exp_ids)

    if all_exp_ids:
        output_collection = f"u/hchiang2/visit_geom/{day_obs}"
        output_run = output_collection + "/" + datetime.now().strftime("%Y%m%d%H%M%S%f")
        _log.info(f"Registering output_run: {output_run}")
        butler.collections.register(output_run, CollectionType.RUN)
        butler.collections.prepend_chain(output_collection, output_run)

        run_all_visits("embargo", all_exp_ids, output_run, n_processes=18)
