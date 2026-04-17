#!/usr/bin/env python3

import argparse
from datetime import datetime
import logging

from astropy.time import Time, TimeDelta

from lsst.daf.butler import Butler, CollectionType
from lsst.pipe.base import Pipeline
from lsst.pipe.base.all_dimensions_quantum_graph_builder import (
    AllDimensionsQuantumGraphBuilder,
)
from lsst.pipe.base.mp_graph_executor import MPGraphExecutor, MPGraphExecutorError
from lsst.pipe.base.single_quantum_executor import SingleQuantumExecutor
from lsst.pipe.base.taskFactory import TaskFactory
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


def run_init(repo, output_run):
    pipeline = Pipeline.fromFile(_get_pipeline_yaml())
    # pipeline.addConfigOverride("parameters", "apdb_config", apdb)
    output_butler = Butler(repo, writeable=True, run=output_run)
    graph = pipeline.to_graph(output_butler.registry)
    graph.check_dataset_type_registrations(output_butler, include_packages=True)
    graph.init_output_run(output_butler)


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
    report : `Report`
        Aggregated execution report for all quanta.
    """
    day_obs = str(exp_ids[0])[:8]
    input_collections = ["LSSTCam/calib", f"LSSTCam/runs/prompt-{day_obs}"]
    butler = Butler(butler_repo, writeable=True, collections=input_collections)

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
    pipeline_graph = pipeline.to_graph()

    quantum_graph_builder = AllDimensionsQuantumGraphBuilder(
        pipeline_graph, butler, where=where, bind=None, output_run=output_run
    )
    predicted = quantum_graph_builder.finish(
        output=None,
        metadata={"skip_existing_in": [], "skip_existing": False, "data_query": where},
        attach_datastore_records=False,
    ).assemble()

    if len(list(predicted)) == 0:
        _log.info("No work to do for any visit.")
        return None

    _log.info(f"Executing {len(list(predicted))} quanta with {n_processes} processes")

    # It appears that SingleQuantumExecutor can run without storing inits
    # TODO: probably should still store them for task provenance?
    # run_init(butler_repo, output_run)

    task_factory = TaskFactory()
    quantum_executor = SingleQuantumExecutor(butler=butler, task_factory=task_factory)
    graph_executor = MPGraphExecutor(
        num_proc=n_processes,
        timeout=3600,
        quantum_executor=quantum_executor,
    )

    try:
        graph_executor.execute(predicted)
    except MPGraphExecutorError as exc:
        _log.error(f"One or more quanta failed: {exc}")

    report = graph_executor.getReport()
    _log.info(f"Execution report: status={report.status}")
    return report


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

    exp_ids = query_exposures_without_visit_geometry(
        butler,
        "BLOCK-407",
        day_obs=day_obs,
        time_start_tai=t_start.isot,
        time_end_tai=t_end.isot,
        collections=chain,
    )
    _log.debug(f"Found {len(exp_ids)} exposures to process")

    if exp_ids:
        output_collection = f"u/hchiang2/visit_geom/{day_obs}"
        output_run = output_collection + "/" + datetime.now().strftime("%Y%m%d%H%M%S%f")
        _log.info(f"Registering output_run: {output_run}")
        butler.collections.register(output_run, CollectionType.RUN)
        butler.collections.prepend_chain(output_collection, output_run)

        run_all_visits("embargo", exp_ids, output_run, n_processes=18)
