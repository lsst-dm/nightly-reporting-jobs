#!/usr/bin/env python3

import argparse
from datetime import datetime
import logging
from multiprocessing import Pool
from functools import partial

from astropy.time import Time, TimeDelta

from lsst.daf.butler import Butler, CollectionType, MissingCollectionError
from lsst.pipe.base import Pipeline
from lsst.pipe.base.all_dimensions_quantum_graph_builder import (
    AllDimensionsQuantumGraphBuilder,
)
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


def run_pipetask(visit_id, repo, output_run):
    """
    Run pipetask for a given VISIT_ID.

    Parameters
    ----------
    visit_id : `int`
        Visit ID (e.g., 2026030100037)
    repo : `str`
        Repository name
    """
    day_obs = str(visit_id)[:8]
    day_obs_str = str(day_obs)

    _log.debug(f"Checking REPO={repo}, VISIT_ID={visit_id}, day_obs={day_obs}")
    pipeline_yaml = _get_pipeline_yaml()

    input_collections = ["LSSTCam/calib", f"LSSTCam/runs/prompt-{day_obs}"]
    butler = Butler(repo, writeable=True, collections=input_collections)
    where = f"instrument='LSSTCam' and exposure={visit_id}"
    # Only process if all detectors have data.
    with butler.query() as query:
        count = query.datasets("preliminary_visit_image").where(where).count()
    if count < 172:
        _log.info(f"visit={visit_id} has only {count} pvi; skipping")
        return None

    _log.debug(f"Running task visit={visit_id}")
    pipeline = Pipeline.fromFile(pipeline_yaml)
    pipeline_graph = pipeline.to_graph()

    quantum_graph_builder = AllDimensionsQuantumGraphBuilder(
        pipeline_graph, butler, where=where, bind=None, output_run=output_run
    )
    predicted = quantum_graph_builder.finish(
        output=None,
        metadata={"skip_existing_in": [], "skip_existing": False, "data_query": where},
        attach_datastore_records=False,
    ).assemble()

    nodes_map = predicted.quantum_only_xgraph.nodes
    quantum_ids = list(predicted)
    if len(quantum_ids) == 0:
        _log.info(f"No work to do for visit_id={visit_id}.")
        return None
    assert len(quantum_ids) <= 1, f"More than one quantum in the graph in {visit_id}!"
    quantum_id = quantum_ids[0]
    node = nodes_map[quantum_id]

    pipeline_node = node["pipeline_node"]
    quantums = predicted.build_execution_quanta(quantum_ids)
    quantum = quantums[quantum_id]

    task_factory = TaskFactory()
    executor = SingleQuantumExecutor(butler=butler, task_factory=task_factory)
    result = executor.execute(pipeline_node, quantum, quantum_id)
    _log.info(
        f"Wrote result {[_ for _ in result.quantum.outputs.get('visit_geometry')]}"
    )
    _log.info(f"Successfully completed visit_id={visit_id}")


def run_parallel(butler_repo, exp_ids, n_processes=4):
    """Run run_pipetask in parallel for multiple exposures.

    Parameters
    ----------
    butler_repo : `str`
        Path to the butler repository.
    exp_ids : `list` of `int`
        List of exposure IDs to process.
    n_processes : `int`, optional
        Number of parallel processes to use. Default is 4.

    Returns
    -------
    results : `list`
        List of results from run_pipetask for each exposure.
    """
    day_obs = str(exp_ids[0])[:8]
    output_collection = f"u/hchiang2/visit_geom/{day_obs}"
    output_run = output_collection + "/" + datetime.now().strftime("%Y%m%d%H%M%S%f")
    _log.info(f"Registering output_run: {output_run}")
    butler = Butler(butler_repo, writeable=True)
    butler.collections.register(output_run, CollectionType.RUN)
    butler.collections.prepend_chain(output_collection, output_run)

    # It appears that SingleQuantumExecutor can run without storing inits
    # TODO: probably should still store them for task provenance?
    # run_init(butler_repo, output_run)

    with Pool(processes=n_processes) as pool:
        results = pool.map(
            partial(run_pipetask, repo=butler_repo, output_run=output_run),
            exp_ids,
        )
    return results


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

    butler = Butler("embargo")
    day_obs = get_day_obs(t_start)
    chain = f"u/hchiang2/visit_geom/{day_obs}"
    try:
        results = butler.collections.query(chain)
    except MissingCollectionError:
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
        run_parallel("embargo", exp_ids, n_processes=12)
