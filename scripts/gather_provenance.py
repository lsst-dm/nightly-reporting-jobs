# This file is part of nightly-reporting-jobs.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

__all__ = [
    "generate_task_report",
]
import logging

from astropy.table import Table
import numpy as np

from lsst.pipe.base.quantum_graph import ProvenanceQuantumGraph

logging.basicConfig(
    format="{levelname} {asctime} {name} - {message}",
    style="{",
)
_log = logging.getLogger(__name__)
_log.setLevel(logging.DEBUG)


def parse_provenance(pqg):
    """
    Create a combined table based on the quantum tasks in the specified ProvenanceQuantumGraph instance.

    Parameters
    ----------
    pqg : `lsst.pipe.base.quantum_graph.ProvenanceQuantumGraph`
        An instance that contains methods to build quantum and task resource usage tables.

    Returns
    -------
    combined_table: `astropy.table.Table`
        A combined Astropy table containing task resource usage and data IDs.

    Raises
    ------
    ValueError
        If the expected conditions regarding rows are not met.
    """

    combined_rows = []

    qt = pqg.make_quantum_table()
    for task in pqg.quanta_by_task:
        t = pqg.make_task_resource_usage_table(task)
        # See pipe_base QuantumResourceUsage doc for the columns
        t.remove_columns(
            ["quantum_id", "prep_time", "init_time", "run_time_cpu", "start"]
        )
        if len(t) != 1:
            raise ValueError(
                f"Expected one row in the table for task '{task}', but got {len(t)} rows."
            )

        quanta = pqg.quanta_by_task[task]
        if len(quanta) != 1:  # This should be true in PP
            raise ValueError(
                f"Expected one row in the table for task '{task}', but got {len(quanta)} rows."
            )
        data_id = next(iter(quanta))
        detector_id = data_id.get("detector")
        group_id = data_id.get("group")
        visit_id = data_id.get("visit") if task != "isr" else data_id.get("exposure")

        row = qt[qt["Task"] == task]
        if len(row) != 1:
            raise ValueError(
                f"Expected a row for Task {task} in quantum table, but got {len(row)} rows."
            )

        # Since t or row has only one row, access that row directly
        row_dict = {  # **t[0], # TBD: load all columns or selective?
            "task": task,
            "visit_id": visit_id,
            "detector_id": detector_id,
            "successful": row["Successful"][0],
            "run_time": t["run_time"][0],  # in seconds
            "memory": t["memory"][0] / 1e9,  # in GB
        }
        combined_rows.append(row_dict)

    if combined_rows:
        combined_table = Table(rows=combined_rows)
        return combined_table
    else:
        raise ValueError(f"Missing values for {pqg.header.metadata['data_query']}")


def get_provenance_table(butler, refs):
    """Read provenance of all refs and return a combined provenance table"""
    column_names = [
        "task",
        "visit_id",
        "detector_id",
        "successful",
        "run_time",
        "memory",
    ]
    column_types = ["S20", "u8", "u1", "u1", "f8", "f8"]
    table = Table(names=column_names, dtype=column_types)

    for ref in refs:
        qg = butler.getURI(ref)
        with ProvenanceQuantumGraph.from_args(qg, datasets=()) as (graph, _):
            for row in parse_provenance(graph):
                table.add_row(row)
    return table


def generate_task_report(butler, collection):
    """

    Parameters
    ----------
    collection : string
        A RUN collection to extract provenance from and summarize.

    Returns
    -------
    statistics_table : `astropy.table.Table`
        A combined Astropy table containing resource usage for each task.
    """

    refs = butler.query_datasets(
        "prompt_provenance",
        collections=collection,
        limit=None,
        find_first=False,
    )
    provenance_table = get_provenance_table(butler, refs)

    column_names = [
        "task",
        "count",
        "med_runtime",
        "max_runtime",
        "med_memory",
        "max_memory",
    ]
    column_types = ["S20", "u4", "f8", "f8", "f8", "f8"]
    statistics_table = Table(names=column_names, dtype=column_types)

    graph = butler.get(refs[0])
    for task_name in graph.quanta_by_task:
        task_rows = provenance_table[provenance_table["task"] == task_name]
        values_runtime = task_rows["run_time"]
        values_memory = task_rows["memory"]

        count = len(values_runtime)
        if count > 0:
            med_runtime = round(np.median(values_runtime), 1)
            max_runtime = round(values_runtime.max(), 1)
            med_memory = round(np.median(values_memory), 1)
            max_memory = round(values_memory.max(), 1)
            statistics_table.add_row(
                [task_name, count, med_runtime, max_runtime, med_memory, max_memory]
            )
        else:
            statistics_table.add_row(
                [
                    task_name,
                    count,
                    float("nan"),
                    float("nan"),
                    float("nan"),
                    float("nan"),
                ]
            )
    return statistics_table
