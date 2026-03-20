"""Check for detector data outputs in butler."""

"""estimate_outputs.py - Check for detector data outputs in butler."""

__all__ = ["count_detectors_with_outputs"]

from lsst.daf.butler import Butler
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
    except (IndexError, DataIdError):
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
    except (DataIdError, MissingDatasetTypeError, LookupError):
        return 0

    return count
