__all__ = ["count_detectors_with_templates"]

from dataclasses import dataclass, field
import enum

import astropy.coordinates
import astropy.units as u
import lsst.geom
import lsst.sphgeom
import lsst.afw.cameraGeom
import lsst.obs.base

from lsst.daf.butler import Butler


@dataclass(frozen=True, kw_only=True)
class SimpleVisit:
    class CoordSys(enum.IntEnum):
        ICRS = 2

    class RotSys(enum.IntEnum):
        SKY = 2

    instrument: str
    filters: str
    coordinateSystem: CoordSys
    position: list[float] = field(compare=False)
    rotationSystem: RotSys
    cameraAngle: float
    detector: int

    def get_boresight_icrs(self):
        """Get ICRS coordinates of the boresight."""
        return astropy.coordinates.SkyCoord(*self.position, unit=u.degree, frame="icrs")

    def get_rotation_sky(self):
        """Get sky rotation angle."""
        return astropy.coordinates.Angle(self.cameraAngle, unit=u.degree)

    def predict_wcs(self, camera: lsst.afw.cameraGeom.Camera):
        """Calculate the expected detector WCS for this visit."""
        icrs = self.get_boresight_icrs()
        icrs = lsst.geom.SpherePoint(icrs.ra.degree, icrs.dec.degree, lsst.geom.degrees)

        rotation = self.get_rotation_sky()
        rotation = rotation.degree * lsst.geom.degrees

        detector = camera[self.detector]
        return lsst.obs.base.utils.createInitialSkyWcsFromBoresight(
            icrs, rotation, detector
        )

    def get_detector_icrs_region(self, camera: lsst.afw.cameraGeom.Camera):
        """Return the detector region in ICRS coordinates."""
        wcs = self.predict_wcs(camera)
        detector = camera[self.detector]
        corners = wcs.pixelToSky(detector.getCorners(lsst.afw.cameraGeom.PIXELS))
        return lsst.sphgeom.ConvexPolygon.convexHull([c.getVector() for c in corners])


def overlap_templates(butler, visit_id, detector_id, exp_record=None):
    """Check if templates overlap with the detector footprint for a given visit.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler instance to query.
    visit_id : `int`
        Visit ID to check.
    detector_id : `int`
        Detector ID to check.
    exp_record : exposure record, optional
        Pre-fetched exposure record. If None, will query it.

    Returns
    -------
    has_overlap : `bool`
        True if overlapping templates are found, False otherwise.
    """
    # Get exposure record if not provided
    if exp_record is None:
        try:
            exp_record = butler.query_dimension_records(
                "exposure", instrument="LSSTCam", visit=visit_id
            )[0]
        except Exception:
            return False

    visit = SimpleVisit(
        instrument=exp_record.instrument,
        detector=detector_id,
        filters=exp_record.physical_filter,
        coordinateSystem=SimpleVisit.CoordSys.ICRS,
        position=[exp_record.tracking_ra, exp_record.tracking_dec],
        rotationSystem=SimpleVisit.RotSys.SKY,
        cameraAngle=exp_record.sky_angle,
    )

    camera = butler.get("camera", instrument="LSSTCam", collections="LSSTCam/calib")
    region = visit.get_detector_icrs_region(camera)

    try:
        templates = butler.query_datasets(
            "template_coadd",
            collections="LSSTCam/templates",
            data_id={
                "instrument": visit.instrument,
                "skymap": "lsst_cells_v2",
                "physical_filter": visit.filters,
            },
            where="patch.region OVERLAPS search_region",
            bind={"search_region": region},
            find_first=True,
            explain=False,
            limit=1,
        )
        # Check if we got any results
        return len(templates) > 0
    except Exception:
        return False


def count_detectors_with_templates(butler, visit_id):
    """Count how many detectors have overlapping templates for a given visit.

    Parameters
    ----------
    butler : `lsst.daf.butler.Butler`
        Butler instance to query.
    visit_id : `int`
        Visit ID to check.

    Returns
    -------
    count : `int`
        Number of detectors (0-188, excluding off detectors) that have overlapping templates.
    """
    off_detectors = (
        120,
        122,
        0,
        20,
        27,
        65,
        123,
        161,
        168,
        188,
        1,
        19,
        30,
        68,
        158,
        169,
        187,
    )

    # Query exposure record once for all detectors
    try:
        exp_record = butler.query_dimension_records(
            "exposure", instrument="LSSTCam", visit=visit_id
        )[0]
    except Exception:
        return 0

    count = 0
    for detector_id in range(189):
        if detector_id in off_detectors:
            continue
        if overlap_templates(butler, visit_id, detector_id, exp_record=exp_record):
            count += 1
    return count
