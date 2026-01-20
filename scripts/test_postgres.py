import time

from lsst.daf.butler import Butler
from lsst.sphgeom import ConvexPolygon, UnitVector3d


def canary(butler):
    region = ConvexPolygon(
        [
            UnitVector3d(0.28049890538712074, 0.06694409379825696, -0.9575170245912901),
            UnitVector3d(0.28000485777961376, 0.06898108445507997, -0.957517044029619),
            UnitVector3d(0.28192066372732705, 0.06944640127965154, -0.9569210713077902),
            UnitVector3d(0.28241501157153726, 0.06740817254613349, -0.9569210518705497),
        ]
    )

    butler.query_datasets(
        "the_monster_20250219",
        collections="refcats",
        where="htm7.region OVERLAPS search_region",
        bind={"search_region": region},
        # find_first=True,
        explain=False,
    )


def main():
    butler = Butler("embargo_readonly")
    for _ in range(10):
        start_time = time.time()

        canary(butler)

        end_time = time.time()
        duration = end_time - start_time

        print(f"Operation took {duration:.4f} seconds.")


if __name__ == "__main__":
    main()
