#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import argparse

import geopandas as gpd


# ---------------------------------------------------------------------
# Configuração (espelhando o script 01)
# ---------------------------------------------------------------------

LIDAR_BASE_DIR = Path("/Users/fernandogomes/dev/ogdc")

ARTICULACAO_INDEX = {
    2017: LIDAR_BASE_DIR / "articulacao_2017.zip",
    2020: LIDAR_BASE_DIR / "articulacao_2020.zip",
    2024: LIDAR_BASE_DIR / "articulacao_2024.gpkg",
}

TILE_ID_FIELD = {
    2017: "cd_quadric",
    2020: "cd_quadric",
    2024: "nome_arquivo",
}

OUTPUT_BASE = Path("data/processed")


# ---------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------

def load_articulacao(year: int) -> gpd.GeoDataFrame:
    path = ARTICULACAO_INDEX[year]
    if not path.exists():
        raise FileNotFoundError(f"Índice não encontrado: {path}")
    return gpd.read_file(path)


def tile_already_processed(year: int, tile_id: str) -> bool:
    base = OUTPUT_BASE / str(year)
    mds = base / "tiles_MDS" / f"mds_tile_{tile_id}.tif"
    hag = base / "tiles_HAG" / f"hag_tile_{tile_id}.tif"
    return mds.exists() and hag.exists()


# ---------------------------------------------------------------------
# Geração dos jobs
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Generate GNU parallel job list for MHAC tiles"
    )
    parser.add_argument(
        "--years",
        nargs="+",
        type=int,
        default=[2017, 2020, 2024],
        help="Anos a processar (default: 2017 2020 2024)",
    )
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("jobs.txt"),
        help="Arquivo de saída (default: jobs.txt)",
    )

    args = parser.parse_args()

    jobs = []

    for year in args.years:
        print(f"[scan] {year}")
        gdf = load_articulacao(year)
        id_field = TILE_ID_FIELD[year]

        for _, row in gdf.iterrows():
            tile_id = str(row[id_field]).strip()

            if tile_already_processed(year, tile_id):
                continue

            cmd = (
                f"python scripts/lidar/01_build_mhac_tiles.py "
                f"--year {year} "
                f"--tile-id {tile_id}"
            )
            jobs.append(cmd)

    args.out.write_text("\n".join(jobs) + "\n", encoding="utf-8")

    print("--------------------------------------------------")
    print(f"Jobs gerados: {len(jobs)}")
    print(f"Arquivo: {args.out.resolve()}")
    print("--------------------------------------------------")


if __name__ == "__main__":
    main()
