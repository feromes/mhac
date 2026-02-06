#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import argparse

import geopandas as gpd
from shapely.geometry import box
import laspy


# ---------------------------------------------------------------------
# Configuração (coerente com o script 01)
# ---------------------------------------------------------------------

LIDAR_BASE_DIR = Path("/Users/fernandogomes/dev/ogdc")

LIDAR_RAW_SUBDIRS = {
    2024: "LiDAR-Sampa-2024",
}

OUTPUT_ARTICULACAO = LIDAR_BASE_DIR / "articulacao_2024_from_laz.gpkg"

FILENAME_SUFFIX = ".laz"


# ---------------------------------------------------------------------
# Utilidades
# ---------------------------------------------------------------------

def raw_dir_for_year(year: int) -> Path:
    path = LIDAR_BASE_DIR / LIDAR_RAW_SUBDIRS[year]
    if not path.exists():
        raise FileNotFoundError(f"Diretório LiDAR não encontrado: {path}")
    return path


def iter_laz_files(year: int):
    return sorted(raw_dir_for_year(year).glob(f"*{FILENAME_SUFFIX}"))


def read_laz_bounds(laz_path: Path):
    """
    Lê apenas o header do LAZ e retorna bounds + CRS (se disponível)
    """
    with laspy.open(laz_path) as f:
        header = f.header

        minx, miny, _ = header.mins
        maxx, maxy, _ = header.maxs

        crs = header.parse_crs()
        return minx, miny, maxx, maxy, crs


# ---------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(
        description="Gera articulação 2024 a partir dos bounds dos arquivos LAZ"
    )
    parser.add_argument(
        "--year",
        type=int,
        default=2024,
        help="Ano do levantamento (default: 2024)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=OUTPUT_ARTICULACAO,
        help="Arquivo GPKG de saída",
    )

    args = parser.parse_args()

    year = args.year
    output = args.output

    laz_files = list(iter_laz_files(year))

    if not laz_files:
        raise RuntimeError("Nenhum arquivo LAZ encontrado.")

    print(f"📂 Encontrados {len(laz_files)} arquivos LAZ ({year})")

    records = []
    crs_final = None

    for laz in laz_files:
        tile_id = laz.stem  # nome do arquivo sem .laz

        minx, miny, maxx, maxy, crs = read_laz_bounds(laz)

        geom = box(minx, miny, maxx, maxy)

        records.append(
            {
                "nome_arquivo": tile_id,
                "geometry": geom,
            }
        )

        if crs_final is None and crs is not None:
            crs_final = crs

    gdf = gpd.GeoDataFrame(records, geometry="geometry", crs=crs_final)

    print("🧭 Resumo da articulação gerada:")
    print(f"  • Total de tiles: {len(gdf)}")
    print(f"  • CRS: {gdf.crs}")

    output.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_file(output, driver="GPKG")

    print(f"💾 Articulação salva em: {output}")


if __name__ == "__main__":
    main()
