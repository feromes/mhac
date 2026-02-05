#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import json
import subprocess
import math

import geopandas as gpd


# ---------------------------------------------------------------------
# Configuração fixa
# ---------------------------------------------------------------------

LIDAR_BASE_DIR = Path("/Users/fernandogomes/dev/ogdc")

LIDAR_RAW_SUBDIRS = {
    2017: "LiDAR-Sampa-2017",
    2020: "LiDAR-Sampa-2020",
    2024: "LiDAR-Sampa-2024",
}

LIDAR_FILENAME_RULES = {
    2017: dict(base_field="cd_quadric", prefix="MDS_color_", suffix=".laz"),
    2020: dict(base_field="cd_quadric", prefix="MDS_", suffix="_1000.laz"),
    2024: dict(base_field="nome_arquivo", prefix="", suffix=".laz"),
}

ARTICULACAO_INDEX = {
    2017: LIDAR_BASE_DIR / "articulacao_2017.zip",
    2020: LIDAR_BASE_DIR / "articulacao_2020.zip",
    2024: LIDAR_BASE_DIR / "articulacao_2024.gpkg",
}

OUTPUT_BASE = Path("data/processed")

RESOLUTION = 1.0
NODATA = -9999


# ---------------------------------------------------------------------
# Utilidades espaciais
# ---------------------------------------------------------------------

def snap_origin(value: float, resolution: float) -> float:
    """Ajusta o valor para o múltiplo inferior da resolução."""
    return math.floor(value / resolution) * resolution


# ---------------------------------------------------------------------
# Utilidades LiDAR
# ---------------------------------------------------------------------

def tile_id_field_for_year(year: int) -> str:
    """Retorna o campo identificador da quadrícula para o ano."""
    return LIDAR_FILENAME_RULES[year]["base_field"]


def raw_dir_for_year(year: int) -> Path:
    sub = LIDAR_RAW_SUBDIRS[year]
    path = LIDAR_BASE_DIR / sub
    if not path.exists():
        raise FileNotFoundError(f"Diretório LiDAR não encontrado: {path}")
    return path


def load_articulacao(year: int) -> gpd.GeoDataFrame:
    path = ARTICULACAO_INDEX[year]
    if not path.exists():
        raise FileNotFoundError(f"Índice de articulação não encontrado: {path}")
    return gpd.read_file(path)


def laz_files_for_tile(row, year: int) -> list[Path]:
    rule = LIDAR_FILENAME_RULES[year]
    base_value = str(row[rule["base_field"]]).strip()
    laz_name = f'{rule["prefix"]}{base_value}{rule["suffix"]}'
    laz_path = raw_dir_for_year(year) / laz_name

    if not laz_path.exists():
        raise FileNotFoundError(f"Arquivo LAZ não encontrado: {laz_path}")

    return [laz_path]


# ---------------------------------------------------------------------
# PDAL
# ---------------------------------------------------------------------

def build_pdal_pipeline(
    laz_files: list[Path],
    origin_x: float,
    origin_y: float,
    width: int,
    height: int,
    out_mds: Path,
    out_hag: Path,
):
    pipeline = {"pipeline": []}

    # 1. Readers
    for laz in laz_files:
        pipeline["pipeline"].append({
            "type": "readers.las",
            "filename": str(laz),
            "override_srs": "EPSG:31983"
        })

    # 2. Merge
    pipeline["pipeline"].append({"type": "filters.merge"})

    # 3. Remoção de vegetação (classes 3, 4, 5)
    pipeline["pipeline"].append({
        "type": "filters.range",
        "limits": "Classification![3:5],Classification![19:19]"
    })

    # 4. Altura normalizada
    pipeline["pipeline"].append({
        "type": "filters.hag_nn"
    })

    # 5. Raster Z (MDS)
    pipeline["pipeline"].append({
        "type": "writers.gdal",
        "filename": str(out_mds),
        "resolution": RESOLUTION,
        "width": width,
        "height": height,
        "origin_x": origin_x,
        "origin_y": origin_y,
        "output_type": "max",
        "dimension": "Z",
        "data_type": "float32",
        "nodata": NODATA,
        "gdaldriver":"GTiff",
        "gdalopts":"COMPRESS=ZSTD,PREDICTOR=3,BIGTIFF=YES,TILED=YES",
        "where": "(Classification != 3 && Classification != 4 && Classification != 5 && Classification != 19)",
        "default_srs": "EPSG:31983",
        "override_srs": "EPSG:31983",
    })

    # 6. Raster HAG
    pipeline["pipeline"].append({
        "type": "writers.gdal",
        "filename": str(out_hag),
        "resolution": RESOLUTION,
        "width": width,
        "height": height,
        "origin_x": origin_x,
        "origin_y": origin_y,
        "output_type": "max",
        "dimension": "HeightAboveGround",
        "data_type": "float32",
        "nodata": NODATA,
        "gdaldriver":"GTiff",
        "gdalopts":"COMPRESS=ZSTD,PREDICTOR=3,BIGTIFF=YES,TILED=YES",
        "where": "(Classification != 3 && Classification != 4 && Classification != 5 && Classification != 19)",
        "default_srs": "EPSG:31983",
        "override_srs": "EPSG:31983",
    })

    return pipeline


def run_pdal_pipeline(pipeline: dict):
    proc = subprocess.run(
        ["pdal", "pipeline", "--stdin"],
        input=json.dumps(pipeline),
        text=True,
        capture_output=True,
    )
    if proc.returncode != 0:
        raise RuntimeError(proc.stderr)


# ---------------------------------------------------------------------
# Processamento de uma quadrícula
# ---------------------------------------------------------------------

def process_one_tile(row, year: int, overwrite: bool = False):
    tile_field = tile_id_field_for_year(year)
    tile_id = str(row[tile_field])

    geom = row.geometry
    minx, miny, maxx, maxy = geom.bounds

    origin_x = snap_origin(minx, RESOLUTION)
    origin_y = snap_origin(miny, RESOLUTION)

    width = math.ceil((maxx - origin_x) / RESOLUTION)
    height = math.ceil((maxy - origin_y) / RESOLUTION)

    base_out = OUTPUT_BASE / str(year)
    out_mds_dir = base_out / "tiles_MDS"
    out_hag_dir = base_out / "tiles_HAG"

    out_mds_dir.mkdir(parents=True, exist_ok=True)
    out_hag_dir.mkdir(parents=True, exist_ok=True)

    out_mds = out_mds_dir / f"mds_tile_{tile_id}.tif"
    out_hag = out_hag_dir / f"hag_tile_{tile_id}.tif"

    if out_mds.exists() and out_hag.exists() and not overwrite:
        print(f"[skip] tile {tile_id}")
        return

    laz_files = laz_files_for_tile(row, year)

    pipeline = build_pdal_pipeline(
        laz_files=laz_files,
        origin_x=origin_x,
        origin_y=origin_y,
        width=width,
        height=height,
        out_mds=out_mds,
        out_hag=out_hag,
    )

    print(f"[run] tile {tile_id}")
    run_pdal_pipeline(pipeline)


# ---------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Build MHAC tiles (MDS + HAG)")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--tile-id", help="Processar apenas uma quadrícula específica")
    parser.add_argument("--overwrite", action="store_true")

    args = parser.parse_args()

    gdf = load_articulacao(args.year)

    if args.tile_id:
        tile_field = tile_id_field_for_year(args.year)

        if tile_field not in gdf.columns:
            raise KeyError(
                f"Campo '{tile_field}' não encontrado no índice de articulação "
                f"para o ano {args.year}. Colunas disponíveis: {list(gdf.columns)}"
            )

        gdf = gdf[gdf[tile_field].astype(str) == args.tile_id]

        if gdf.empty:
            raise ValueError(
                f"Quadrícula não encontrada para o ano {args.year}: {args.tile_id}"
            )

    for _, row in gdf.iterrows():
        process_one_tile(row, args.year, overwrite=args.overwrite)


if __name__ == "__main__":
    main()
