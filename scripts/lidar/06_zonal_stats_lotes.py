#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import re

import geopandas as gpd
import rasterio
from rasterstats import zonal_stats
import numpy as np
import pandas as pd
from tqdm import tqdm


# ---------------------------------------------------------------------
# Configuração fixa de diretórios
# ---------------------------------------------------------------------

BASE_DIR = Path("/Users/fernandogomes/MacLab/mhac")

LOTES_DIR = BASE_DIR / "data/lotes"
CITY_MOSAICS_DIR = BASE_DIR / "data/city_mosaics"

OUTPUT_DIR = BASE_DIR / "data/zonal_stats/lotes"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------

def compute_pixel_area(raster_path: Path) -> float:
    with rasterio.open(raster_path) as ds:
        res_x, res_y = ds.res
    return abs(res_x * res_y)


def load_filtered_raster(raster_path: Path, min_height: float = 2.0):
    with rasterio.open(raster_path) as ds:
        data = ds.read(1).astype("float32")
        affine = ds.transform
        nodata = ds.nodata

    invalid = data <= min_height
    if nodata is not None:
        invalid |= data == nodata

    data[invalid] = np.nan
    return data, affine


def parse_district_from_filename(path: Path):
    """
    Extrai número e nome do distrito a partir do nome do arquivo.
    Ex: SIRGAS_GPKG_LOTES_01_AGUA_RASA.gpkg
    """
    m = re.match(
        r"SIRGAS_GPKG_LOTES_(\d{2})_(.+)\.gpkg",
        path.name,
    )
    if not m:
        return None

    return {
        "numero": int(m.group(1)),
        "nome": m.group(2).lower(),
        "path": path,
    }


def parse_args():
    parser = argparse.ArgumentParser(
        description="Zonal statistics dos lotes fiscais sobre mosaicos LiDAR"
    )

    parser.add_argument(
        "--year",
        type=int,
        required=True,
        choices=[2017, 2020, 2024],
        help="Ano do mosaico LiDAR",
    )

    parser.add_argument(
        "--raster",
        type=str,
        default="hag",
        help="Prefixo do raster (ex: hag, mds, etc.)",
    )

    parser.add_argument(
        "--distrito",
        type=int,
        default=None,
        help="Número do distrito (ex: 90). Se omitido, processa todos.",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Número de lotes por bloco de processamento",
    )

    return parser.parse_args()


# ---------------------------------------------------------------------
# Script principal
# ---------------------------------------------------------------------

def main():
    args = parse_args()

    raster_path = (
        CITY_MOSAICS_DIR
        / f"{args.raster}_{args.year}_mosaic_nodata.tif"
    )

    if not raster_path.exists():
        raise FileNotFoundError(f"Raster não encontrado: {raster_path}")

    print("=" * 60)
    print("ZONAL STATS — LOTES FISCAIS")
    print(f"Ano: {args.year}")
    print(f"Raster: {raster_path.name}")
    print("=" * 60)

    # -----------------------------------------------------------------
    # Descobrir distritos
    # -----------------------------------------------------------------

    districts = []
    for p in LOTES_DIR.glob("SIRGAS_GPKG_LOTES_*.gpkg"):
        info = parse_district_from_filename(p)
        if info:
            districts.append(info)

    if args.distrito is not None:
        districts = [
            d for d in districts if d["numero"] == args.distrito
        ]

    if not districts:
        raise RuntimeError("Nenhum distrito encontrado para processar.")

    print(f"→ Distritos selecionados: {len(districts)}")

    # -----------------------------------------------------------------
    # Raster e pixel area
    # -----------------------------------------------------------------

    pixel_area = compute_pixel_area(raster_path)

    print("→ Carregando raster filtrado (pixels > 2.0 m)...")
    raster_data, raster_affine = load_filtered_raster(
        raster_path, min_height=2.0
    )

    # -----------------------------------------------------------------
    # Loop por distrito
    # -----------------------------------------------------------------

    for d in districts:
        print("-" * 60)
        print(f"→ Distrito {d['numero']:02d} — {d['nome'].upper()}")

        gdf = gpd.read_file(d["path"])

        # Filtro semântico
        gdf = gdf[gdf["lo_tp_lote"] == "F"].copy()
        gdf = gdf.reset_index(drop=True)

        if gdf.empty:
            print("  • Nenhum lote tipo 'F', pulando.")
            continue

        print(f"  • Lotes selecionados: {len(gdf)}")

        # Área e contagem teórica
        gdf["area_m2"] = gdf.geometry.area
        gdf["count_total"] = (
            gdf["area_m2"] / pixel_area
        ).round().astype("Int64")

        # Chunking
        chunks = [
            gdf.iloc[i : i + args.chunk_size]
            for i in range(0, len(gdf), args.chunk_size)
        ]

        all_results = []

        for chunk in tqdm(
            chunks,
            desc=f"Processando distrito {d['numero']:02d}",
            unit="bloco",
        ):
            geoms = list(chunk.geometry.values)

            stats = zonal_stats(
                geoms,
                raster_data,
                affine=raster_affine,
                stats=[
                    "count",
                    "min",
                    "sum",
                    "max",
                    "mean",
                    "median",
                    "std",
                ],
                nodata=np.nan,
                all_touched=True,
            )

            stats_df = (
                pd.DataFrame(stats)
                .rename(columns={"count": "count_valid"})
            )

            chunk_reset = chunk.reset_index(drop=True)

            partial = chunk_reset.join(stats_df)

            partial["valid_frac"] = (
                partial["count_valid"] / partial["count_total"]
            )
            partial["nodata_frac"] = 1.0 - partial["valid_frac"]
            partial["valid_area_m2"] = (
                partial["count_valid"] * pixel_area
            )

            partial["year"] = args.year
            partial["raster"] = args.raster
            partial["height_threshold_m"] = 2.0
            partial["distrito"] = d["numero"]
            partial["distrito_nome"] = d["nome"]

            all_results.append(partial)

        result = gpd.GeoDataFrame(
            pd.concat(all_results, ignore_index=True),
            crs=gdf.crs,
        )

        out_path = (
            OUTPUT_DIR
            / f"lotes_distrito_{d['numero']:02d}_{d['nome']}_{args.raster}_{args.year}.gpkg"
        )

        print(f"→ Salvando resultado em:\n  {out_path}")
        result.to_file(out_path, driver="GPKG")

    print("✓ Zonal stats por lote finalizado com sucesso.")


if __name__ == "__main__":
    main()
