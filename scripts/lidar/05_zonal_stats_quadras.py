#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

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

QUADRAS_GPKG = BASE_DIR / "data/downloads/SIRGAS_GPKG_quadraMDSF-DISSOLVIDO.gpkg"
CITY_MOSAICS_DIR = BASE_DIR / "data/city_mosaics"

OUTPUT_DIR = BASE_DIR / "data/zonal_stats"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------

def compute_pixel_area(raster_path: Path) -> float:
    """Retorna a área de um pixel (m²) a partir do raster."""
    with rasterio.open(raster_path) as ds:
        res_x, res_y = ds.res
    return abs(res_x * res_y)


def load_filtered_raster(raster_path: Path, min_height: float = 2.0):
    """
    Carrega o raster e converte TODOS os pixels inválidos
    (<= min_height e nodata original) em np.nan.
    """
    with rasterio.open(raster_path) as ds:
        data = ds.read(1).astype("float32")
        affine = ds.transform
        nodata = ds.nodata

    invalid = (data <= min_height)
    if nodata is not None:
        invalid |= (data == nodata)

    data[invalid] = np.nan

    return data, affine



def parse_args():
    parser = argparse.ArgumentParser(
        description="Zonal statistics das quadras fiscais sobre mosaicos LiDAR"
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
        "--limit",
        type=int,
        default=None,
        help="Número máximo de quadras (modo teste)",
    )

    parser.add_argument(
        "--chunk-size",
        type=int,
        default=1000,
        help="Número de quadras por bloco de processamento",
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
    print("ZONAL STATS — QUADRAS FISCAIS")
    print(f"Ano: {args.year}")
    print(f"Raster: {raster_path.name}")
    print("=" * 60)

    # -----------------------------------------------------------------
    # Leitura das quadras
    # -----------------------------------------------------------------

    print("→ Lendo quadras fiscais...")
    gdf = gpd.read_file(QUADRAS_GPKG)

    # -----------------------------------------------------------------
    # Geração do identificador SQ (Setor + Quadra)
    # -----------------------------------------------------------------

    gdf["qd_setor"] = gdf["qd_setor"].astype(str).str.zfill(3)
    gdf["qd_fiscal"] = gdf["qd_fiscal"].astype(str).str.zfill(3)

    gdf["SQ"] = gdf["qd_setor"] + gdf["qd_fiscal"]


    if args.limit:
        gdf = gdf.head(args.limit)

    gdf = gdf.reset_index(drop=True)

    print(f"→ Quadras selecionadas: {len(gdf)}")

    # -----------------------------------------------------------------
    # Preparação geométrica
    # -----------------------------------------------------------------

    pixel_area = compute_pixel_area(raster_path)

    gdf["area_m2"] = gdf.geometry.area
    gdf["count_total"] = (gdf["area_m2"] / pixel_area).round().astype("Int64")

    # -----------------------------------------------------------------
    # Carregar raster filtrado (HAG > 2.0)
    # -----------------------------------------------------------------

    print("→ Carregando raster filtrado (pixels > 2.0 m)...")
    raster_data, raster_affine = load_filtered_raster(
        raster_path, min_height=2.0
    )

    # -----------------------------------------------------------------
    # Estatísticas zonais em blocos
    # -----------------------------------------------------------------

    print("→ Calculando estatísticas zonais em blocos...")

    chunks = [
        gdf.iloc[i : i + args.chunk_size]
        for i in range(0, len(gdf), args.chunk_size)
    ]

    all_results = []

    for chunk in tqdm(chunks, desc="Processando quadras", unit="bloco"):
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
            geojson_out=False,
            all_touched=False,
        )

        stats_df = pd.DataFrame(stats).rename(
            columns={"count": "count_valid"}
        )

        chunk_reset = chunk.reset_index(drop=True)

        partial = chunk_reset[
            ["qd_id", "SQ", "qd_setor", "qd_fiscal", "geometry", "area_m2", "count_total"]
        ].copy()

        partial = partial.join(stats_df)

        # Derivadas (AGORA só pixels > 2m)
        partial["valid_frac"] = partial["count_valid"] / partial["count_total"]
        partial["nodata_frac"] = 1.0 - partial["valid_frac"]
        partial["valid_area_m2"] = partial["count_valid"] * pixel_area

        # Metadados
        partial["year"] = args.year
        partial["raster"] = args.raster
        partial["height_threshold_m"] = 2.0

        all_results.append(partial)

    # -----------------------------------------------------------------
    # Consolidação final
    # -----------------------------------------------------------------

    result = gpd.GeoDataFrame(
        pd.concat(all_results, ignore_index=True),
        crs=gdf.crs,
    )

    # -----------------------------------------------------------------
    # Escrita do GPKG
    # -----------------------------------------------------------------

    suffix = "test" if args.limit else "full"
    out_path = OUTPUT_DIR / f"quadras_{args.raster}_{args.year}_{suffix}.gpkg"

    print("→ Salvando resultado em:")
    print(f"  {out_path}")

    result.to_file(out_path, driver="GPKG")

    print("✓ Zonal stats finalizado com sucesso.")


if __name__ == "__main__":
    main()
