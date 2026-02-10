#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import pandas as pd
import geopandas as gpd

# ---------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------

BASE_DIR = Path("data")

CADASTRO_DIR = BASE_DIR / "cadastro"
ZONAL_DIR = BASE_DIR / "zonal_stats"

YEARS = [2017, 2020, 2024]

OUT_DIR = Path("data/analises")
OUT_DIR.mkdir(parents=True, exist_ok=True)

KEY_FIELD = "SQ"  # ajuste se necessário


# ---------------------------------------------------------------------
# Funções utilitárias
# ---------------------------------------------------------------------

def load_cadastro(year: int) -> gpd.GeoDataFrame:
    path = CADASTRO_DIR / f"quadras_iptu_{year}.gpkg"
    gdf = gpd.read_file(path)
    gdf["year"] = year
    return gdf


def load_lidar(year: int) -> gpd.GeoDataFrame:
    path = ZONAL_DIR / f"quadras_hag_{year}_full.gpkg"
    gdf = gpd.read_file(path)
    gdf["year"] = year
    return gdf


def normalize_key(gdf: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    gdf[KEY_FIELD] = gdf[KEY_FIELD].astype(str).str.zfill(6)
    return gdf


def join_cadastro_lidar(
    cad: gpd.GeoDataFrame,
    lidar: gpd.GeoDataFrame,
) -> gpd.GeoDataFrame:
    return cad.merge(
        lidar.drop(columns="geometry"),
        on=[KEY_FIELD, "year"],
        how="outer",
        suffixes=("_cad", "_lidar"),
    )


# ---------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------

def main():
    joined_by_year = []

    for year in YEARS:
        print(f"▶ Processando {year}")

        cad = normalize_key(load_cadastro(year))
        lidar = normalize_key(load_lidar(year))

        gdf = join_cadastro_lidar(cad, lidar)

        # salva versão geográfica
        out_gpkg = OUT_DIR / f"quadras_cadastro_vs_lidar_{year}.gpkg"
        gdf.to_file(out_gpkg, driver="GPKG")

        joined_by_year.append(
            pd.DataFrame(gdf.drop(columns="geometry"))
        )

    # versão empilhada (long format)
    df_all = pd.concat(joined_by_year, ignore_index=True)

    out_csv = OUT_DIR / "quadras_cadastro_vs_lidar_all_years.csv"
    df_all.to_csv(out_csv, index=False)

    print("✔ Análises iniciais salvas com sucesso")


if __name__ == "__main__":
    main()
