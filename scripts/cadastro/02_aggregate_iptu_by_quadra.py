#!/usr/bin/env python3
"""
02_aggregate_iptu_by_quadra.py

Agrega a base canônica de IPTU por Setor + Quadra (SQ),
normalizando áreas por fração ideal, calculando métricas
cadastrais e vinculando o resultado à geometria oficial
das quadras fiscais (GeoPackage).

Autor: Fernando Gomes
Projeto: MHAC / OGDC
"""

from pathlib import Path
import pandas as pd
import geopandas as gpd

# ---------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parents[2]

CADASTRO_DIR = BASE_DIR / "data" / "iptu_canonical"
OUTPUT_DIR = BASE_DIR / "data" / "cadastro"
DOWNLOADS_DIR = BASE_DIR / "data" / "downloads"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

ANOS = [2017, 2020, 2024]

QUADRAS_GPKG = DOWNLOADS_DIR / "SIRGAS_GPKG_quadraMDSF-DISSOLVIDO.gpkg"

# Campos esperados no IPTU
COL_NUM_CONTRIB = "numero_do_contribuinte"
COL_FRACAO = "fracao_ideal"
COL_AREA_TERRENO = "area_do_terreno"
COL_AREA_CONSTRUIDA = "area_construida"
COL_AREA_OCUPADA = "area_ocupada"
COL_PAVIMENTOS = "quantidade_de_pavimentos"


# ---------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------

def extrair_setor_quadra(df: pd.DataFrame) -> pd.DataFrame:
    """Extrai setor, quadra e SQ a partir do número do contribuinte."""
    df[COL_NUM_CONTRIB] = df[COL_NUM_CONTRIB].astype(str).str.zfill(6)

    df["setor"] = df[COL_NUM_CONTRIB].str.slice(0, 3)
    df["quadra"] = df[COL_NUM_CONTRIB].str.slice(3, 6)
    df["SQ"] = df["setor"] + df["quadra"]

    return df


def normalizar_areas(df: pd.DataFrame) -> pd.DataFrame:
    """Aplica fração ideal nas áreas compartilhadas."""
    df["area_terreno_corrigida"] = df[COL_AREA_TERRENO] * df[COL_FRACAO]
    df["area_ocupada_corrigida"] = df[COL_AREA_OCUPADA] * df[COL_FRACAO]
    df["area_construida_corrigida"] = df[COL_AREA_CONSTRUIDA]

    return df


def agregar_por_quadra(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega métricas cadastrais por Setor + Quadra."""
    return (
        df.groupby(["SQ", "setor", "quadra"], as_index=False)
        .agg(
            area_terreno_m2=("area_terreno_corrigida", "sum"),
            area_construida_m2=("area_construida_corrigida", "sum"),
            area_ocupada_m2=("area_ocupada_corrigida", "sum"),
            pavimentos_min=(COL_PAVIMENTOS, "min"),
            pavimentos_max=(COL_PAVIMENTOS, "max"),
        )
    )


# ---------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------

def processar_ano(ano: int) -> pd.DataFrame:
    print(f"\n▶ Processando IPTU {ano}")

    path = CADASTRO_DIR / f"iptu_{ano}_canonical.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")

    df = pd.read_parquet(path)

    df = extrair_setor_quadra(df)
    df = normalizar_areas(df)

    quadras = agregar_por_quadra(df)
    quadras["ano"] = ano

    print(f"  • Quadras agregadas: {len(quadras):,}")

    return quadras


def main():
    # -----------------------------------------------------------------
    # 1. Processa IPTU (tabular)
    # -----------------------------------------------------------------
    quadras_all = [processar_ano(ano) for ano in ANOS]
    df_iptu = pd.concat(quadras_all, ignore_index=True)

    # -----------------------------------------------------------------
    # 2. Carrega geometria oficial das quadras
    # -----------------------------------------------------------------
    print("\n▶ Carregando geometria das quadras fiscais")
    gdf_quadras = gpd.read_file(
        QUADRAS_GPKG,
        columns=["qd_id", "qd_setor", "qd_fiscal", "geometry"],
    )

    gdf_quadras["qd_setor"] = gdf_quadras["qd_setor"].astype(str).str.zfill(3)
    gdf_quadras["qd_fiscal"] = gdf_quadras["qd_fiscal"].astype(str).str.zfill(3)
    gdf_quadras["SQ"] = gdf_quadras["qd_setor"] + gdf_quadras["qd_fiscal"]

    # ------------------------------------------------------------------
    # 2.5. IPTUs sem quadra fiscal (SQ) → SQ = "000000"
    # ------------------------------------------------------------------

    iptus_sem_quadra = df_iptu[
        ~df_iptu["SQ"].isin(gdf_quadras["SQ"])
    ]

    print(
        f"⚠ IPTUs sem quadra correspondente: "
        f"{len(iptus_sem_quadra):,}"
    )

    # -----------------------------------------------------------------
    # 3. Join espacial (atributivo)
    # -----------------------------------------------------------------
    print("▶ Realizando join IPTU ↔ Quadras")
    gdf_final = gdf_quadras.merge(
        df_iptu,
        on="SQ",
        how="left",
        # validate="one_to_one",
    )

    # -----------------------------------------------------------------
    # 4. Exportação geográfica (um arquivo por ano)
    # -----------------------------------------------------------------
    print("\n▶ Exportando GeoPackages por ano")

    for ano in ANOS:
        gdf_ano = gdf_final[gdf_final["ano"] == ano].copy()

        output_path = OUTPUT_DIR / f"quadras_iptu_{ano}.gpkg"

        gdf_ano.to_file(
            output_path,
            layer="quadras_iptu",
            driver="GPKG",
        )

        print(
            f"  • {ano}: {len(gdf_ano):,} quadras → {output_path.name}"
        )

    print("\n✅ Processo concluído")
    print(f"📦 Arquivo geográfico final: {output_path}")
    print(f"🧱 Quadras totais: {len(gdf_final):,}")


if __name__ == "__main__":
    main()
