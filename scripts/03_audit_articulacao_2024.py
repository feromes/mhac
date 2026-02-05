#!/usr/bin/env python3
from pathlib import Path
import geopandas as gpd

# --------------------------------------------------
# Configuração
# --------------------------------------------------

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

LIDAR_RAW_SUBDIRS = {
    2017: "LiDAR-Sampa-2017",
    2020: "LiDAR-Sampa-2020",
    2024: "LiDAR-Sampa-2024",
}

LIDAR_FILENAME_RULES = {
    2017: dict(prefix="MDS_color_", suffix=".laz"),
    2020: dict(prefix="MDS_", suffix="_1000.laz"),
    2024: dict(prefix="", suffix=".laz"),
}

OUT_DIR = Path("data/audit")
OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# Função auditoria por ano
# --------------------------------------------------

def audit_year(year):

    print(f"\n===============================")
    print(f"📅 Auditoria do ano {year}")
    print(f"===============================")

    artic_path = ARTICULACAO_INDEX[year]
    tile_field = TILE_ID_FIELD[year]

    raw_dir = LIDAR_BASE_DIR / LIDAR_RAW_SUBDIRS[year]
    rule = LIDAR_FILENAME_RULES[year]

    prefix = rule["prefix"]
    suffix = rule["suffix"]

    if not artic_path.exists():
        raise FileNotFoundError(f"Articulação não encontrada: {artic_path}")

    if not raw_dir.exists():
        raise FileNotFoundError(f"Diretório LiDAR não encontrado: {raw_dir}")

    # --------------------------------------------------
    # Leitura articulação (aceita zip SHP e GPKG)
    # --------------------------------------------------

    print("📥 Carregando articulação...")
    gdf = gpd.read_file(artic_path)

    print(f"Total de registros: {len(gdf)}")

    # --------------------------------------------------
    # Diagnóstico geométrico
    # --------------------------------------------------

    geom_null = gdf.geometry.isna().sum()
    geom_empty = gdf.geometry.apply(lambda g: g.is_empty if g is not None else False).sum()
    geom_invalid = gdf.geometry.apply(
        lambda g: (g is not None and not g.is_empty and not g.is_valid)
    ).sum()

    print("\n🧭 Diagnóstico geométrico:")
    print(f"  • Geometria nula: {geom_null}")
    print(f"  • Geometria vazia: {geom_empty}")
    print(f"  • Geometria inválida: {geom_invalid}")

    # --------------------------------------------------
    # Arquivos LAZ físicos
    # --------------------------------------------------

    laz_files = sorted(p.stem for p in raw_dir.glob(f"*{suffix}") if p.is_file())
    laz_set = set(laz_files)

    print(f"\n📂 LAZ físicos encontrados: {len(laz_files)}")

    # --------------------------------------------------
    # Tiles da articulação
    # --------------------------------------------------

    art_tiles = (
        gdf[tile_field]
        .dropna()
        .astype(str)
        .str.strip()
        .unique()
        .tolist()
    )

    art_laz_names = [
        f"{prefix}{tile}{suffix[:-4]}" for tile in art_tiles
    ]

    art_set = set(art_laz_names)

    # --------------------------------------------------
    # Comparação
    # --------------------------------------------------

    laz_not_in_art = sorted(laz_set - art_set)
    art_not_in_laz = sorted(art_set - laz_set)

    print("\n🔍 Comparação disco × articulação:")
    print(f"  • LAZ no disco sem articulação: {len(laz_not_in_art)}")
    print(f"  • Articulação sem LAZ no disco: {len(art_not_in_laz)}")

    # --------------------------------------------------
    # Salvando relatórios
    # --------------------------------------------------

    (OUT_DIR / f"laz_not_in_articulacao_{year}.txt").write_text("\n".join(laz_not_in_art))
    (OUT_DIR / f"articulacao_not_in_laz_{year}.txt").write_text("\n".join(art_not_in_laz))

    print(f"💾 Relatórios do ano {year} salvos em {OUT_DIR}")

# --------------------------------------------------
# Execução
# --------------------------------------------------

def main():
    for year in [2017, 2020, 2024]:
        audit_year(year)

if __name__ == "__main__":
    main()
