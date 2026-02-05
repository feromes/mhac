#!/usr/bin/env python3
from pathlib import Path
import subprocess
import argparse
import sys

# --------------------------------------------------
# CONFIGURAÇÃO
# --------------------------------------------------

MHAC_BASE = Path("/Users/fernandogomes/MacLab/mhac")

DATA_BASE = MHAC_BASE / "data/processed"

YEARS = [2017, 2020, 2024]

PRODUCT_DIRS = {
    "hag": "tiles_HAG",
    "mds": "tiles_MDS",
}

OUTPUT_DIR = MHAC_BASE / "data/city_mosaics"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# EXECUTOR VERBOSO
# --------------------------------------------------

def run(cmd):
    print("\n[RUNNING]")
    print(" ".join(cmd))
    result = subprocess.run(cmd)
    if result.returncode != 0:
        print("\n❌ ERRO ao executar comando.")
        sys.exit(result.returncode)

# --------------------------------------------------
# PIPELINE
# --------------------------------------------------

def process_product_year(product, year):

    print("\n====================================================")
    print(f"PROCESSANDO: {product.upper()} | {year}")
    print("====================================================")

    def ensure_projection(tiles_dir, epsg="EPSG:31983"):

        fixed_dir = tiles_dir.parent / (tiles_dir.name + "_fixed")
        fixed_dir.mkdir(exist_ok=True)

        tifs = list(tiles_dir.glob("*.tif"))

        print(f"Verificando CRS de {len(tifs)} tiles...")

        for tif in tifs:

            info = subprocess.run(
                ["gdalinfo", str(tif)],
                capture_output=True,
                text=True
            ).stdout

            has_crs = "Coordinate System is:" in info and "null" not in info

            out = fixed_dir / tif.name

            if has_crs:
                subprocess.run(["cp", str(tif), str(out)])
            else:
                print(f"Fixando CRS: {tif.name}")
                subprocess.run([
                    "gdal_translate",
                    str(tif),
                    str(out),
                    "-a_srs", epsg
                ])

        return fixed_dir

    # tiles_dir = DATA_BASE / str(year) / PRODUCT_DIRS[product]
    tiles_dir_raw = DATA_BASE / str(year) / PRODUCT_DIRS[product]
    tiles_dir = ensure_projection(tiles_dir_raw)

    if not tiles_dir.exists():
        print(f"⚠️ Diretório inexistente: {tiles_dir}")
        return

    vrt = OUTPUT_DIR / f"{product}_{year}.vrt"
    mosaic = OUTPUT_DIR / f"{product}_{year}_mosaic.tif"
    mosaic_nodata = OUTPUT_DIR / f"{product}_{year}_mosaic_nodata.tif"
    mosaic_filled = OUTPUT_DIR / f"{product}_{year}_mosaic_filled.tif"

    tifs = sorted(str(p) for p in tiles_dir.glob("*.tif"))

    if not tifs:
        print("⚠️ Nenhum TIFF encontrado.")
        return

    # --------------------------------------------------
    # 1. BUILD VRT
    # --------------------------------------------------

    print("\n[1/4] Construindo VRT...")
    run([
        "gdalbuildvrt",
        str(vrt),
        *tifs
    ])

    # --------------------------------------------------
    # 2. TRANSLATE
    # --------------------------------------------------

    print("\n[2/4] Gerando mosaico GeoTIFF...")
    run([
        "gdal_translate",
        str(vrt),
        str(mosaic),
        "-co", "COMPRESS=LZW",
        "-co", "BIGTIFF=YES",
        "-co", "TILED=YES"
    ])

    # --------------------------------------------------
    # 3. PADRONIZANDO NODATA
    # --------------------------------------------------

    print("\n[3/4] Padronizando NODATA...")
    run([
        "gdalwarp",
        "-dstnodata", "-9999",
        "-r", "near",
        str(mosaic),
        str(mosaic_nodata)
    ])

    # --------------------------------------------------
    # 4. FILL NODATA
    # --------------------------------------------------

    print("\n[4/4] Preenchendo NODATA...")
    run([
        "gdal_fillnodata.py",
        "-md", "50",
        "-si", "2",
        str(mosaic_nodata),
        str(mosaic_filled)
    ])

    print("\n✅ Finalizado:", mosaic_filled)

# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--product", type=str)
    args = parser.parse_args()

    years = [args.year] if args.year else YEARS
    products = [args.product] if args.product else PRODUCT_DIRS.keys()

    for year in years:
        for product in products:
            process_product_year(product, year)

if __name__ == "__main__":
    main()
