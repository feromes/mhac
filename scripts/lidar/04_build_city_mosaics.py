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

def process_product_year(product, year, fill_nodata=False):

    print("\n====================================================")
    print(f"PROCESSANDO: {product.upper()} | {year}")
    print("====================================================")

    tiles_dir = DATA_BASE / str(year) / PRODUCT_DIRS[product]

    if not tiles_dir.exists():
        print(f"⚠️ Diretório inexistente: {tiles_dir}")
        return

    tifs = sorted(tiles_dir.glob("*.tif"))
    print(f"Total de tiles encontrados: {len(tifs)}")

    if not tifs:
        print("⚠️ Nenhum TIFF encontrado.")
        return

    vrt = OUTPUT_DIR / f"{product}_{year}.vrt"
    mosaic = OUTPUT_DIR / f"{product}_{year}_mosaic.tif"
    mosaic_nodata = OUTPUT_DIR / f"{product}_{year}_mosaic_nodata.tif"
    mosaic_filled = OUTPUT_DIR / f"{product}_{year}_mosaic_filled.tif"

    # --------------------------------------------------
    # FILELIST (robusto para milhares de tiles)
    # --------------------------------------------------

    filelist = OUTPUT_DIR / f"{product}_{year}_tiles.txt"
    filelist.write_text("\n".join(str(p) for p in tifs))

    # --------------------------------------------------
    # 1. BUILD VRT
    # --------------------------------------------------

    print("\n[1/3] Construindo VRT...")
    run([
        "gdalbuildvrt",
        "-input_file_list", str(filelist),
        str(vrt)
    ])

    # --------------------------------------------------
    # 2. TRANSLATE
    # --------------------------------------------------

    print("\n[2/3] Gerando mosaico GeoTIFF...")
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

    print("\n[3/3] Padronizando NODATA...")
    run([
        "gdalwarp",
        "-dstnodata", "-9999",
        "-r", "near",
        str(mosaic),
        str(mosaic_nodata)
    ])

    # --------------------------------------------------
    # 4. FILL (opcional)
    # --------------------------------------------------

    if fill_nodata:
        print("\n[4/4] Preenchendo NODATA...")
        run([
            "gdal_fillnodata.py",
            "-md", "50",
            "-si", "2",
            str(mosaic_nodata),
            str(mosaic_filled)
        ])
        print("\n✅ Finalizado:", mosaic_filled)
    else:
        print("\n✅ Finalizado:", mosaic_nodata)

# --------------------------------------------------
# MAIN
# --------------------------------------------------

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int)
    parser.add_argument("--product", type=str)
    parser.add_argument("--fill", action="store_true", help="Executar fillnodata")
    args = parser.parse_args()

    years = [args.year] if args.year else YEARS
    products = [args.product] if args.product else PRODUCT_DIRS.keys()

    for year in years:
        for product in products:
            process_product_year(product, year, fill_nodata=args.fill)

if __name__ == "__main__":
    main()
