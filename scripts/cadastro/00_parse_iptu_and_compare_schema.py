#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import zipfile
import pandas as pd
import json

# --------------------------------------------------
# Resolução da raiz do projeto
# --------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]

IPTU_DIR = Path("/Users/fernandogomes/dev/ogdc/IPTU")
DATA_DIR = PROJECT_ROOT / "data" / "iptu"

DATA_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# Configuração dos arquivos
# --------------------------------------------------
IPTU_FILES = {
    "2017": "IPTU_2017.zip",
    "2020": "IPTU_2020.zip",
    "2024": "IPTU_2024.zip",
}

# --------------------------------------------------
# Funções auxiliares
# --------------------------------------------------
def read_iptu_zip(zip_path: Path) -> pd.DataFrame:
    """
    Lê o primeiro CSV encontrado dentro do ZIP.
    Ajuste encoding/sep se necessário.
    """
    with zipfile.ZipFile(zip_path) as z:
        csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
        if not csv_files:
            raise RuntimeError(f"Nenhum CSV encontrado em {zip_path}")

        csv_name = csv_files[0]

        with z.open(csv_name) as f:
            df = pd.read_csv(
                f,
                sep=";",
                encoding="latin1",
                low_memory=False
            )

    return df


def schema_from_df(df: pd.DataFrame) -> dict:
    """
    Extrai schema simples: coluna -> dtype (string).
    """
    return {col: str(dtype) for col, dtype in df.dtypes.items()}


# --------------------------------------------------
# Processamento principal
# --------------------------------------------------
schemas = {}
dataframes = {}

for year, zip_name in IPTU_FILES.items():
    zip_path = IPTU_DIR / zip_name
    print(f"\n📦 Lendo IPTU {year}: {zip_path.name}")

    df = read_iptu_zip(zip_path)

    print(f"  → {len(df):,} registros")
    print(f"  → {len(df.columns)} colunas")

    schemas[year] = schema_from_df(df)
    dataframes[year] = df

    # Salva Parquet bruto
    parquet_path = DATA_DIR / f"iptu_{year}.parquet"
    df.to_parquet(parquet_path, engine="pyarrow", compression="snappy")

    print(f"  ✔ Parquet salvo em {parquet_path}")

# --------------------------------------------------
# Comparação de schemas
# --------------------------------------------------
all_columns = {
    year: set(schema.keys())
    for year, schema in schemas.items()
}

common_columns = set.intersection(*all_columns.values())
all_unique_columns = set.union(*all_columns.values())

schema_comparison = {
    "common_columns": sorted(common_columns),
    "per_year": {
        year: {
            "only_in_this_year": sorted(all_columns[year] - common_columns),
            "missing_from_this_year": sorted(common_columns - all_columns[year]),
        }
        for year in schemas
    },
    "dtype_differences": {}
}

# Detectar colunas comuns com tipos diferentes
for col in common_columns:
    dtypes = {
        year: schemas[year][col]
        for year in schemas
    }
    if len(set(dtypes.values())) > 1:
        schema_comparison["dtype_differences"][col] = dtypes

# --------------------------------------------------
# Salva relatório de schema
# --------------------------------------------------
schema_report_path = DATA_DIR / "iptu_schema_comparison.json"
with open(schema_report_path, "w", encoding="utf-8") as f:
    json.dump(schema_comparison, f, indent=2, ensure_ascii=False)

print("\n📊 Comparação de schema concluída")
print(f"✔ Relatório salvo em {schema_report_path}")
