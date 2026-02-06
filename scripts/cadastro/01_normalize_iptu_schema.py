#!/usr/bin/env python3
from __future__ import annotations

from pathlib import Path
import pandas as pd
import unicodedata
import re

# --------------------------------------------------
# Projeto
# --------------------------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[2]

IPTU_DIR = PROJECT_ROOT / "data" / "iptu"
OUT_DIR = PROJECT_ROOT / "data" / "iptu_canonical"

OUT_DIR.mkdir(parents=True, exist_ok=True)

# --------------------------------------------------
# Utilidades
# --------------------------------------------------
def normalize_colname(col: str) -> str:
    """
    Normaliza nome de coluna:
    - remove BOM
    - remove acentos
    - snake_case
    - remove caracteres especiais
    """
    col = col.replace("ï»¿", "").strip()
    col = unicodedata.normalize("NFKD", col)
    col = col.encode("ascii", "ignore").decode("ascii")
    col = col.lower()
    col = re.sub(r"[^\w]+", "_", col)
    col = re.sub(r"_+", "_", col)
    return col.strip("_")


def force_float(df: pd.DataFrame, cols: list[str]):
    """
    Converte colunas para float64 de forma robusta.
    Qualquer valor não numérico vira NaN (errors='coerce').
    """
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(
                df[c]
                .astype(str)
                .str.replace(",", ".", regex=False)
                .str.strip(),
                errors="coerce",
            ).astype("float64")


def force_string(df: pd.DataFrame, cols: list[str]):
    """
    Força colunas categóricas/textuais para string.
    """
    for c in cols:
        if c in df.columns:
            df[c] = df[c].astype("string")


# --------------------------------------------------
# Schema canônico IPTU
# --------------------------------------------------
# 🔢 Numéricos (todos float64 por consistência temporal)
FLOAT_COLUMNS = [
    "ano_construcao_corrigido",
    "ano_inicio_vida_contribuinte",
    "ano_exercicio",
    "mes_inicio_vida_contribuinte",
    "area_construida",
    "area_terreno",
    "area_ocupada",
    "quantidade_pavimentos",
    "quantidade_esquinas_frentes",
    "fracao_ideal",
    "fator_obsolescencia",
    "valor_m2_construcao",
    "valor_m2_terreno",
    "testada_para_calculo",
    "fase_contribuinte",
]

# 🔤 Categóricos / Identitários
STRING_COLUMNS = [
    "numero_contribuinte",
    "bairro_imovel",
    "cep_imovel",
    "codlog_imovel",
    "nome_logradouro_imovel",
    "numero_imovel",
    "complemento_imovel",
    "referencia_imovel",
    "tipo_padrao_construcao",
    "tipo_terreno",
    "tipo_uso_imovel",
    "numero_condominio",
    "numero_nl",
    "data_cadastramento",
]

# --------------------------------------------------
# Processamento por ano
# --------------------------------------------------
for parquet_path in sorted(IPTU_DIR.glob("iptu_*.parquet")):
    year = parquet_path.stem.split("_")[-1]
    print(f"\n🔧 Normalizando IPTU {year}")

    df = pd.read_parquet(parquet_path)

    # Normalizar nomes das colunas
    df = df.rename(columns={c: normalize_colname(c) for c in df.columns})

    # Resolver variações conhecidas (ex: barras, pluralização)
    rename_fixes = {
        "quantidade_esquinas_frentes": "quantidade_esquinas_frentes",
        "quantidade_esquinas_frentes_": "quantidade_esquinas_frentes",
        "quantidade_esquinas_frentes__": "quantidade_esquinas_frentes",
    }
    df = df.rename(columns=rename_fixes)

    # Forçar tipos
    force_float(df, FLOAT_COLUMNS)
    force_string(df, STRING_COLUMNS)

    # Salvar Parquet canônico
    out_path = OUT_DIR / f"iptu_{year}_canonical.parquet"
    df.to_parquet(out_path, engine="pyarrow", compression="snappy")

    print(f"  ✔ salvo em {out_path}")
    print(f"  → {len(df.columns)} colunas | {len(df):,} registros")

print("\n✅ Normalização IPTU concluída com sucesso")
