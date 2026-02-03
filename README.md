# MHAC — Morphological Height Above Construction

Este repositório contém os **scripts iniciais para a geração do MHAC (Morphological Height Above Construction)** a partir de dados LiDAR aerotransportados (ALS), com foco na produção de **rasters de 1 metro de resolução** ano-a-ano para a cidade de São Paulo.

Nesta primeira etapa, o objetivo é **produzir um conjunto consistente de tiles rasterizados** (MDS e HAG) para diferentes campanhas LiDAR, que servirão como base para extrações posteriores de métricas morfológicas, análises multitemporais e comparação com dados cadastrais.

---

## 🎯 Objetivo desta fase

* Gerar **rasters de altura máxima** a partir de nuvens LiDAR ALS
* Padronizar:

  * resolução espacial (1 m)
  * origem do grid (snap em múltiplos inteiros)
  * estatística de agregação (valor máximo)
* Produzir resultados **comparáveis entre diferentes anos** (2017, 2020, 2024)
* Persistir artefatos intermediários que servirão de base para análises futuras

> Nesta fase, o projeto **não realiza ainda análises morfológicas agregadas**, mosaicos globais ou tratamentos avançados de pós-processamento.

---

## 📁 Estrutura do repositório

```
mhac/
├── data/
│   └── processed/
│       ├── 2017/
│       │   ├── tiles_MDS/
│       │   └── tiles_HAG/
│       ├── 2020/
│       │   ├── tiles_MDS/
│       │   └── tiles_HAG/
│       └── 2024/
│           ├── tiles_MDS/
│           └── tiles_HAG/
└── scripts/
    └── 01_build_mhac_tiles.py
```

### Descrição dos produtos

* **MDS (Modelo Digital de Superfície filtrado)**
  Raster de 1 m representando o valor máximo da coordenada Z dos pontos classificados como edificação.

* **HAG (Height Above Ground)**
  Raster de 1 m representando a altura máxima acima do terreno (normalizada via `filters.hag_nn`).

Cada arquivo corresponde a **uma quadrícula LiDAR** (tile) definida no índice de articulação oficial de cada campanha.

---

## ⚙️ Script principal

### `01_build_mhac_tiles.py`

Script responsável por:

* localizar os arquivos LAZ correspondentes a cada quadrícula
* filtrar retornos de vegetação
* normalizar alturas em relação ao terreno
* rasterizar usando **estatística de valor máximo**
* garantir alinhamento espacial consistente entre anos

### Execução básica (tile único)

```bash
python scripts/01_build_mhac_tiles.py \
  --year 2020 \
  --tile-id 3313-311
```

### Com sobrescrita

```bash
python scripts/01_build_mhac_tiles.py \
  --year 2020 \
  --tile-id 3313-311 \
  --overwrite
```

---

## 🧠 Decisões metodológicas (resumo)

* **Resolução espacial:** 1 metro
* **Estatística de agregação:** valor máximo (Z e HAG)
* **Motivação:**

  * evitar subestimação causada por médias em ambientes com forte verticalidade
  * reduzir sensibilidade à geometria de aquisição e às linhas de voo
  * garantir maior robustez para comparações multitemporais

O MHAC representa a **envoltória superior observada do ambiente construído**, sendo adequado para análises comparativas com dados cadastrais e estudos de dinâmica morfológica.

---

## 🚧 Estado do projeto

* ✅ Geração de tiles MDS e HAG por ano
* ✅ Padronização espacial entre campanhas
* ⏳ Geração de mosaicos urbanos
* ⏳ Pós-processamento (preenchimento de vazios, suavizações)
* ⏳ Extração de métricas e análises multitemporais

---

## 📌 Observação

Os dados LiDAR brutos **não são versionados neste repositório**.
O código assume acesso local aos arquivos LAZ e aos índices de articulação correspondentes.

---

## ✍️ Autor

Fernando Gomes
Projeto em desenvolvimento contínuo
