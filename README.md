# Cluster-Transfusion

Projeto de estratificação de pacientes críticos com transfusão, usando matching por propensity score, embeddings temporais (MiniRocket) e descoberta de subgrupos clínicos.

## Objetivo

Gerar uma análise reprodutível para o estudo de associação entre transfusão de concentrado de hemácias e desfechos clínicos em UTI, com foco em:

- coorte temporal padronizada por `stay_id`;
- pareamento transfundidos vs. controles;
- clusterização por representação temporal;
- varredura de regras e subgrupos com diferença de mortalidade.

## Estrutura local esperada

```text
dataset/
  timegrid_features/                # privado (não versionado)
  outputs_outcomes/                 # privado (não versionado)
    outcomes_by_stay_full.csv
configs/
  lab_itemids.yaml
outputs/                            # gerado localmente (não versionado)
scripts/
```

## Requisitos

- Python 3.10+
- duckdb
- pandas
- numpy
- pyarrow
- scikit-learn

```bash
pip install duckdb pandas numpy pyarrow scikit-learn
```

## Execução

O fluxo é fechado e já está configurado com os valores usados no estudo.

```bash
python scripts/run_all.py
```

Configuração fixa em `scripts/run_all.py`:

- `RUN_ID=run_cal03_replace_full_w48`
- `WINDOW=48`
- `SEED=42`
- `CALIPER=0.3`
- `RATIO=1`
- `REPLACE=True`
- `K_LIST=2,3,4,5,6`
- `EMBEDDING=minirocket`

## Etapas executadas automaticamente

1. `step0_build_outcomes_cohort.py`
2. `step1_build_baseline_features.py`
3. `step2_match_controls.py`
4. `step3_embed_minirocket_temporal.py`
5. `step4_reports.py`

## Saídas

As saídas são geradas localmente em `outputs/runs/run_cal03_replace_full_w48/` e não são versionadas no repositório público.

## Publicação

Este repositório não inclui dados sensíveis de pacientes. Arquivos com identificadores (`stay_id`, `subject_id`) e artefatos de execução permanecem apenas no ambiente local.
