# Cluster-Transfusion (MiniRocket)

Pipeline para reproduzir a analise final com **MiniRocket** (janela de 48h) usando dados clinicos locais.

## Publicacao segura

Este repositorio foi preparado para publicacao publica sem incluir dados sensiveis.

- Nao versiona dados de pacientes nem artefatos de execucao.
- Arquivos com identificadores (`stay_id`, `subject_id`) ficam somente no ambiente local.
- Logs com caminhos locais da maquina tambem ficam fora do Git.

## Estrutura esperada (local)

```text
dataset/
  timegrid_features/                # privado (nao versionado)
  outputs_outcomes/                 # privado (nao versionado)
    outcomes_by_stay_full.csv
configs/
  lab_itemids.yaml
outputs/                            # gerado localmente (nao versionado)
scripts/
```

## Dependencias

- Python 3.10+
- duckdb
- pandas
- numpy
- pyarrow
- scikit-learn

```bash
pip install duckdb pandas numpy pyarrow scikit-learn
```

## Execucao do pipeline

```bash
python scripts/run_all.py
```

Configuracao fixa em `scripts/run_all.py`:
- `RUN_ID=run_cal03_replace_full_w48`
- `WINDOW=48`
- `SEED=42`
- `CALIPER=0.3`
- `RATIO=1`
- `REPLACE=True`
- `K_LIST=2,3,4,5,6`
- embedding: `minirocket`

## Passos executados

1. `step0_build_outcomes_cohort.py`
2. `step1_build_baseline_features.py`
3. `step2_match_controls.py`
4. `step3_embed_minirocket_temporal.py`
5. `step4_reports.py --embedding minirocket --run_scan_suite`

## Nota para paper

Se voce ja versionou dados antes desta limpeza, remova o historico sensivel antes de publicar (por exemplo com `git filter-repo`) e gere um novo remoto publico.
