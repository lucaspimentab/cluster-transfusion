# Cluster-Transfusion (MiniRocket)

Pipeline limpo para reproduzir os resultados finais com **MiniRocket** (janela 48h).

## Estrutura esperada

```text
dataset/
  timegrid_features/
  outputs_outcomes/
    outcomes_by_stay_full.csv
configs/
  lab_itemids.yaml
outputs/
```

## Dependências

- Python 3.10+
- duckdb
- pandas
- numpy
- pyarrow
- scikit-learn

```bash
pip install duckdb pandas numpy pyarrow scikit-learn
```

## Pipeline final (sem argumentos)

Comando único para reproduzir o fluxo atual:

```bash
python scripts/run_all.py
```

Configuração fixa no `scripts/run_all.py`:
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
5. `step4_reports.py --embedding minirocket`

## Resultado final principal

Arquivo final para discussão clínica:

`outputs/runs/run_cal03_replace_full_w48/w48/reports/relatorio_subgrupos_minirocket_k3_comorb_lab_vital_formalizado.txt`

Manifest dos arquivos finais mantidos:

`outputs/runs/run_cal03_replace_full_w48/w48/reports/MANIFEST_RESULTADOS_FINAIS.txt`

## Observações

- Fluxo e artefatos de **TS2Vec** foram removidos.
- Run antigo `run_cal03_noreplace_w48` foi removido para reduzir ruído.
