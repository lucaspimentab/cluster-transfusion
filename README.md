# Cluster-Transfusion

Repositório do estudo **Data-Driven Identification of Clinical Phenotypes and Outcomes After Red Blood Cell Transfusion in ICU Patients**.

## Artigo

**Autores:** Alicia Chaves, Leticia Ribeiro, Lucas Pimenta Braga, Luísa Barros Ribeiro Andrade, Paulo Henrique Cardoso, Samuel L.V. Miranda, Anisio Mendes Lacerda, Gisele L. Pappa, Alexandre Guimarães de Almeida Barros, Wagner Meira Jr.

**Afiliações:**
- Department of Computer Science, Federal University of Minas Gerais (UFMG)
- Department of Internal Medicine, INCT-NeuroTec-R, Federal University of Minas Gerais (UFMG)

## Resumo do trabalho

Este projeto investiga a heterogeneidade de resposta à transfusão de concentrado de hemácias em pacientes de UTI. A abordagem combina:

- representação temporal com **MiniRocket**;
- agrupamento com **K-means** para identificar fenótipos clínicos;
- pareamento causal (**PSM**) para comparar transfundidos e controles em perfis clínicos semelhantes;
- análise de subgrupos para identificar contextos de benefício e risco.

## Estrutura do repositório

```text
configs/
  lab_itemids.yaml

dataset/
  _archive/                         # código de montagem do dataset a partir do MIMIC-IV
  timegrid_features/                # dados locais privados (não versionados)
  outputs_outcomes/                 # dados locais privados (não versionados)

scripts/                            # pipeline final do artigo (MiniRocket + scan)
outputs/                            # artefatos locais (não versionados)
```

## Criação do dataset (MIMIC-IV)

O código para montar o dataset está em:

- `dataset/_archive/run_pipeline.py`
- `dataset/_archive/src/`
- `dataset/_archive/configs/`

Configurar credenciais via variáveis de ambiente:

```bash
MIMIC_DB_NAME
MIMIC_DB_USER
MIMIC_DB_PASSWORD
MIMIC_DB_HOST
MIMIC_DB_PORT
MIMIC_DB_OPTIONS
```

Execução:

```bash
cd dataset/_archive
python run_pipeline.py
```

## Pipeline final do artigo (MiniRocket + scan)

Execução única:

```bash
python scripts/run_all.py
```

Configuração fixa utilizada no estudo (`scripts/run_all.py`):

- `RUN_ID=run_cal03_replace_full_w48`
- `WINDOW=48`
- `SEED=42`
- `CALIPER=0.3`
- `RATIO=1`
- `REPLACE=True`
- `K_LIST=2,3,4,5,6`
- `EMBEDDING=minirocket`
- `run_scan_suite=True`

Etapas executadas automaticamente:

1. `step0_build_outcomes_cohort.py`
2. `step1_build_baseline_features.py`
3. `step2_match_controls.py`
4. `step3_embed_minirocket_temporal.py`
5. `step4_reports.py`

## Dados sensíveis

Este repositório público **não** inclui dados de pacientes.

- Arquivos `.parquet` e CSV sensíveis do dataset são mantidos localmente.
- Artefatos de execução em `outputs/` também permanecem locais.
