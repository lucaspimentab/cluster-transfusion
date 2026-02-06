# Relatorio completo - clusters fenotipicos (TS2Vec, k=3, full replace, w48)

Data de geracao: 2026-01-24

## 1. Objetivo
Identificar subgrupos fenotipicos entre pacientes transfundidos (CH) no periodo pre-T0 (48h) e avaliar desfechos comparando transfundidos vs controles pareados.

## 2. Dados e coorte
- Fonte principal: timegrid em parquet (bins de 5 min) com sinais/labs e colunas de transfusao, mais outcomes por internacao.
- T0 (t0_transf): primeiro tempo de transfusao detectado na timegrid.
- Total de stays no cohort: 40167.
- Transfundidos no cohort: 10639.

## 3. Pipeline (como os resultados foram gerados)
1) Step0 - outcomes_cohort: cria flag transfused e t0_transf por stay (primeira transfusao).
2) Step1 - baseline_features: agrega variaveis numericas no pre-T0 (janela 48h), com medias/medianas/min/max/std e deltas/slope; gera t0_table e baseline_features.parquet.
3) Step2 - matching: propensity score matching (PSM), caliper=0.3, ratio=1:1, replace=True (full replace).
4) Step4 - TS2Vec: embeddings das series temporais pre-T0 dos transfundidos; janela 48h; passo temporal inferido 5 min; selecao de features por missing<=0.8 (439 candidatas, 400 selecionadas). Treino TS2Vec em CPU com 5 epocas.
5) Step5 - clusters e relatorios: KMeans nos embeddings padronizados; k em [2..6]; limiar de silhouette=0.10; gera cluster_benefit e cluster_phenotype.

## 4. Matching (full replace, w48)
- Pares pareados: 10009 (transfundidos pareados: 10009; controles unicos: 3158).
- Qualidade do balanceamento (abs SMD pos-matching): media 0.060, mediana 0.026, max 0.501.
- % covariaveis com abs SMD <=0.10: 0.788; <=0.20: 0.942.

### 4.1 Pseudo-T0 dos controles (validacao)
O pseudo-T0 dos controles e definido como o **offset mediano** entre min_time e t0 dos transfundidos.
Resultados (w48):
- Transfundidos: offset mediano = 490 min (~8.2h), p10=45 min, p90=4355 min.
- Controles: offset fixo = 490 min (por definicao).
- Nenhum controle foi "clampado" em t0=max_time; todos tinham LOS >= 490 min.
Implicacao: os controles sao elegiveis no T0, mas o T0 nao e risk-set (usa a mediana, nao a distribuicao completa).

### 4.2 Elegibilidade e cobertura do pre-window (anti-immortal time)
Com janela de 48h e passo de 5 min, o esperado seria ~576 bins por paciente. Observado:
- Transfundidos: mediana n_rows_pre = 99 (~8.2h); apenas 16% com janela completa.
- Controles: mediana n_rows_pre = 99; 0% com janela completa.
Ou seja, a janela efetiva e curta para a maioria dos pacientes porque o T0 ocorre cedo.
Para reduzir vies temporal, considere criterios de elegibilidade como:
- exigir t0 - min_time >= 48h (janela completa), ou
- exigir n_rows_pre >= 80% da janela (>= 461 bins).

## 5. Clustering TS2Vec (k=3)
- Silhouette (k=3): 0.318 (n_samples=10639).
- Tamanhos por cluster (transfundidos):
  - Matched (apenas transfundidos pareados):

| cluster | n_transfused | n_control | n_pairs |
| --- | --- | --- | --- |
| 0 | 5365 | 2099 | 5365 |
| 1 | 10 | 10 | 10 |
| 2 | 4634 | 1757 | 4634 |

  - Full (todos transfundidos com embedding):

| cluster | n_transfused |
| --- | --- |
| 0 | 5789 |
| 1 | 10 |
| 2 | 4840 |

Observacao: o cluster 1 tem n=10 e nao e interpretavel clinicamente. O relatorio abaixo foca clusters 0 e 2.

## 6. Desfechos por cluster (transfundido - controle)
Valores de diff_mean; sinal negativo = menor no transfundido.

### Cluster 0 - desfechos
| metric | diff_mean |
| --- | --- |
| mortality_anytime | -0.128 |
| vm_time_hours | 32.790 |
| ventilation_hours | 21.229 |
| icu_los_hours | 184.347 |
| rrt_on | 0.022 |
| any_vasopressor | 0.114 |
| nee_mcgkgmin_max | 0.074 |
| sofa_delta | 0.329 |

### Cluster 2 - desfechos
| metric | diff_mean |
| --- | --- |
| mortality_anytime | 0.016 |
| vm_time_hours | 81.972 |
| ventilation_hours | 43.233 |
| icu_los_hours | 459.101 |
| rrt_on | 0.058 |
| any_vasopressor | -0.120 |
| nee_mcgkgmin_max | 0.054 |
| sofa_delta | -0.289 |

## 7. Fenotipo (pre-T0) - Cluster 0
Principais variaveis clinicas (>=10% vs media geral dos transfundidos k=3):
| feature | cluster_mean | overall_mean | pct_diff |
| --- | --- | --- | --- |
| rrt_on_mean | 0.007 | 0.019 | -0.624 |
| epinephrine_rate_mcgkgmin_mean | 0.001 | 0.001 | 0.578 |
| any_vasopressor_mean | 0.031 | 0.024 | 0.315 |
| glucose_vital_mean | 249.612 | 337.078 | -0.259 |
| vasopressin_rate_unitsmin_or_equiv_mean | 0.048 | 0.039 | 0.223 |
| norepinephrine_rate_mcgkgmin_mean | 0.005 | 0.005 | 0.169 |
| urea_mean | 19.931 | 23.735 | -0.160 |
| lactate_mean | 2.158 | 1.889 | 0.142 |
| creatinine_mean | 1.116 | 1.284 | -0.130 |
| nee_mcgkgmin_mean | 0.004 | 0.004 | 0.121 |
| bilirubin_total_mean | 1.114 | 1.267 | -0.120 |

Comorbidades (media = prevalencia; >=10% vs geral):
| feature | cluster_mean | overall_mean | pct_diff |
| --- | --- | --- | --- |
| dementia_mean | 0.018 | 0.023 | -0.243 |
| peptic_ulcer_disease_mean | 0.045 | 0.059 | -0.240 |
| cerebrovascular_disease_mean | 0.099 | 0.117 | -0.152 |
| peripheral_vascular_disease_mean | 0.177 | 0.157 | 0.125 |
| aids_mean | 0.004 | 0.005 | -0.118 |
| congestive_heart_failure_mean | 0.244 | 0.276 | -0.115 |
| metastatic_solid_tumor_mean | 0.059 | 0.066 | -0.111 |
| renal_disease_mean | 0.206 | 0.232 | -0.109 |

Sangue (RBC) - medias/medianas:
| feature | cluster_mean | overall_mean | pct_diff |
| --- | --- | --- | --- |
| rbc_totalamount_ml_icu_mean | 34.880 | 33.901 | 0.029 |
| rbc_totalamount_ml_icu_median | 5.775 | 14.335 | -0.597 |
| rbc_amount_ml_event_mean | 34.880 | 33.901 | 0.029 |
| rbc_amount_ml_event_median | 5.775 | 14.335 | -0.597 |

## 8. Fenotipo (pre-T0) - Cluster 2
Principais variaveis clinicas (>=10% vs media geral dos transfundidos k=3):
| feature | cluster_mean | overall_mean | pct_diff |
| --- | --- | --- | --- |
| rrt_on_mean | 0.033 | 0.019 | 0.745 |
| epinephrine_rate_mcgkgmin_mean | 0.000 | 0.001 | -0.689 |
| any_vasopressor_mean | 0.015 | 0.024 | -0.375 |
| glucose_vital_mean | 442.114 | 337.078 | 0.312 |
| vasopressin_rate_unitsmin_or_equiv_mean | 0.029 | 0.039 | -0.265 |
| norepinephrine_rate_mcgkgmin_mean | 0.004 | 0.005 | -0.202 |
| urea_mean | 28.243 | 23.735 | 0.190 |
| lactate_mean | 1.568 | 1.889 | -0.170 |
| creatinine_mean | 1.476 | 1.284 | 0.150 |
| bilirubin_total_mean | 1.450 | 1.267 | 0.145 |
| nee_mcgkgmin_mean | 0.003 | 0.004 | -0.144 |

Comorbidades (media = prevalencia; >=10% vs geral):
| feature | cluster_mean | overall_mean | pct_diff |
| --- | --- | --- | --- |
| peptic_ulcer_disease_mean | 0.076 | 0.059 | 0.289 |
| dementia_mean | 0.030 | 0.023 | 0.284 |
| cerebrovascular_disease_mean | 0.138 | 0.117 | 0.180 |
| peripheral_vascular_disease_mean | 0.134 | 0.157 | -0.148 |
| aids_mean | 0.005 | 0.005 | 0.143 |
| congestive_heart_failure_mean | 0.314 | 0.276 | 0.137 |
| renal_disease_mean | 0.262 | 0.232 | 0.132 |
| metastatic_solid_tumor_mean | 0.075 | 0.066 | 0.129 |
| severe_liver_disease_mean | 0.108 | 0.097 | 0.119 |

Sangue (RBC) - medias/medianas:
| feature | cluster_mean | overall_mean | pct_diff |
| --- | --- | --- | --- |
| rbc_totalamount_ml_icu_mean | 32.778 | 33.901 | -0.033 |
| rbc_totalamount_ml_icu_median | 24.603 | 14.335 | 0.716 |
| rbc_amount_ml_event_mean | 32.778 | 33.901 | -0.033 |
| rbc_amount_ml_event_median | 24.603 | 14.335 | 0.716 |

## 8A. Volumes RBC (confirmacao de unidade e definicao)
As colunas no timegrid sao em **mL**:
- rbc_amount_ml_event: volume do evento (mediana de valores >0 = 350 mL).
- rbc_totalamount_ml_icu: cumulativo na UTI (mediana de valores >0 = 1002 mL).
Valores muito baixos vistos nas tabelas de fenotipo (medias/medianas) ocorrem porque a agregacao e feita sobre **todos os bins**, e a maior parte e zero. Para volume clinico, use soma/maximo em bins com transfusao.
Resumo por cluster (pre-window, por stay):
| cluster | n | rbc_event_sum_mean | rbc_event_sum_median | rbc_event_count_mean | rbc_event_max_mean | rbc_totalamount_max_mean |
| --- | --- | --- | --- | --- | --- | --- |
| 0 | 5789 | 596.045 | 350.000 | 1.000 | 596.045 | 596.045 |
| 2 | 4840 | 386.157 | 350.000 | 1.000 | 386.157 | 386.157 |

## 9. Hemoglobina (pre-T0) e faixas para comparacao
As variaveis abaixo sao derivadas do baseline_features (janela 48h pre-T0):
- hemoglobin_min: menor Hb registrada nas 48h antes do T0 (proxy de limiar clinico).
- hemoglobin_pre_mean: media de Hb nas 48h antes do T0.
As proporcoes <7/<8/<9/<10 g/dL servem para comparar com faixas usadas na literatura; nao definem, por si, estrategia restritiva/liberal no seu dado.

**Hemoglobina minima (48h pre-T0)**
| cluster | n | hemoglobin_min_mean | hemoglobin_min_median | hemoglobin_min_pct_lt_7 | hemoglobin_min_pct_lt_8 | hemoglobin_min_pct_lt_9 | hemoglobin_min_pct_lt_10 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 5789 | 9.348 | 8.500 | 0.157 | 0.387 | 0.579 | 0.696 |
| 2 | 4840 | 8.817 | 7.800 | 0.281 | 0.538 | 0.696 | 0.771 |

**Hemoglobina media pre-T0**
| cluster | n | hemoglobin_pre_mean_mean | hemoglobin_pre_mean_median | hemoglobin_pre_mean_pct_lt_7 | hemoglobin_pre_mean_pct_lt_8 | hemoglobin_pre_mean_pct_lt_9 | hemoglobin_pre_mean_pct_lt_10 |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 5789 | 11.383 | 11.370 | 0.006 | 0.044 | 0.132 | 0.267 |
| 2 | 4840 | 10.249 | 9.688 | 0.016 | 0.195 | 0.382 | 0.548 |

**Hemoglobina ultima medida pre-T0 (last pre) e distancia ate T0**
Aqui, hemoglobin_last_pre coincide com hemoglobin_at_t0 (mediana de distancia = 0 min), o que indica que ha medida de Hb no mesmo bin temporal do T0.
| cluster | n | hb_last_pre_mean | hb_last_pre_median | hb_last_pre_pct_lt_7 | hb_last_pre_pct_lt_8 | hb_last_pre_pct_lt_9 | hb_last_pre_pct_lt_10 | minutes_from_last_hb_to_t0_mean | minutes_from_last_hb_to_t0_median |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 5789 | 9.715 | 9.100 | 0.122 | 0.293 | 0.473 | 0.629 | 0.000 | 0.000 |
| 2 | 4840 | 8.921 | 7.900 | 0.263 | 0.514 | 0.673 | 0.758 | 0.000 | 0.000 |

## 10. Leitura clinica provavel (hipoteses)
- Cluster 0: perfil hemodinamico mais ativo (vasopressores/NEE/lactato maiores) com menor disfuncao renal/hepatica e menos RRT. Mortalidade menor, mas VM/LOS maiores.
- Cluster 2: perfil com maior disfuncao renal/hepatica (creatinina/ureia/bilirrubina maiores), mais RRT e menos uso de vasopressor; mortalidade levemente maior e grande aumento de VM/LOS.

## 11. Exames e variaveis consideradas (exemplos)
- Hemograma: hemoglobina, hematocrito, plaquetas.
- Metabolicos/renal/hepatico: lactato, creatinina, ureia, bilirrubina total/direta, glicose (lab e vital).
- Sinais vitais: FC, PA sistolica/diastolica/MBP, FR, temperatura, SpO2.
- Escalas: SOFA, APSIII, SAPSII (quando disponiveis).
- Terapias: vasopressores (norepi, epi, vasopressina), NEE, ventilacao_on, RRT_on.
- Comorbidades: ICC, doenca renal, doenca hepatica, demencia, AVC previo, neoplasias, etc. (media = prevalencia).
- Sangue: rbc_amount_ml_event_* e rbc_totalamount_ml_icu_* (pre-T0).
Obs: a lista completa de features esta no arquivo cluster_phenotype_ts2vec_k3_full_long.csv.

## 12. Contexto da literatura (resumo)
- Ensaios randomizados em UTI/septic shock (TRICC, TRISS) mostram que limiar restritivo (Hb ~7 g/dL) e pelo menos tao seguro quanto limiar liberal (Hb ~9-10 g/dL), sem beneficio consistente em mortalidade para estrategias liberais.
- Diretrizes recentes (AABB 2023) recomendam estrategia restritiva para adultos hemodinamicamente estaveis, incluindo pacientes criticos, com limiar ~7 g/dL (com excecoes clinicas).
- Esses achados sugerem cautela ao interpretar reducao de mortalidade associada a maior transfusao em subgrupos observacionais, pois pode haver confusao residual e efeito de sobrevivencia.

## 13. Limitacoes
- Observacional: matching reduz, mas nao elimina confusao residual.
- Clusters sao definidos apenas entre transfundidos; controles entram apenas na comparacao de desfechos.
- Full replace reutiliza controles; aumenta eficiencia mas pode aumentar vies/variancia.
- Hb pre-transfusao foi estimada (ultima medida pre-T0), mas nao ha protocolo explicito para rotular estrategia restritiva/liberal.
- Cluster 1 (n=10) nao interpretavel.

## 14. Como reproduzir
Comandos (exemplo):
  python scripts/run_all.py --window 48 --run_id run_cal03_replace_full_w48 --replace
Ou etapas:
  python scripts/step0_build_outcomes_cohort.py --run_id run_cal03_replace_full_w48
  python scripts/step1_build_baseline_features.py --window 48 --run_id run_cal03_replace_full_w48
  python scripts/step2_match_controls.py --window 48 --run_id run_cal03_replace_full_w48 --replace
  python scripts/step4_embed_ts2vec_temporal.py --window 48 --run_id run_cal03_replace_full_w48
  python scripts/step5_reports.py --window 48 --run_id run_cal03_replace_full_w48 --embedding ts2vec

## 15. Referencias (literatura)
- Holst LB et al. Lower versus higher hemoglobin threshold for transfusion in septic shock. N Engl J Med. 2014;371(15):1381-1391. PMID:25270275.
- Hebert PC et al. A multicenter, randomized, controlled clinical trial of transfusion requirements in critical care (TRICC). N Engl J Med. 1999;340(6):409-417. PMID:9971864.
- Red Blood Cell Transfusion: 2023 AABB International Guidelines. JAMA. 2023. PMID:37824153.