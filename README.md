# cluster-transfusion
itemids = [int(k.split("_")[1]) for k in PARAMETROS_IMPUTACAO.keys()]

query = f"""
WITH lab_counts AS (
    SELECT
        c.stay_id,
        l.itemid
    FROM coorte_uti c
    LEFT JOIN mimiciv_hosp.labevents l
           ON c.hadm_id = l.hadm_id
          AND l.itemid = ANY(ARRAY{itemids})
    GROUP BY c.stay_id, l.itemid
)
SELECT
    itemid,
    COUNT(DISTINCT stay_id) AS n_with_value
FROM lab_counts
WHERE itemid IS NOT NULL
GROUP BY itemid;
"""

df_lab_cov = pd.read_sql(query, conn)

n_total = pd.read_sql("SELECT COUNT(DISTINCT stay_id) AS n FROM coorte_uti;", conn).iloc[0,0]

df_lab_cov['n_total'] = n_total
df_lab_cov['missing_pct'] = 100 * (1 - df_lab_cov['n_with_value'] / n_total)

df_lab_cov['lab_name'] = df_lab_cov['itemid'].map({
    int(k.split("_")[1]): k for k in PARAMETROS_IMPUTACAO.keys()
})

df_missing40 = df_lab_cov[df_lab_cov['missing_pct'] > 40].sort_values("missing_pct", ascending=False)
print(df_missing40[['itemid','lab_name','missing_pct']])

