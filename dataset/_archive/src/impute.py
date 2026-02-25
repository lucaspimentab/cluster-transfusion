from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List, Callable, Optional

import decimal
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq
from tqdm import tqdm

from .constants import STAY_BUCKET_MOD
from .io_utils import ensure_dir, partition_dirs, write_dataset_in_chunks


# =============================================================================
# Arrow/Pandas sanitation (fix Decimal->float & object->numeric issues)
# =============================================================================
def _coerce_decimal_objects_to_float(s: pd.Series) -> pd.Series:
    """
    Converte series object que pode conter decimal.Decimal/strings numéricas
    para float (ou NaN quando não convertível).
    """
    def _to_py(x):
        if x is None:
            return None
        if isinstance(x, decimal.Decimal):
            return float(x)
        return x

    s2 = s.map(_to_py)
    return pd.to_numeric(s2, errors="coerce")


def sanitize_for_arrow(df: pd.DataFrame, prefer_numeric_cols: Optional[List[str]] = None) -> pd.DataFrame:
    """
    Garante que colunas object com conteúdo numérico (ex.: Decimal) virem numéricas,
    evitando ArrowInvalid na escrita.
    - Se prefer_numeric_cols for fornecido, força tentativa nessas colunas primeiro.
    - Para todas as colunas object restantes, tenta converter para numérico somente
      se houver sinais de serem numéricas (Decimal/ints/floats/strings numéricas).
    """
    if df.empty:
        return df

    cols = list(df.columns)

    # 1) Força conversão em colunas que você sabe que deveriam ser numéricas
    if prefer_numeric_cols:
        for c in prefer_numeric_cols:
            if c in df.columns and df[c].dtype == "object":
                df[c] = _coerce_decimal_objects_to_float(df[c])

    # 2) Conversão defensiva em outras object
    for c in cols:
        if c not in df.columns:
            continue
        if df[c].dtype != "object":
            continue

        col = df[c]
        # pega amostra não-nula
        sample = col.dropna().head(50)
        if sample.empty:
            continue

        # Se encontrar Decimal ou maioria numérica, tenta converter
        has_decimal = any(isinstance(x, decimal.Decimal) for x in sample)
        if has_decimal:
            df[c] = _coerce_decimal_objects_to_float(col)
            continue

        # Heurística: se muitos itens parecem numéricos, converte
        numeric_like = 0
        for x in sample:
            if isinstance(x, (int, float, np.integer, np.floating)) and not (isinstance(x, float) and np.isnan(x)):
                numeric_like += 1
            elif isinstance(x, str):
                # tenta parse rápido
                try:
                    float(x)
                    numeric_like += 1
                except Exception:
                    pass

        if numeric_like / len(sample) >= 0.8:
            df[c] = pd.to_numeric(col, errors="coerce")

    return df


# =============================================================================
# Imputation helpers
# =============================================================================
def _ensure_partition_cols(df: pd.DataFrame, partition_cols: List[str]) -> pd.DataFrame:
    for col in partition_cols:
        if col in df.columns:
            continue
        if col == "stay_bucket" and "stay_id" in df.columns:
            df[col] = df["stay_id"] % STAY_BUCKET_MOD
        else:
            df[col] = None
    return df


def _apply_prev_state_to_first_rows(
    df: pd.DataFrame,
    prev_state: Dict[int, Dict[str, Any]],
    cols: List[str],
) -> None:
    """
    Para cada stay_id presente no df, se existir um valor anterior (prev_state),
    injeta esse valor APENAS na primeira linha do stay_id (se estiver NaN).

    Importante: isso acontece DEPOIS de calcular as masks de missing no _impute_frame,
    para que a coluna *_imputed marque corretamente o que estava faltando.
    """
    if not prev_state:
        return
    if "stay_id" not in df.columns:
        return

    # df já deve estar ordenado por stay_id,tbin antes desta chamada
    first_idx = df.drop_duplicates("stay_id", keep="first").index
    stay_to_first_idx = df.loc[first_idx, ["stay_id"]].reset_index().set_index("stay_id")["index"].to_dict()

    for stay_id, idx in stay_to_first_idx.items():
        try:
            sid = int(stay_id)
        except Exception:
            continue
        state = prev_state.get(sid)
        if not state:
            continue
        for c in cols:
            if c in df.columns and pd.isna(df.at[idx, c]):
                v = state.get(c, None)
                if v is not None:
                    df.at[idx, c] = v


def _extract_last_state(
    df: pd.DataFrame,
    cols: List[str],
) -> Dict[int, Dict[str, Any]]:
    """
    Extrai, para cada stay_id, o último valor NÃO-NaN observado em cada coluna de cols.
    Isso permite manter o forward-fill consistente entre arquivos.
    """
    if "stay_id" not in df.columns:
        return {}

    last_rows = df.drop_duplicates("stay_id", keep="last")
    out: Dict[int, Dict[str, Any]] = {}

    use_cols = [c for c in cols if c in last_rows.columns]
    if not use_cols:
        return {}

    for row in last_rows[["stay_id"] + use_cols].itertuples(index=False):
        sid = int(row[0])
        vals: Dict[str, Any] = {}
        for i, c in enumerate(use_cols, start=1):
            v = row[i]
            if pd.notna(v):
                vals[c] = v
        if vals:
            out[sid] = vals
    return out


def _reorder_with_imputed(df: pd.DataFrame, feature_cols: List[str]) -> pd.DataFrame:
    feature_set = set(feature_cols)
    cols = list(df.columns)
    used = set()
    new_cols = []

    for col in cols:
        if col in used:
            continue
        if col in feature_set:
            new_cols.append(col)
            used.add(col)
            imputed_col = f"{col}_imputed"
            if imputed_col in df.columns:
                new_cols.append(imputed_col)
                used.add(imputed_col)
        elif col.endswith("_imputed") and col[:-8] in feature_set:
            used.add(col)
            continue
        else:
            new_cols.append(col)
            used.add(col)

    for col in cols:
        if col not in used:
            new_cols.append(col)
            used.add(col)

    return df[new_cols]


def _impute_frame(
    df: pd.DataFrame,
    defaults: Dict[str, Any],
    feature_cols: List[str],
    no_ffill_cols: Optional[List[str]] = None,
    prev_state: Optional[Dict[int, Dict[str, Any]]] = None,
) -> pd.DataFrame:
    """
    Forward-fill por stay_id no tempo (right fill) + defaults iniciais.
    Mantém colunas *_imputed indicando quais valores estavam ausentes antes do preenchimento.
    """
    df = df.sort_values(["stay_id", "tbin"], kind="mergesort")
    missing_masks: Dict[str, pd.Series] = {}

    no_ffill = set(no_ffill_cols or [])

    # 1) garante colunas e registra missing antes de qualquer preenchimento
    for col in feature_cols:
        imputed_col = f"{col}_imputed"
        if imputed_col in df.columns:
            continue

        if col not in df.columns:
            df[col] = defaults.get(col, None)
            missing_masks[col] = pd.Series(True, index=df.index)
        else:
            missing_masks[col] = df[col].isna()

    # 2) aplica estado anterior APENAS na primeira linha do stay (se existir)
    ffill_cols = [col for col in feature_cols if col not in no_ffill]
    if prev_state and ffill_cols:
        _apply_prev_state_to_first_rows(df, prev_state, ffill_cols)

    # 3) forward-fill (right fill) por stay_id
    if ffill_cols:
        df[ffill_cols] = df.groupby("stay_id", sort=False)[ffill_cols].ffill()

    # 4) defaults iniciais quando não há histórico
    for col, default in defaults.items():
        if col not in feature_cols:
            continue
        if col not in df.columns:
            df[col] = default
            continue
        if default is None:
            continue

        df[col] = df[col].fillna(default)

    # 5) flags *_imputed
    for col, mask in missing_masks.items():
        df[f"{col}_imputed"] = mask.astype("int8")

    df = _reorder_with_imputed(df, feature_cols)
    return df


def _iter_parquet_files(path: Path) -> List[Path]:
    return sorted(path.rglob("*.parquet"))


# =============================================================================
# Main API
# =============================================================================
def impute_dataset(
    input_path: Path,
    output_path: Path,
    defaults: Dict[str, Any],
    feature_cols: List[str],
    partition_cols: List[str],
    compression: str,
    max_rows_per_file: int,
    max_rows_per_group: int,
    postprocess_fn: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None,
    no_ffill_cols: Optional[List[str]] = None,
    prefer_numeric_cols: Optional[List[str]] = None,
) -> None:
    """
    Imputa um dataset parquet (possivelmente particionado) SEM carregar tudo na memória.
    - Se houver partições (ex.: stay_bucket=...), processa partição por partição.
    - Dentro de cada partição, processa arquivo por arquivo e mantém um estado de ffill por stay_id.

    Fix robusto: antes de converter para Arrow, sanitiza colunas object (Decimal) -> float.
    """
    ensure_dir(output_path)

    partitions = partition_dirs(input_path)

    no_ffill = set(no_ffill_cols or [])
    ffill_cols = [c for c in feature_cols if c not in no_ffill]

    # Se você não passar prefer_numeric_cols, usamos feature_cols como "preferidas"
    if prefer_numeric_cols is None:
        prefer_numeric_cols = list(feature_cols)

    if partitions:
        for part in tqdm(partitions, desc="impute_partitions"):
            parquet_files = _iter_parquet_files(Path(part))
            if not parquet_files:
                continue

            prev_state: Dict[int, Dict[str, Any]] = {}

            for pf in tqdm(parquet_files, desc=f"files_{Path(part).name}", leave=False):
                table = pq.read_table(pf)
                df = table.to_pandas()

                df = _ensure_partition_cols(df, partition_cols)
                df = _impute_frame(
                    df,
                    defaults,
                    feature_cols,
                    no_ffill_cols=no_ffill_cols,
                    prev_state=prev_state,
                )

                if postprocess_fn is not None:
                    df = postprocess_fn(df)

                # Atualiza estado para continuar ffill no próximo arquivo do mesmo bucket
                df_sorted = df.sort_values(["stay_id", "tbin"], kind="mergesort")
                prev_state = _extract_last_state(df_sorted, ffill_cols)

                # --- FIX: sanitize antes de Arrow ---
                df = sanitize_for_arrow(df, prefer_numeric_cols=prefer_numeric_cols)

                out_table = pa.Table.from_pandas(df, preserve_index=False)
                write_dataset_in_chunks(
                    out_table.to_batches(),
                    output_path,
                    partition_cols=partition_cols,
                    compression=compression,
                    max_rows_per_file=max_rows_per_file,
                    max_rows_per_group=max_rows_per_group,
                )
        return

    # Caso não particionado
    parquet_files = _iter_parquet_files(input_path)
    if not parquet_files:
        # fallback: lê tudo (só para casos pequenos)
        ds_in = ds.dataset(str(input_path), format="parquet")
        table = ds_in.to_table()
        df = table.to_pandas()

        df = _ensure_partition_cols(df, partition_cols)
        df = _impute_frame(df, defaults, feature_cols, no_ffill_cols=no_ffill_cols)

        if postprocess_fn is not None:
            df = postprocess_fn(df)

        df = sanitize_for_arrow(df, prefer_numeric_cols=prefer_numeric_cols)

        out_table = pa.Table.from_pandas(df, preserve_index=False)
        write_dataset_in_chunks(
            out_table.to_batches(),
            output_path,
            partition_cols=partition_cols,
            compression=compression,
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
        )
        return

    prev_state: Dict[int, Dict[str, Any]] = {}
    for pf in tqdm(parquet_files, desc="impute_files"):
        table = pq.read_table(pf)
        df = table.to_pandas()

        df = _ensure_partition_cols(df, partition_cols)
        df = _impute_frame(
            df,
            defaults,
            feature_cols,
            no_ffill_cols=no_ffill_cols,
            prev_state=prev_state,
        )

        if postprocess_fn is not None:
            df = postprocess_fn(df)

        df_sorted = df.sort_values(["stay_id", "tbin"], kind="mergesort")
        prev_state = _extract_last_state(df_sorted, ffill_cols)

        # --- FIX: sanitize antes de Arrow ---
        df = sanitize_for_arrow(df, prefer_numeric_cols=prefer_numeric_cols)

        out_table = pa.Table.from_pandas(df, preserve_index=False)
        write_dataset_in_chunks(
            out_table.to_batches(),
            output_path,
            partition_cols=partition_cols,
            compression=compression,
            max_rows_per_file=max_rows_per_file,
            max_rows_per_group=max_rows_per_group,
        )
