from datetime import datetime
import numpy as np
import pandas as pd


def check_duplicates_and_nulls(df: pd.DataFrame, key_cols: list[str], value_col: str = "value") -> dict:
    """Sprawdza duplikaty po kluczu oraz braki (NaN/None) w kolumnie wartości."""
    total_rows = len(df)
    if total_rows == 0:
        return {
            "total_rows": 0,
            "duplicate_keys": 0,
            "null_values": 0,
            "null_pct": 0.0,
        }

    dup_count = int(df.duplicated(subset=key_cols).sum()) if set(key_cols).issubset(df.columns) else 0
    null_count = int(df[value_col].isna().sum()) if value_col in df.columns else 0
    null_pct = round((null_count / total_rows) * 100, 2)

    return {
        "total_rows": total_rows,
        "duplicate_keys": dup_count,
        "null_values": null_count,
        "null_pct": null_pct,
    }


def check_time_grid(
    df: pd.DataFrame,
    group_col: str,
    dt_col: str = "datetime",
    now_dt: datetime | None = None,
) -> dict:
    """Sprawdza zakres dat, liczbę grup, brakujące godziny na siatce, przyszłość oraz minuty != 0."""
    if df.empty or dt_col not in df.columns:
        return {
            "groups_cnt": 0,
            "min_dt": None,
            "max_dt": None,
            "missing_hours_total": 0,
            "future_rows": 0,
            "non_zero_minute_rows": 0,
        }

    dt_series = pd.to_datetime(df[dt_col], errors="coerce").dropna()
    if dt_series.empty:
        return {
            "groups_cnt": 0,
            "min_dt": None,
            "max_dt": None,
            "missing_hours_total": 0,
            "future_rows": 0,
            "non_zero_minute_rows": 0,
        }

    min_dt = dt_series.min()
    max_dt = dt_series.max()
    ref_now = pd.Timestamp(now_dt or datetime.now())
    future_rows = int((dt_series > ref_now).sum())
    non_zero_minute_rows = int((dt_series.dt.minute != 0).sum())

    missing_hours_total = 0
    groups_cnt = 0

    if group_col in df.columns:
        work_df = df[[group_col, dt_col]].copy()
        work_df[dt_col] = pd.to_datetime(work_df[dt_col], errors="coerce")
        work_df = work_df.dropna(subset=[dt_col])
        groups_cnt = int(work_df[group_col].nunique())

        for _, grp in work_df.groupby(group_col):
            hours = grp[dt_col].dt.floor("h").drop_duplicates()
            if len(hours) > 1:
                expected_range = pd.date_range(start=hours.min(), end=hours.max(), freq="h")
                missing_hours_total += max(0, len(expected_range) - len(hours))

    return {
        "groups_cnt": groups_cnt,
        "min_dt": min_dt,
        "max_dt": max_dt,
        "missing_hours_total": int(missing_hours_total),
        "future_rows": future_rows,
        "non_zero_minute_rows": non_zero_minute_rows,
    }


def check_value_ranges(
    df: pd.DataFrame,
    value_col: str = "value",
    min_allowed: float | None = None,
    max_allowed: float | None = None,
) -> dict:
    """Sprawdza podstawowe statystyki, kwantyle oraz przekroczenia dopuszczalnych granic."""
    if df.empty or value_col not in df.columns:
        return {
            "min_val": None,
            "median_val": None,
            "p99_val": None,
            "max_val": None,
            "below_min_cnt": 0,
            "above_max_cnt": 0,
        }

    s = pd.to_numeric(df[value_col], errors="coerce").dropna()
    if s.empty:
        return {
            "min_val": None,
            "median_val": None,
            "p99_val": None,
            "max_val": None,
            "below_min_cnt": 0,
            "above_max_cnt": 0,
        }

    below_min_cnt = int((s < min_allowed).sum()) if min_allowed is not None else 0
    above_max_cnt = int((s > max_allowed).sum()) if max_allowed is not None else 0

    return {
        "min_val": float(s.min()),
        "median_val": float(s.median()),
        "p99_val": float(np.percentile(s, 99)),
        "max_val": float(s.max()),
        "below_min_cnt": below_min_cnt,
        "above_max_cnt": above_max_cnt,
    }


def check_flatlines(
    df: pd.DataFrame,
    group_col: str,
    dt_col: str = "datetime",
    value_col: str = "value",
    min_streak: int = 6,
) -> dict:
    """
    Wykrywa ciągłe (godzina po godzinie) serie identycznych wartości pod rząd.
    Przerwa w czasie większa niż 1h przerywa ciągłość serii.
    """
    if df.empty or not {group_col, dt_col, value_col}.issubset(df.columns):
        return {"flatline_streaks_cnt": 0, "max_flatline_len": 0}

    work = df[[group_col, dt_col, value_col]].copy()
    work[dt_col] = pd.to_datetime(work[dt_col], errors="coerce")
    work[value_col] = pd.to_numeric(work[value_col], errors="coerce")
    work = work.dropna(subset=[dt_col, value_col]).sort_values([group_col, dt_col])

    streaks_cnt = 0
    max_streak_len = 0

    for _, grp in work.groupby(group_col):
        grp = grp.drop_duplicates(subset=[dt_col]).sort_values(dt_col)
        if len(grp) < 2:
            continue

        is_1h_step = grp[dt_col].diff() == pd.Timedelta(hours=1)
        is_same_val = grp[value_col].eq(grp[value_col].shift(1))
        continued_flat = is_1h_step & is_same_val

        # Nowy segment zaczyna się, gdy nie ma ciągłej równej wartości krok w krok
        segment_id = (~continued_flat).cumsum()
        # Długość serii w krokach ciągłego powtórzenia: +1 dla pierwszego elementu serii
        streak_lengths = continued_flat.groupby(segment_id).transform("sum") + 1

        qualifying = streak_lengths[continued_flat & (streak_lengths >= min_streak)]
        if not qualifying.empty:
            max_streak_len = max(max_streak_len, int(qualifying.max()))
            streaks_cnt += int(segment_id.loc[qualifying.index].nunique())

    return {
        "flatline_streaks_cnt": streaks_cnt,
        "max_flatline_len": max_streak_len,
    }


def check_meteo_columns_nulls(df: pd.DataFrame) -> dict:
    """Sprawdza łączną liczbę braków (NaN/None) we wszystkich kolumnach pogodowych."""
    meteo_cols = [
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "surface_pressure",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
        "precipitation",
        "cloud_cover",
        "boundary_layer_height",
        "shortwave_radiation",
    ]
    present_cols = [c for c in meteo_cols if c in df.columns]
    if df.empty or not present_cols:
        return {"meteo_nulls_total": 0, "meteo_cols_with_nulls": 0}

    null_per_col = df[present_cols].isna().sum()
    return {
        "meteo_nulls_total": int(null_per_col.sum()),
        "meteo_cols_with_nulls": int((null_per_col > 0).sum()),
    }


def run_data_quality_audit(
    df: pd.DataFrame,
    dataset_name: str,
    group_col: str,
    key_cols: list[str],
    dt_col: str = "datetime",
    value_col: str = "value",
    min_allowed: float | None = None,
    max_allowed: float | None = None,
    flatline_min_streak: int = 6,
    extra_checks: list[callable] | None = None,
) -> dict:
    """Główna funkcja orkiestrująca audyt jakości danych."""
    checks = [
        ("duplikaty_i_nulle", lambda d: check_duplicates_and_nulls(d, key_cols=key_cols, value_col=value_col)),
        ("siatka_czasowa", lambda d: check_time_grid(d, group_col=group_col, dt_col=dt_col)),
        ("zakres_wartosci", lambda d: check_value_ranges(d, value_col=value_col, min_allowed=min_allowed, max_allowed=max_allowed)),
        ("flatlines", lambda d: check_flatlines(d, group_col=group_col, dt_col=dt_col, value_col=value_col, min_streak=flatline_min_streak)),
    ]

    report: dict = {"dataset": dataset_name}

    for check_name, fn in checks:
        report[check_name] = fn(df)

    if extra_checks:
        for idx, extra_fn in enumerate(extra_checks):
            extra_name = getattr(extra_fn, "__name__", f"extra_check_{idx}")
            report[extra_name] = extra_fn(df)

    return report


def audit_summary_df(reports: list[dict]) -> pd.DataFrame:
    """Spłaszcza listę raportów do wygodnej tabeli w Jupyter Notebooku."""
    rows = []
    for rep in reports:
        flat = {"dataset": rep.get("dataset")}
        for section, metrics in rep.items():
            if section == "dataset" or not isinstance(metrics, dict):
                continue
            for k, v in metrics.items():
                flat[f"{section}.{k}"] = v
        rows.append(flat)
    return pd.DataFrame(rows)