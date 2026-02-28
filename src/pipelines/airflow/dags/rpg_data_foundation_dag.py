from datetime import datetime
from os import getenv
from pathlib import Path
from typing import TYPE_CHECKING

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from config.data_foundation import Config
from data.conflation import conflate_parquet, silver_to_gold
from data.osm_ingest import extract_osm_pois, fetch_osm_pois_via_overpass
from data.overture_ingest import (
    BBox,
    OvertureTheme,
    build_overture_parquet_url,
    sample_overture_by_bbox,
)
from pipelines.airflow.validation import get_validated_config
from pipelines.shared import (
    _is_cache_valid,
    find_all_theme_samples,
    get_bbox_from_config,
    get_overture_cache_path,
    get_osm_bbox_from_config,
    get_osm_cache_path,
)

if TYPE_CHECKING:
    pass  # Config used for type hints


def _default_data_dir() -> Path:
    return Path(".") / "data"


def _raw_overture_temp(cfg: Config) -> Path:
    """Temp dir for sampled Overture data: data/raw/overture/temp."""
    return cfg.local.raw / "overture" / "temp"


def _raw_osm_temp(cfg: Config) -> Path:
    """Temp dir for extracted OSM data: data/raw/osm/temp."""
    return cfg.local.raw / "osm" / "temp"


def _overture_source(cfg: Config, data_root: Path, theme: OvertureTheme) -> str:
    """Resolve Overture source from config or env for a specific theme."""
    uri_map = {
        OvertureTheme.PLACES: cfg.datasets.overture_places,
        OvertureTheme.ADMINISTRATIVE: cfg.datasets.overture_administrative,
        OvertureTheme.LAND: cfg.datasets.overture_land,
        OvertureTheme.WATER: cfg.datasets.overture_water,
    }
    uri = uri_map.get(theme, "")
    if uri:
        return uri
    release = (
        cfg.overture_release_date or getenv("RPG_OVERTURE_RELEASE_DATE", "")
    ).strip()
    if release:
        return build_overture_parquet_url(
            release,
            base_url=cfg.datasets.overture_places_base,
            theme=theme,
        )
    return str(data_root / "raw" / "overture" / f"{theme.value}.parquet")


def task_overture_sample(
    *,
    config: Config | None = None,
    theme: OvertureTheme = OvertureTheme.PLACES,
    **_context: object,
) -> None:
    """Sample Overture data for a specific theme."""
    cfg = config or get_validated_config()
    data_root = _default_data_dir()
    (data_root / "raw" / "overture").mkdir(parents=True, exist_ok=True)

    source = _overture_source(cfg, data_root, theme)
    bbox = BBox(
        minx=cfg.bbox_minx,
        maxx=cfg.bbox_maxx,
        miny=cfg.bbox_miny,
        maxy=cfg.bbox_maxy,
    )
    temp_dir = _raw_overture_temp(cfg)
    temp_dir.mkdir(parents=True, exist_ok=True)
    output = temp_dir / f"overture_{theme.value}_sample.parquet"

    # Use shared cache path
    bbox_tuple = get_bbox_from_config(cfg)
    cache_path = get_overture_cache_path(cfg, theme, bbox_tuple)

    print(f"[overture:{theme.value}] cache_path={cache_path}, exists={cache_path.exists()}", flush=True)

    # Get data (from cache or download)
    if cfg.cache_enabled and _is_cache_valid(cache_path, cfg.cache_ttl_hours):
        import shutil

        print(f"[overture:{theme.value}] Using cached data: {cache_path}", flush=True)
        data_path = cache_path
    else:
        import shutil

        print(f"[overture:{theme.value}] Downloading from source", flush=True)
        sample_overture_by_bbox(source, bbox, output, theme, limit=cfg.overture_sample_limit)

        # Cache the data if enabled
        if cfg.cache_enabled:
            import shutil
            cache_path.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(output, cache_path)
            print(f"[overture:{theme.value}] Cached to: {cache_path}", flush=True)

        print(f"[overture:{theme.value}] Wrote to output: {output}", flush=True)
        return

    # Copy from cache to output
    import shutil
    shutil.copy2(cache_path, output)
    print(f"[overture:{theme.value}] Copied from cache to output: {output}", flush=True)


# Task factories for each theme
def task_overture_places(config: Config | None = None, **_context: object) -> None:
    task_overture_sample(config=config, theme=OvertureTheme.PLACES)


def task_overture_administrative(config: Config | None = None, **_context: object) -> None:
    task_overture_sample(config=config, theme=OvertureTheme.ADMINISTRATIVE)


def task_overture_land(config: Config | None = None, **_context: object) -> None:
    task_overture_sample(config=config, theme=OvertureTheme.LAND)


def task_overture_water(config: Config | None = None, **_context: object) -> None:
    task_overture_sample(config=config, theme=OvertureTheme.WATER)


def task_fetch_osm(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    """Fetch OSM POIs via Overpass (bbox and overpass_url from config), write to raw/osm/mini_region.parquet. No-op when RPG_OSM_EXTRACT_URI is set."""
    cfg = config or get_validated_config()
    if cfg.datasets.osm_extract:
        return
    raw_osm_dir = cfg.local.raw / "osm"
    raw_osm_dir.mkdir(parents=True, exist_ok=True)
    output = raw_osm_dir / "mini_region.parquet"

    # Use shared cache path
    bbox_swne = get_osm_bbox_from_config(cfg)
    cache_path = get_osm_cache_path(cfg, bbox_swne)

    if cfg.cache_enabled and _is_cache_valid(cache_path, cfg.cache_ttl_hours):
        import shutil

        print(f"[osm] Using cached data: {cache_path}", flush=True)
        shutil.copy2(cache_path, output)
        return

    # Fetch from Overpass API
    fetch_osm_pois_via_overpass(
        bbox_swne,
        output,
        overpass_url=cfg.datasets.overpass_url,
    )

    # Save to cache
    if cfg.cache_enabled:
        import shutil

        cache_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(output, cache_path)
        print(f"[osm] Cached to: {cache_path}", flush=True)


def task_osm_extract(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    cfg = config or get_validated_config()
    raw_osm_dir = cfg.local.raw / "osm"
    raw_osm_dir.mkdir(parents=True, exist_ok=True)

    source = (
        cfg.datasets.osm_extract
        if cfg.datasets.osm_extract
        else (cfg.local.raw / "osm" / "mini_region.parquet").resolve()
    )
    if not any(
        str(source).startswith(p) for p in ("http://", "https://", "s3://", "gs://")
    ):
        source_path = Path(source)
        if not source_path.exists():
            default_relative = "data/raw/osm/mini_region.parquet"
            raise FileNotFoundError(
                f"OSM extract not found: {source_path}\n"
                "Provide an OSM Parquet file by either:\n"
                "  1. Set RPG_OSM_EXTRACT_URI (or --osm-source) to a path or URI,\n"
                f"  2. Place a file at {default_relative}"
            )
    temp_dir = _raw_osm_temp(cfg)
    temp_dir.mkdir(parents=True, exist_ok=True)
    output = temp_dir / "osm_pois.parquet"
    extract_osm_pois(Path(source), output)


def task_build_silver(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    """Build silver by merging all available Overture theme samples and conflate with OSM."""
    import pandas as pd

    cfg = config or get_validated_config()

    # Use shared function to find all theme samples
    theme_samples = find_all_theme_samples(cfg)

    if not theme_samples:
        # Fall back to legacy single file
        legacy_path = _raw_overture_temp(cfg) / "overture_sample.parquet"
        if legacy_path.exists():
            theme_samples[OvertureTheme.PLACES] = legacy_path
        else:
            print("[build_silver] No Overture sample found, skipping conflation", flush=True)
            return

    print(f"[build_silver] Found {len(theme_samples)} theme samples: {list(theme_samples.keys())}", flush=True)

    # Merge all theme dataframes
    dfs = []
    for theme, path in theme_samples.items():
        df = pd.read_parquet(path)
        df["theme"] = theme.value
        print(f"[build_silver] Loaded {len(df)} rows from {theme.value}", flush=True)
        dfs.append(df)

    # Combine all themes into one dataframe
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
    else:
        print("[build_silver] No data to conflate", flush=True)
        return

    # Write combined overture data to temp location
    overture_combined = _raw_overture_temp(cfg) / "overture_combined.parquet"
    overture_combined.parent.mkdir(parents=True, exist_ok=True)
    combined_df.to_parquet(overture_combined, index=False)
    print(f"[build_silver] Combined {len(combined_df)} rows from all themes", flush=True)

    osm_path = _raw_osm_temp(cfg) / "osm_pois.parquet"
    silver_path = cfg.local.silver / cfg.silver_venues_filename()
    conflate_parquet(
        overture_combined,
        osm_path,
        silver_path,
        radius_m=50.0,
        output_format=cfg.local_output_format,
    )


def task_build_gold(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    cfg = config or get_validated_config()
    cfg.local.gold.mkdir(parents=True, exist_ok=True)
    silver_path = cfg.local.silver / cfg.silver_venues_filename()
    gold_path = cfg.local.gold / cfg.gold_venues_filename()
    silver_to_gold(silver_path, gold_path, output_format=cfg.local_output_format)


def task_cleanup_raw_temp(
    *,
    config: Config | None = None,
    **_context: object,
) -> None:
    """Remove temp raw files (data/raw/overture/temp, data/raw/osm/temp) after pipeline. Set RPG_CLEANUP_RAW_TEMP=false to keep them."""
    if getenv("RPG_CLEANUP_RAW_TEMP", "true").lower() in {"0", "false", "no", "n"}:
        return
    cfg = config or get_validated_config()
    for temp_dir in (_raw_overture_temp(cfg), _raw_osm_temp(cfg)):
        if temp_dir.exists():
            for p in temp_dir.iterdir():
                if p.is_file():
                    p.unlink()


with DAG(
    dag_id="rpg_data_foundation",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rpg", "data-foundation"],
) as dag:
    # Overture theme tasks (places by default, plus admin/land/water)
    overture_places = PythonOperator(
        task_id="overture_places",
        python_callable=task_overture_places,
    )
    overture_administrative = PythonOperator(
        task_id="overture_administrative",
        python_callable=task_overture_administrative,
    )
    overture_land = PythonOperator(
        task_id="overture_land",
        python_callable=task_overture_land,
    )
    overture_water = PythonOperator(
        task_id="overture_water",
        python_callable=task_overture_water,
    )

    fetch_osm = PythonOperator(
        task_id="fetch_osm",
        python_callable=task_fetch_osm,
    )

    osm_extract = PythonOperator(
        task_id="osm_extract",
        python_callable=task_osm_extract,
    )

    build_silver = PythonOperator(
        task_id="build_silver",
        python_callable=task_build_silver,
    )

    build_gold = PythonOperator(
        task_id="build_gold",
        python_callable=task_build_gold,
    )

    cleanup_raw_temp = PythonOperator(
        task_id="cleanup_raw_temp",
        python_callable=task_cleanup_raw_temp,
    )

    # Overture themes run in parallel and feed into build_silver
    (overture_places >> build_silver)
    (overture_administrative >> build_silver)
    (overture_land >> build_silver)
    (overture_water >> build_silver)
    (fetch_osm >> osm_extract >> build_silver)
    (build_silver >> build_gold >> cleanup_raw_temp)
