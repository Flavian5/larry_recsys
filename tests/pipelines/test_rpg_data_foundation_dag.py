from pathlib import Path

from airflow.models import DAG

from config.data_foundation import Config
from pipelines.airflow.dags.rpg_data_foundation_dag import (
    dag,
    task_cleanup_raw_temp,
)


def test_dag_imports_and_has_expected_id() -> None:
    assert isinstance(dag, DAG)
    assert dag.dag_id == "rpg_data_foundation"


def test_dag_has_expected_tasks_and_order() -> None:
    task_ids = [t.task_id for t in dag.tasks]
    expected_tasks = [
        "build_gold",
        "build_silver",
        "cleanup_raw_temp",
        "fetch_osm",
        "osm_extract",
        "overture_places",
        "overture_administrative",
        "overture_land",
        "overture_water",
    ]
    assert sorted(task_ids) == sorted(expected_tasks)

    # All Overture theme tasks should feed into build_silver
    overture_places = dag.get_task("overture_places")
    overture_administrative = dag.get_task("overture_administrative")
    overture_land = dag.get_task("overture_land")
    overture_water = dag.get_task("overture_water")
    fetch_osm = dag.get_task("fetch_osm")
    osm_extract = dag.get_task("osm_extract")
    build_silver = dag.get_task("build_silver")
    build_gold = dag.get_task("build_gold")
    cleanup_raw_temp = dag.get_task("cleanup_raw_temp")

    # All Overture themes feed into build_silver
    assert build_silver in overture_places.downstream_list
    assert build_silver in overture_administrative.downstream_list
    assert build_silver in overture_land.downstream_list
    assert build_silver in overture_water.downstream_list
    # OSM pipeline
    assert osm_extract in fetch_osm.downstream_list
    assert build_silver in osm_extract.downstream_list
    assert build_gold in build_silver.downstream_list
    assert cleanup_raw_temp in build_gold.downstream_list


def test_tasks_use_injected_config_when_provided(tmp_path: Path) -> None:
    """When config is passed, tasks use it instead of calling get_validated_config()."""
    from unittest.mock import patch

    cfg = Config.for_test(tmp_path)
    with patch(
        "pipelines.airflow.dags.rpg_data_foundation_dag.get_validated_config"
    ) as m:
        task_cleanup_raw_temp(config=cfg)
        m.assert_not_called()
