from pathlib import Path

from airflow.models import DAG

from config.data_foundation import Config
from pipelines.airflow.dags.rpg_data_foundation_dag import (
    dag,
    task_upload_gold_to_gcs,
)


def test_dag_imports_and_has_expected_id() -> None:
    assert isinstance(dag, DAG)
    assert dag.dag_id == "rpg_data_foundation"


def test_dag_has_expected_tasks_and_order() -> None:
    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == [
        "overture_sample",
        "osm_extract",
        "build_silver",
        "build_gold",
        "upload_gold_to_gcs",
    ]

    overture_sample = dag.get_task("overture_sample")
    osm_extract = dag.get_task("osm_extract")
    build_silver = dag.get_task("build_silver")
    build_gold = dag.get_task("build_gold")
    upload_gold_to_gcs = dag.get_task("upload_gold_to_gcs")

    assert osm_extract in overture_sample.downstream_list
    assert build_silver in osm_extract.downstream_list
    assert build_gold in build_silver.downstream_list
    assert upload_gold_to_gcs in build_gold.downstream_list


def test_tasks_use_injected_config_when_provided(tmp_path: Path) -> None:
    """When config is passed, tasks use it instead of calling get_validated_config()."""
    from unittest.mock import patch

    cfg = Config.for_test(tmp_path)
    with patch(
        "pipelines.airflow.dags.rpg_data_foundation_dag.get_validated_config"
    ) as m:
        # Tasks receive config=cfg so they must not call get_validated_config
        task_upload_gold_to_gcs(config=cfg)  # no-op when GCS sync disabled
        m.assert_not_called()
