from airflow.models import DAG

from pipelines.airflow.dags.rpg_data_foundation_dag import dag


def test_dag_imports_and_has_expected_id() -> None:
    assert isinstance(dag, DAG)
    assert dag.dag_id == "rpg_data_foundation"


def test_dag_has_expected_tasks_and_order() -> None:
    task_ids = [t.task_id for t in dag.tasks]
    assert task_ids == ["overture_sample", "osm_extract", "build_silver", "build_gold", "upload_gold_to_gcs"]

    overture_sample = dag.get_task("overture_sample")
    osm_extract = dag.get_task("osm_extract")
    build_silver = dag.get_task("build_silver")
    build_gold = dag.get_task("build_gold")
    upload_gold_to_gcs = dag.get_task("upload_gold_to_gcs")

    assert osm_extract in overture_sample.downstream_list
    assert build_silver in osm_extract.downstream_list
    assert build_gold in build_silver.downstream_list
    assert upload_gold_to_gcs in build_gold.downstream_list

