"""DAG integrity: imports without errors and has the expected shape. Needs Airflow installed."""

import pytest

pytest.importorskip("airflow")


def test_dag_loads_and_has_expected_tasks():
    from airflow.models import DagBag

    bag = DagBag(dag_folder="dags", include_examples=False)
    assert bag.import_errors == {}
    dag = bag.get_dag("weather_pipeline")
    assert set(dag.task_ids) == {"list_cities", "fetch", "transform_validate", "load", "report"}
    assert dag.catchup and dag.max_active_runs == 1
    assert dag.get_task("fetch").downstream_task_ids == {"transform_validate"}
