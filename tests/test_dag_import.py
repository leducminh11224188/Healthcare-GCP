"""
Unit tests for Airflow DAGs.
Ensures all DAGs can be imported without errors,
have the correct structure, and proper configurations.
"""

import os
import sys
import pytest

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestDAGImport:
    """Test that all DAGs can be imported without errors."""

    def setup_method(self):
        """Set up test fixtures."""
        os.environ["AIRFLOW_HOME"] = "/tmp/airflow"
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

    def test_dagbag_no_import_errors(self):
        """Test that DAGBag has no import errors."""
        from airflow.models import DagBag

        dag_bag = DagBag(
            dag_folder="Airflow/",
            include_examples=False,
        )
        assert len(dag_bag.import_errors) == 0, \
            f"DAG import errors: {dag_bag.import_errors}"

    def test_dagbag_has_expected_dags(self):
        """Test that all expected DAGs are loaded."""
        from airflow.models import DagBag

        dag_bag = DagBag(
            dag_folder="Airflow/",
            include_examples=False,
        )

        expected_dags = ["parent_dag", "pyspark_dag", "bigquery_dag"]
        for dag_id in expected_dags:
            assert dag_id in dag_bag.dags, \
                f"DAG '{dag_id}' not found in DagBag"


class TestParentDAG:
    """Test parent_dag structure and configuration."""

    def setup_method(self):
        os.environ["AIRFLOW_HOME"] = "/tmp/airflow"
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

    def get_dag(self):
        from airflow.models import DagBag
        dag_bag = DagBag(dag_folder="Airflow/", include_examples=False)
        return dag_bag.dags.get("parent_dag")

    def test_parent_dag_exists(self):
        dag = self.get_dag()
        assert dag is not None, "parent_dag not found"

    def test_parent_dag_has_tasks(self):
        dag = self.get_dag()
        assert len(dag.tasks) >= 2, \
            f"parent_dag should have at least 2 tasks, got {len(dag.tasks)}"

    def test_parent_dag_task_ids(self):
        dag = self.get_dag()
        task_ids = [task.task_id for task in dag.tasks]
        assert "trigger_pyspark_dag" in task_ids
        assert "trigger_bigquery_dag" in task_ids


class TestPySparkDAG:
    """Test pyspark_dag structure and configuration."""

    def setup_method(self):
        os.environ["AIRFLOW_HOME"] = "/tmp/airflow"
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

    def get_dag(self):
        from airflow.models import DagBag
        dag_bag = DagBag(dag_folder="Airflow/", include_examples=False)
        return dag_bag.dags.get("pyspark_dag")

    def test_pyspark_dag_exists(self):
        dag = self.get_dag()
        assert dag is not None, "pyspark_dag not found"

    def test_pyspark_dag_has_6_tasks(self):
        dag = self.get_dag()
        assert len(dag.tasks) == 6, \
            f"pyspark_dag should have 6 tasks, got {len(dag.tasks)}"

    def test_pyspark_dag_task_order(self):
        dag = self.get_dag()
        task_ids = [task.task_id for task in dag.tasks]
        assert "start_cluster" in task_ids
        assert "stop_cluster" in task_ids
        assert "pyspark_task_1" in task_ids
        assert "pyspark_task_4" in task_ids

    # def test_pyspark_dag_no_schedule(self):
    #     """pyspark_dag should not have its own schedule (triggered by parent)."""
    #     dag = self.get_dag()
    #     assert dag.schedule_interval is None or str(dag.timetable) == "NeverTimetable"


class TestBigQueryDAG:
    """Test bigquery_dag structure and configuration."""

    def setup_method(self):
        os.environ["AIRFLOW_HOME"] = "/tmp/airflow"
        os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"

    def get_dag(self):
        from airflow.models import DagBag
        dag_bag = DagBag(dag_folder="Airflow/", include_examples=False)
        return dag_bag.dags.get("bigquery_dag")

    def test_bigquery_dag_exists(self):
        dag = self.get_dag()
        assert dag is not None, "bigquery_dag not found"

    def test_bigquery_dag_has_3_tasks(self):
        dag = self.get_dag()
        assert len(dag.tasks) == 3, \
            f"bigquery_dag should have 3 tasks, got {len(dag.tasks)}"

    def test_bigquery_dag_task_ids(self):
        dag = self.get_dag()
        task_ids = [task.task_id for task in dag.tasks]
        assert "bronze_tables" in task_ids
        assert "silver_tables" in task_ids
        assert "gold_tables" in task_ids


class TestPythonSyntax:
    """Test that all Python files have valid syntax."""

    PYTHON_FILES = [
        "Airflow/parent_dag.py",
        "Airflow/pyspark_dag.py",
        "Airflow/bq_dag.py",
        "Ingestion Dataproc/hospitalA_postgresToLanding.py",
        "Ingestion Dataproc/hospitalB_postgresToLanding.py",
        "Ingestion Dataproc/claims.py",
        "Ingestion Dataproc/cpt_codes.py",
    ]

    @pytest.mark.parametrize("filepath", PYTHON_FILES)
    def test_python_file_syntax(self, filepath):
        """Test that each Python file compiles without syntax errors."""
        import py_compile
        try:
            py_compile.compile(filepath, doraise=True)
        except py_compile.PyCompileError as e:
            pytest.fail(f"Syntax error in {filepath}: {e}")


class TestSQLFilesExist:
    """Test that all required SQL files exist."""

    SQL_FILES = [
        "Medallion BigQuery/bronze.sql",
        "Medallion BigQuery/silver.sql",
        "Medallion BigQuery/gold.sql",
    ]

    @pytest.mark.parametrize("filepath", SQL_FILES)
    def test_sql_file_exists(self, filepath):
        """Test that each SQL file exists."""
        assert os.path.exists(filepath), f"SQL file not found: {filepath}"

    @pytest.mark.parametrize("filepath", SQL_FILES)
    def test_sql_file_not_empty(self, filepath):
        """Test that each SQL file is not empty."""
        assert os.path.getsize(filepath) > 0, f"SQL file is empty: {filepath}"
