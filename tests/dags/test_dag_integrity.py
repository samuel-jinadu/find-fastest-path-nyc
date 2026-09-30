import glob
import os

import pytest
from airflow.models.dagbag import DagBag

DAG_PATH = os.path.join(
    os.path.dirname(__file__), "..", "..", "dags", "**", "*.py"
)
DAG_FILES = glob.glob(DAG_PATH, recursive=True)


@pytest.mark.parametrize("dag_file", DAG_FILES)
def test_dag_integrity(dag_file, caplog):
    """Parse each DAG file and raise any error the DagBag swallowed."""
    DagBag(dag_folder=dag_file, include_examples=False)
    for record in caplog.records:
        if record.levelname == "ERROR":
            raise record.exc_info[1]
        if "assumed to contain no DAGs" in record.message:
            pytest.fail(f"No DAGs found in {dag_file}")