import pytest

from src.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    return get_spark("tests", driver_memory="1g")
