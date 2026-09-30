import pytest
from pac_man.config import initialize_parsl
import parsl

@pytest.fixture(scope="session", autouse=True)
def setup_parsl():
    # Initialize the local subprocess pool (process isolation required for CASA)
    initialize_parsl(backend="subprocess")
    yield
    parsl.clear()


