import pytest
from pac_man.config import initialize_parsl
from pac_man.executor import PipelineExecutorPlugin
import parsl

# We use the global setup_parsl fixture from conftest.py

def dummy_task(x, y):
    return x + y

def test_executor_mapping():
    plugin = PipelineExecutorPlugin()
    
    # We want to run dummy_task(1, 2) and dummy_task(3, 4)
    args_list = [(1, 2), (3, 4)]
    
    # Map the tasks using Parsl
    futures = plugin.map_subtasks(dummy_task, args_list)
    
    # Ensure they return immediately as futures
    assert len(futures) == 2
    
    # Block and wait for actual results
    results = plugin.gather_results(futures)
    assert results == [3, 7]
