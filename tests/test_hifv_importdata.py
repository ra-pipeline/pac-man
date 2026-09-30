import pytest
from pac_man.config import initialize_parsl
from pac_man.executor import PipelineExecutorPlugin
import parsl

# We use the global setup_parsl fixture from conftest.py

def hifv_importdata_subtask(vis: str, session: str):
    """
    Mock tier0 subtask for importing a single ASDM/MS.
    This function runs in isolation on a worker node.
    """
    import time
    
    # Simulate the heavy IO of reading an ASDM and converting to MS
    time.sleep(1)
    
    # Return the metadata 'delta' that the main pipeline Context needs to absorb
    return {
        "vis": vis,
        "session": session,
        "status": "imported",
        "mock_metadata": {
            "num_antennas": 27,
            "spws": [0, 1, 2, 3],
            "file_size_gb": 1.2
        }
    }

def test_hifv_importdata_parallel():
    plugin = PipelineExecutorPlugin()
    
    # Suppose the PPR asks us to import 4 separate observation files.
    # These are the arguments we will pass to the subtask.
    asdm_files = [
        ("uid___A002_X123456_X001", "session_1"),
        ("uid___A002_X123456_X002", "session_1"),
        ("uid___A002_X123456_X003", "session_2"),
        ("uid___A002_X123456_X004", "session_2"),
    ]
    
    # Map the subtasks asynchronously.
    # Note: On HTCondor we would pass `resource_spec={'cores': 2, 'memory': 8000}`
    # but Parsl's ThreadPoolExecutor (used in this local test) does not support it.
    futures = plugin.map_subtasks(
        func=hifv_importdata_subtask, 
        args_list=asdm_files
    )
    
    # The futures return instantly. The main thread is not blocked here.
    assert len(futures) == 4
    
    # Now we block and wait for all the parallel imports to finish across the cluster
    results = plugin.gather_results(futures)
    
    # The core pipeline now takes these results and updates the main Context
    assert len(results) == 4
    
    # Let's verify the metadata made it back
    imported_vis_names = [res["vis"] for res in results]
    assert "uid___A002_X123456_X001" in imported_vis_names
    
    for res in results:
        assert res["status"] == "imported"
        assert res["mock_metadata"]["num_antennas"] == 27
        print(f"Successfully imported {res['vis']} into {res['session']}")
