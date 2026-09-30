import os
import time
import pickle
import parsl
from parsl.app.app import python_app
from pac_man.config import initialize_parsl

# ==============================================================================
# Mock Pipeline State
# ==============================================================================

def save_context(context: dict, filepath: str):
    """Simulates cli.h_save() by serializing the Context to disk."""
    import pickle
    with open(filepath, 'wb') as f:
        pickle.dump(context, f)
    return filepath

# ==============================================================================
# Parsl Asynchronous Apps (The Detached Operations)
# ==============================================================================

@python_app
def async_execute_stage(stage_name: str, duration: int):
    """
    Simulates the actual crunching of data (prepare & analyse).
    This runs on the Condor cluster.
    """
    import time
    print(f"[{stage_name}] EXECUTING: Crunching data for {duration} seconds on Condor...")
    time.sleep(duration)
    return f"Result payload for {stage_name}"

@python_app
def async_qa_calculation(context_path: str, stage_name: str):
    """
    Simulates QA score calculation. Reads the checkpointed context from disk.
    This runs on a local thread while the next stage is executing.
    """
    import time
    # Because MockContext is defined in __main__, we need to define/import it here or use pickle safely.
    # For this mock, we'll just rely on pickle to deserialize the object automatically.
    import pickle
    print(f"[{stage_name}] QA: Loading context from {context_path}...")
    # Simulate I/O overhead of loading context
    time.sleep(0.5) 
    with open(context_path, 'rb') as f:
        context = pickle.load(f)
    print(f"[{stage_name}] QA: Calculating scores for {len(context['results'])} results...")
    time.sleep(1) # Simulate QA calculation
    return f"QA Score for {stage_name}: 1.0"

@python_app
def async_weblog_render(context_path: str, qa_future, stage_name: str):
    """
    Simulates weblog rendering. Depends on QA finishing first.
    Reads the checkpointed context from disk.
    """
    import time
    print(f"[{stage_name}] WEBLOG: Waiting for QA, then rendering page...")
    time.sleep(2) # Simulate slow CASA weblog rendering
    return f"Rendered HTML for {stage_name} (QA was: {qa_future})"

# ==============================================================================
# The Workflow Orchestrator
# ==============================================================================

def run_pipeline_workflow():
    """
    Simulates executeppr.py / recipereducer.py using the "Checkpoint & Detach" pattern.
    """
    print("\n--- Starting PAC-MAN Workflow Orchestrator ---")
    
    # In reality, this would be read from the PPR XML
    recipe = [
        ("hifv_importdata", 2),
        ("hifv_hanning", 3),
        ("hifv_flagdata", 4),
        ("hifv_vlasetjy", 2)
    ]
    
    global_context = {
        "results": [],
        "stage_counter": 0
    }
    
    # Keep track of the background rendering futures so we can wait at the very end
    background_tasks = []
    
    for stage_name, duration in recipe:
        global_context["stage_counter"] += 1
        print(f"\n=== Starting Stage {global_context['stage_counter']}: {stage_name} ===")
        
        # 1. Execute the Stage synchronously (blocks until Condor returns)
        # In reality, this would be the Tier0 mapping across MSes
        result_future = async_execute_stage(stage_name, duration)
        result = result_future.result() # Wait for data processing to finish
        print(f"[{stage_name}] DONE EXECUTING. Merging result into global context.")
        
        # 2. Update the Global Context locally
        global_context["results"].append(result)
        
        # 3. Serialize a Checkpoint to the shared file system
        checkpoint_path = f"pipeline-stage-{global_context['stage_counter']}.context"
        save_context(global_context, checkpoint_path)
        print(f"[{stage_name}] Checkpoint saved to {checkpoint_path}")
        
        # 4. Detach QA and Weblog rendering!
        # These are fired into the background and we DO NOT wait for them.
        qa_future = async_qa_calculation(checkpoint_path, stage_name)
        render_future = async_weblog_render(checkpoint_path, qa_future, stage_name)
        
        background_tasks.append(render_future)
        
        print(f"[{stage_name}] Moving immediately to next stage while QA/Weblog run in background...")
        
    print("\n=== Main Recipe Complete! ===")
    print("Waiting for any remaining background Weblog rendering to finish...")
    
    for task in background_tasks:
        print(f"Completed Background Task: {task.result()}")
        
    print("Pipeline Execution Fully Complete.\n")

if __name__ == "__main__":
    # Use local subprocess pool for this test (safe for CASA)
    initialize_parsl(backend="subprocess")
    run_pipeline_workflow()
