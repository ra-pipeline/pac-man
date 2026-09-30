from typing import Callable, Any, List
from parsl.app.app import python_app

class PipelineExecutorPlugin:
    """
    An external abstraction layer designed to mimic a plugin interface that the 
    core NRAO `pipeline` `tier0` framework can interact with.
    
    Instead of the core pipeline orchestrating jobs, it hands functions to this 
    plugin, which wraps them in Parsl Apps and dispatches them.
    """
    
    def __init__(self):
        # We store references to dynamically created Parsl apps
        self._registered_apps = {}

    def _get_or_create_app(self, func: Callable):
        """
        Lazily wraps a standard Python function into a Parsl @python_app.
        """
        func_name = func.__name__
        if func_name not in self._registered_apps:
            # Wrap the function as a Parsl python_app
            # We can dynamically assign executors or resource specs here
            self._registered_apps[func_name] = python_app(func)
            
        return self._registered_apps[func_name]

    def map_subtasks(self, func: Callable, args_list: List[tuple], resource_spec: dict = None) -> List[Any]:
        """
        Takes a function and a list of arguments, and distributes them 
        across the active Parsl executor.
        
        Args:
            func: The Python function (e.g., tier0 casa task wrapper)
            args_list: List of argument tuples to pass to the function.
            resource_spec: Parsl dict specifying cores/memory requirements.
            
        Returns:
            A list of Parsl AppFutures.
        """
        app = self._get_or_create_app(func)
        
        futures = []
        for args in args_list:
            if resource_spec:
                future = app(*args, parsl_resource_specification=resource_spec)
            else:
                future = app(*args)
            futures.append(future)
            
        return futures

    def gather_results(self, futures: List[Any]) -> List[Any]:
        """
        Blocks until all distributed tasks have completed and returns their results.
        """
        return [f.result() for f in futures]
