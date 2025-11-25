"""
    simple/workflow_manager.py
    (c) David Merrell 2025

    Perhaps the simplest possible implementation
    of dagger's AbstractManager abstract base class.

    This isn't meant to be used in practice. Rather,
    it's a minimal working implementation that is used
    as an illustrative example.

    A SimpleManager simply executes its workflow
    one task at a time, without any parallelism.
    It performs a topological sort of the
    Tasks and then runs them one at a time.
"""

from dagger.abstract import AbstractManager

def _topological_sort(root_task):
    pass

class SimpleManager(AbstractManager):
    
    def run(self):
        """
        Run a workflow, one task at a time.
        """
        # Preparation: validate the DAG and enforce 
        # consistency between Tasks' states.
        self.validate_dag()
        self.enforce_incomplete()
        sorted_tasks = _topological_sort(self.root_task)
        
        sorted_task_names = [t.identifier for t in sorted_tasks]
        
        # Print out the tasks to console
        print("Sorted tasks:")
        for t, tn in zip(sorted_tasks, sorted_task_names):
            print(f"{tn}\t{str(tn.state.name)}")

        # Run the tasks
        for task in sorted_tasks:
            if task.is_ready() and (task.state == TaskState.WAITING):
                print(f"Running {task.identifier}")
                task.run()
           
        print("No more runnable tasks")

        return


