"""
    controller.py
    David Merrell (c) 2023
"""


"""
    A Controller manages the execution of a workflow.
    It performs all of the job scheduling and enforces
    resource constraints (if any).

    A Controller should have the following attributes:
        * dag
        * resources
    
    A Controller must have the following methods:
        * assess_state
        * run_workflow
"""
class Controller:

    def __init__(self, dag, resources={}):

        self.dag = dag
        self.resources = resources

        return

    def assess_state(self):
        raise NotImplementedError(f"Need to implement `assess_state` method for {type(self)}")

    def run_workflow(self):
        raise NotImplementedError(f"Need to implement `run_workflow` method for {type(self)}")

        # Run the simplest possible DAG traversal algorithm that respects resource constraints.
            # Assumption: when we call `start_task()` for a Task, then that task runs in the background.
            # Assumption: we maintain a list of running tasks
            # Assumption: we can periodically check whether tasks are completed
            # Assumption: we also maintain a list of runnable tasks (i.e., tasks whose inputs are ready)
                # We select from this list of runnable tasks whenever adequate resources are available
                # Greedily choose the largest task that fits within available_resources
                    # If the list of running tasks is empty but none of the runnable jobs fit 
                    # within resource constraints, choose from the runnable tasks but impose
                    # the DAG's resource constraints on it. (Raise a warning?)
            # Maintain a dictionary of *available_resources*. 
                # When we run a task, we deduct the corresponding resources from available_resources.
                # When the task is no longer running, we return its resources to available_resources. 
                
        # If we encounter a KeyBoardInterrupt, then kill all of the running tasks
