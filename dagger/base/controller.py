"""
    controller.py
    David Merrell (c) 2023
"""

from task import DAGGER_START_FLAG, DAGGER_END_FLAG

"""
    A Controller manages the execution of a workflow.
    It performs all of the job scheduling, subject to
    resource constraints (if any).

    A Controller should have the following attributes:
        * dag
        * resources
    
    A Controller must have the following methods:
        * sync_dag
        * run
"""
class Controller:

    def __init__(self, dag, resources={}):

        self.dag = dag
        self.resources = resources
        return

    """
        Controller.sync_dag()

        Traverse the DAG and partition its Tasks into
        (1) completed, (2) ready, and (3) uncompleted.
        In other words: inspect the tasks and data, and 
        figure out which tasks need to run. 
    """
    def sync_dag(self):
        dag = self.dag
        cur_node = dag.tasks[]

    def run(self):
        raise NotImplementedError(f"Need to implement `run` method for {type(self)}")


"""
    A GreedyController runs a DAG in a greedy fashion.
    That is, it maintains a list of runnable jobs and chooses
    to run the most important of them (subject to available resources.) 
"""
class GreedyController(Controller):

    def run(self):
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

