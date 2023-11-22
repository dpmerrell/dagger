"""
    controller.py
    David Merrell (c) 2023
"""

from util import DAGGER_START_FLAG, DAGGER_END_FLAG
from collections import deque

"""
    A Controller manages the execution of a workflow.
    It performs all of the job scheduling, subject to
    resource constraints (if any).

    A Controller should have the following attributes:
        * dag
        * resources
    
    A Controller must have the following methods:
        * assess_state
        * run
"""
class Controller:

    def __init__(self, dag, resources={}, **kwargs):

        self.dag = dag
        self.resources = resources

        for k,v in kwargs.items():
            setattr(self, k, v)

        return

    """
        Traverse the DAG and identify its (1) completed
        and (2) ready Tasks.
        In other words: inspect the tasks and data, and 
        figure out which tasks need to run. 
    """
    def assess_state(self):

        # Sync the DAG's Datum objects
        self.dag.sync_data()

        # Compute an edge-set representation of the DAG
        edgesets = self.dag.compute_task_edgesets()

        # Traverse the DAG from the start, in a BFS fashion
        completed = set([DAGGER_START_FLAG])
        ready = set()
        queue = deque([DAGGER_START_FLAG])
        while len(queue) > 0:

            task_uid = queue.popleft()
            children = edgesets[task_uid]
            for child_uid in children:

                # Check that all this child's parents are complete
                child_task = self.dag.tasks[child_uid]
                parents_completed = all(p in completed for p in child_task.get_parent_task_uids())
                if not parents_completed:

                    # Check that all the child task's outputs are newer than its inputs, 
                    # as determined by timestamps.
                    max_input_ts = max(self.dag.data[uid].timestamp for uid in child_task.inputs.values())
                    timestamps_valid = all(self.dag.data[uid].timestamp > max_input_ts for uid in child_task.outputs.values())

                    # If timestamps check out, then the task is complete!
                    if timestamps_valid:
                        completed.add(child_uid) # Add it to the `completed` set
                        queue.append(child_uid)  # Add it to the queue
                    # If timestamps do not check out, then the task is *ready* to run.
                    else:
                        ready.add(child_uid)     # Add it to the `ready` set
                   
        return completed, ready 
        

    def run(self):
        raise NotImplementedError(f"Need to implement `run` method for {type(self)}")


"""
    A GreedyController runs a DAG in a greedy fashion.
    That is, it maintains a list of runnable tasks and chooses
    to run the highest-priority one (subject to available resources.) 
"""
class GreedyController(Controller):

    priority_func = lambda task: 1

    """
        Run the simplest possible DAG traversal algorithm that respects resource constraints.
    """
    def run(self):
        raise NotImplementedError(f"Need to implement `run` method for {type(self)}")

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


