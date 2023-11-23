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
    def assess_state(self, edgesets=None):

        if edgesets is None:        
            # Compute an edge-set representation of the DAG
            edgesets = self.dag.compute_task_edgesets()

        # Sync the DAG's Datum objects
        self.dag.sync_data()

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
        
    def _restore_resources(self, available_resources, task):
        for k in available_resources.keys():
            available_resources[k] = available_resources[k] + task.resources[k] 

    def run(self):
        raise NotImplementedError(f"Need to implement `run` method for {type(self)}")


"""
    A GreedyController runs a DAG in a greedy fashion.
    That is, it maintains a list of runnable tasks and chooses
    to run the highest-priority one (subject to available resources.) 
"""
class GreedyController(Controller):

    # Function that scores the priority of a task
    # (larger values -> higher priority)
    priority_func = lambda task: 1

    # Time between iterations of main loop (in seconds)
    check_interval = 1

    """
        Run a simple DAG traversal algorithm that respects resource constraints.
    """
    def run(self):

        # Get an edge-set representation of the DAG.
        # Figure out which tasks are completed, and which are ready to run.
        edgesets = self.dag.compute_task_edgesets()
        completed_tasks, ready_tasks = self.assess_state(edgesets=edgesets)

        # Maintain a dictionary of available resources
        available_resources = self.resources.copy()

        # Maintain a set of running tasks
        running_tasks = set()

        try:
            while len(ready_tasks) > 0:

                # Find all running tasks that have finished
                just_finished = set(uid for uid in running_tasks if not self.dag.tasks[uid].is_running())
               
                # Update the controller's workflow state accordingly 
                for uid in just_finished:
                    # Restore each of their resources to the Controller's resources
                    self._restore_resources(available_resources, self.dag.tasks[uid])    
                    # Remove each of them from running_tasks
                    running_tasks.remove(uid)
                    # Add each of them to completed_tasks
                    completed_tasks.add(uid)

                # Update the set of ready_tasks.
                    # For each newly-completed task, inspect its children 
                    # and figure out which ones are ready now.
                
                # Find the highest-priority ready_tasks that fit within resource constraints (if any exist).

                # If any exist, then start them
                    # Be sure to also (1) remove them from ready_tasks and 
                    #                 (2) add them to running_tasks

                # Otherwise, end this iteration 

        except KeyBoardInterrupt:
            for task in running_tasks:
                task.kill() 




