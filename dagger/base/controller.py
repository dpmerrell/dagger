"""
    controller.py
    David Merrell (c) 2023
"""

from dagger.base.util import DAGGER_START_FLAG, DAGGER_END_FLAG
from dagger.base.util import TaskState

from collections import deque
from bisect import insort

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

        # Sync the DAG's Datum objects;
        # and then sync its Tasks' states.
        self.dag.sync_data()
        self.dag.sync_tasks()
 
        # Get the DAG's edgesets
        edgesets = self.dag.edgesets

        # Traverse the DAG from the start, in a BFS fashion
        completed = set([DAGGER_START_FLAG])
        ready = set()
        queue = deque([DAGGER_START_FLAG])
        while len(queue) > 0:

            task_uid = queue.popleft()
            child_task_uids = edgesets[task_uid]

            for child_uid in child_task_uids:
                child_task = self.dag.tasks[child_uid]

                # Check that all this child's parents are complete
                if all(p in completed for p in child_task.get_parent_task_uids()):
                    # Check that this task is complete
                    if child_task.state == TaskState.COMPLETE: 
                        completed.add(child_uid) # Add it to the `completed` set
                        queue.append(child_uid)  # Add it to the queue
                    # Otherwise, the task is *ready* to run.
                    else:
                        ready.add(child_uid)  # Add it to the `ready` set
                   
        return completed, ready 
    

    """
        Helper method that updates the available_resources
        dictionary on starting a task.
    """        
    def _deduct_resources(self, available_resources, task):
        for k in available_resources.keys():
            available_resources[k] = available_resources[k] - task.resources[k] 

    """
        Helper method that updates the available_resources
        dictionary after a task finishes.
    """        
    def _restore_resources(self, available_resources, task):
        for k in available_resources.keys():
            available_resources[k] = available_resources[k] + task.resources[k] 
    
    """
        _constrain_task_resources(task)
        
        Update the Task such that its
        requested resources respect the constraints
        imposed by the controller.
    """ 
    def _constrain_task_resources(self, available_resources, task):
        c_keys = self.resources.keys()
        for t_k, t_v in task.resources.items():
            if t_k in c_keys:
                task.resources[t_k] = min(t_v, self.resources[t_k])
        return

    """
        _check_task_resources(task)
    
        Return a bool indicating whether the 
        task's resources fit in the GreedyController's 
        available resources. 
    """ 
    def _check_task_resources(self, available_resources, task):
        c_keys = self.resources.keys()
        for t_k, t_v in task.resources.items():
            if (t_k in c_keys) and (t_v > self.resources[t_k]):
                return False
        return True


    def run(self):
        raise NotImplementedError(f"Need to implement `run` method for {type(self)}")


"""
    A GreedyController runs a DAG in a greedy fashion.
    That is, it maintains a list of runnable tasks and chooses
    to run the highest-priority ones (subject to available resources.) 
"""
class GreedyController(Controller):

    # Function that scores the priority of a task
    # (smaller values -> higher priority; may be negative)
    priority_func = lambda task: 1

    # Time between iterations of main loop (in seconds)
    loop_interval = 1

    """
        Run a simple DAG traversal algorithm that respects resource constraints.
    """
    def run(self, halt_on_failure=False):

        # Get an edge-set representation of the DAG.
        edgesets = self.dag.edgesets
        
        # Figure out which tasks have finished, and which are ready to run.
        # Attach a "priority" value to each of the ready tasks.
        complete_tasks, ready_tasks = self.assess_state(edgesets=edgesets)
        ready_tasks = sorted([(self.priority_func(t_uid), t_uid) for t_uid in ready_tasks])

        # Maintain a dictionary of available resources
        available_resources = self.resources.copy()

        # Maintain a set of running tasks
        running_tasks = set()

        try:
            # Main loop
            while len(ready_tasks) > 0:

                # Find all running tasks that have finished
                just_finished = set(uid for uid in running_tasks if not self.dag.tasks[uid].state != TaskState.RUNNING)
               
                # Update the sets of running_tasks, complete_tasks,
                # and ready tasks
                for finished_uid in just_finished:
                    finished_task = self.dag.tasks[finished_uid]

                    # Restore each of their resources to the Controller's resources
                    self._restore_resources(available_resources, finished_task)
 
                    # Remove each of them from running_tasks
                    running_tasks.remove(finished_uid)
   
                    # If the task was successful:
                    if finished_task.state == TaskState.COMPLETE:
                        # Add it to the complete_tasks 
                        complete_tasks.add(finished_uid)
                        # Inspect its children.
                        for child_uid in edgesets[finished_uid]:
                            child_task = self.dag.tasks[child_uid]
                            
                            # If all the child's parents are complete,
                            # then add it to the ready tasks!
                            if all(p in complete_tasks for p in child_task.get_parent_task_uids()):
                                insort(ready_tasks, (self.priority_func(child_task), child_uid))
    
                # Start as many ready_tasks as possible, given resource constraints.
                # We use a greedy heuristic: iterate through the ready tasks,
                # ordered by their priorities. (A knapsack algorithm would truly 
                # solve this, but at much greater computational expense.)
                started_idx = set()
                for i, (priority, ready_uid) in enumerate(ready_tasks):
                    ready_task = self.dag.tasks[ready_uid]

                    if len(running_tasks) == 0:
                        # There are no running tasks, so *all* resources
                        # should be available. In order to make any progress,
                        # at least one task should be running. However, we may
                        # need to constrain the  ready_task's resources to fit 
                        # within the total available resources.
                        self._constrain_task_resources(available_resources, ready_task)

                    # if the task's resources fit within available resources:
                    if self._check_task_resources(available_resources, ready_task):
                        # Start the task!
                        ready_task.start()
                        self._deduct_resources(available_resources, ready_task)
                        # Be sure to also (1) add ready_uid to running_tasks and 
                        #                 (2) mark it for removal from ready_tasks
                        running_tasks.add(ready_uid)
                        started_idx.add(i)

                ready_tasks = [t for t in ready_tasks if t not in started_idx]    

            # Wait for the next iteration
            sleep(self.loop_interval)    

        except KeyBoardInterrupt:
            for task in running_tasks:
                task.kill() 



