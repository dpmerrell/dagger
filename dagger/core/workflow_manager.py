"""
    core/workflow_manager.py
    (c) David Merrell 2025

    WorkflowManager implementation that executes tasks
    with multiprocess parallelism. Relies on
    the `loky` package as a backend.
"""

from dagger.abstract import AbstractManager, AbstractCommunicator
from dagger.core.helpers import construct_edge_lists
from loky import get_reusable_executor
from multiprocessing import Manager 
import copy

class ProcessCommunicator(AbstractCommunicator):
    """
    Uses a multiprocessing `Manager.Value` to report a
    Task's state to the master process.
    """

    def __init__(self, state_value):
        """
        Create a Communicator object that stores
        a `multiprocessing.Value` representing the
        Task state in shared memory.
        """
        self.state_value = state_value
        return

    def _report_state(self, task):
        """
        Update the state in shared memory,
        using this Task's state.
        """
        self.state_value.value = task.state.value
        return


class WorkflowManager(AbstractManager):
    """
    WorkflowManager implementation that executes tasks
    with multiprocess parallelism. Relies on
    the `loky` package as a backend.
    """

    def __init__(self, root_task, resources={}, start_node="START", executor_kwargs={}):
        """
        WorkflowManager implementation that executes tasks
        with multiprocess parallelism. Relies on
        the `loky` package as a backend.
        """
        super().__init__(root_task)
        self.resources = resources

        # Keep an edge-list representation of the DAG
        self.edge_lists = construct_edge_lists(root_task, start_node=start_node) 

        # Maintain collections of Tasks in each state.
        self.waiting = set()
        self.completed = set([start_node])
        self.failed = set()

        # 'Running' tasks are a special case, since
        # they're actually running in separate processes and
        # communication is nontrivial.
        self.running = {}

        # An `executor` object launches processes;
        # a `Manager` object enables shared state between them.
        self.executor = get_reusable_executor(**executor_kwargs)
        self.manager = Manager() 
        return

    def launch_task(self, task):
        """
        Execute a Task in a separate process.
        Ensure its state is accessible to this process.
        """
        # Wrap the Task's state in a `Value` object
        # shared between processes. Keep it in a safe place.
        state_value = manager.Value(task.state.value)
        self.running[task] = {"state_value": state_value}

        # Make a shallow copy of the task and remove its
        # dependencies. Give it a Communicator that also
        # contains the shared state.
        task_copy = copy.copy(task)
        task_copy.dependencies = []
        task_copy.communicator = ProcessCommunicator(state_value)

        # Define a wrapper function for the task,
        # which runs it and returns its outputs.
        def run_task():
            task_copy.run()
            return task.outputs

        # Submit the wrapper function
        self.running[task]["submission"] = self.executor.submit(run_task)
        return

    def running_task_state(self, task):
        return self.running[task]["state_value"].value

    def run(self):
        """
        A loop that continually checks for available
        jobs and launches them, subject to resource
        constraints.
        """
        while len(self.waiting) > 0:
            # Check tasks in 'running' list to see if
            # any of them finished

            # If a task is COMPLETE, then 
            # move it to `completed` and check whether
            # any of its children are ready to run.
            
            # If a child is ready to run, move it to 
            # the `running` list and run it.
            # TODO figure out loky "reusable executor"
            pass

