"""
    core/workflow_manager.py
    (c) David Merrell 2025

    WorkflowManager implementation that executes tasks
    with multiprocess parallelism. Relies on
    the `loky` package as a backend.
"""

from dagger.abstract import AbstractManager, AbstractCommunicator
from dagger.abstract.helpers import resources_available, \
                                    increment_resources, \
                                    decrement_resources

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
    the `loky` reusable executor as a backend.
    
    Uses a greedy algorithm to launch tasks subject to resource 
    constraints. The user may specify a dictionary of arbitrary
    resource constraints that the running tasks must satisfy. 
    """

    def __init__(self, root_task, resources=None, executor_kwargs=None):
        """
        WorkflowManager implementation that executes tasks
        with multiprocess parallelism. Relies on
        the `loky` reusable executor as a backend.
        """
        super().__init__(root_task)
        self.resources = {} if resources is None else resources

        # 'Running' tasks are a special case, since
        # they're actually running in separate processes and
        # communication is nontrivial.
        self.running_state_values = {}
        self.running_submissions = {}

        # A loky `executor` object launches processes;
        # a `multiprocessing.Manager` object enables shared state between them.
        self._executor_kwargs = {} if executor_kwargs is None else executor_kwargs
        self.executor = get_reusable_executor(**(self._executor_kwargs))
        self.mp_manager = Manager() 
        return

    def _launch_task(self, task):
        """
        Execute a Task in a separate process.
        Ensure its state is accessible to this process.
        """
        # Wrap the Task's state in a `Value` object
        # shared between processes. Keep it in a safe place.
        state_value = self.mp_manager.Value(task.state.value)
        self.running_state_values[task] = state_value

        # Need to decrement the task's resources from the 
        # WorkflowManager's resources.
        self.resources = decrement_resources(self.resources,
                                             task.resources)

        # Make a shallow copy of the task and remove its
        # dependencies. Give it a Communicator that also
        # contains the shared state.
        task_copy = copy.copy(task)
        task_copy.dependencies = []
        task_copy.communicator = ProcessCommunicator(state_value)

        # Define a wrapper function for the task copy,
        # which runs and returns it.
        def run_task():
            task_copy.run()
            return task_copy

        # Submit the wrapper function
        self.running_submissions[task] = self.executor.submit(run_task)
        return

    def _wrapup_task(self, task):
        """
        Do the following after a Task finishes 
        (enters COMPLETED or FAILED state):
        * Finalize the Task's outputs
        * Remove the task's multiprocessing infrastructure
        * Restore its resources to the WorkflowManager.
        """
        self.running_state_values.pop(task)
        self.running_submissions.pop(task)
        # Need to restore task's resources to the manager 
        self.resources = increment_resources(self.resources,
                                             task.resources)

    def _get_running_task_state(self, task):
        t_sv = self.running_state_values[task].value
        return TaskState(t_sv)

    def _choose_tasks(self, ready_tasks):
        """
        Greedily select tasks that fit within the 
        WorkflowManager's available resources
        """
        chosen = []
        # The actual resources are decremented at
        # task launch; so we just work with a copy 
        # of the resources dictionary
        resources = copy.deepcopy(self.resources)

        # Iterate through ready tasks and naively
        # select the first ones that satisfy resource
        # constraints
        for rt in ready_tasks:
            if resources_available(resources, rt.resources):
                chosen.append(rt)
                resources = decrement_resources(resources, rt.resources)
        return chosen

    def _interrupt(self):
        """
        Shutdown/reset the loky executor 
        -and-
        Reset all state for the running tasks.
        """
        # Reset the executor
        self.executor.shutdown(wait=False, kill_workers=True)
        self.executor = get_reusable_executor(**(self._executor_kwargs))

        # Clear all state for running tasks
        self.running_task_states = {}
        self.running_submissions = {}
        for task in self.running:
            # Move each task to `WAITING`
            task.update_state(TaskState.WAITING)
            self.waiting.add(task)

            # Need to restore task's resources to the WorkflowManager
            self.resources = increment_resources(self.resources,
                                                 task.resources)

        self.running = []
        return

