"""
    workflow_manager.py
    (c) 2025 David Merrell

    Definition of AbstractManager, an abstract base class
    representing a workflow manager; an object that executes 
    a DAG of Tasks.

    It accesses its DAG via a root Task (`end_task`) 
    which represents the "final" task in the DAG.

    It executes the workflow via the `run()` method,
    and interrupts it via `interrupt()`

    It also has some convenience methods for assessing
    and validating the workflow. 
"""

from abc import ABC, abstractmethod
from collections import defaultdict

from dagger.abstract.helpers import cycle_exists, construct_adj_list 
from dagger.abstract import TaskState 

class AbstractManager(ABC):
    """
    Class representing a workflow manager -- an object
    that executes a DAG of Tasks.

    It accesses its DAG via a Task (`end_task`) 
    representing the "final" task in the DAG.

    It executes the workflow via the `run()` method.
    In the abstract, this runs a graph traversal algorithm
    that launches jobs while respecting DAG dependencies.

    It also has some convenience methods for assessing 
    the state of the workflow.
    """

    def __init__(self, end_task):

        # Validate that this is a DAG
        self.end_task = end_task
        self.validate_dag()

        # Construct and store an adjacency list
        # representation of the DAG
        adj_list = construct_adj_list(end_task)
        self.adj_list = adj_list

        # These data structures capture the state of
        # the DAG execution algorithm (i.e., `run`).
        self.waiting = set()
        self.ready = []
        self.running = []
        self.failed = set()
        self.complete = set()

    def validate_dag(self):
        """
        Check that the tasks + dependencies
        actually form a DAG. I.e., there
        are no circular dependencies.
        """
        if cycle_exists(self.end_task):
            raise ValueError("Workflow ending at {self.end_task.identifier} contains a cycle!")

    def run(self):
        """
        Execute the DAG of tasks terminating at `end_task`.

        Runs a loop with the following logic:
        
        while there are running tasks:
        (a) check for any running tasks that have 'finished'
        (b) if there are any, update the 'ready' tasks
        (c) launch a subset of the 'ready' tasks
        """
        try:
            # Initialize workflow state, and
            # then start launching ready tasks
            self.initialize_workflow_state()
            self._launch_ready_tasks(self.ready)

            # While there are 'running' tasks...
            while self.running:
                # ...check if any of the running tasks are finished
                finished_tasks = self._get_finished_tasks(self.running)
                if finished_tasks:
                    # If they are, then wrap them up. 
                    self._wrapup_finished_tasks(finished_tasks)
                    
                    # Update the ready tasks
                    self._update_ready_tasks(finished_tasks)
                    # Launch some more tasks
                    self._launch_ready_tasks(self.ready)

        except KeyboardInterrupt:
            print("Workflow interrupted by user. Killing tasks and cleaning up. Please wait")
            # ...check if any of the running tasks are finished
            self._interrupt()
            raise KeyboardInterrupt

    def initialize_workflow_state(self, verify_tasks=True):
        """
        Traverse the workflow's DAG, 
        assess the state of its tasks, and 
        update its `waiting`, `complete`,`failed`, `ready` attributes.

        Some logic to enforce:
        * All tasks must be in exactly one of `waiting`, `complete`, or `failed`
            - When in doubt, a task should be in `waiting`
        * All tasks downstream of a waiting task or failed task should themselves be waiting
        * A task is `ready` iff all of its dependencies are complete 
            - `ready` must be a subset of `waiting`
        """
        # Set the state collections to empty
        self.waiting = set()
        self.ready = []
        self.running = []
        self.complete = set()
        self.failed = set()

        # Traverse the DAG "backwards", starting at `end_task`.
        # Update the collections according to the logic given above.
        self._rec_initialize_state(self.end_task, set(), verify_tasks=verify_tasks)
        return

    def _rec_initialize_state(self, task, visited, verify_tasks=True):

        # Initialize state for all dependencies
        # (that haven't been visited yet)
        for d in task.dependencies:
            if d not in visited:
                self._rec_initialize_state(d, visited, verify_tasks=verify_tasks)

        # If any dependencies are WAITING or FAILED,
        # then this task is WAITING.
        if any((((d in self.waiting) or (d in self.failed) or (d in self.ready)) for d in task.dependencies)):
            task.update_state(TaskState.WAITING)
            self.waiting.add(task)
        # Otherwise, assign the task to the appropriate set
        else:
            # Shallow check whether the task is FAILED
            if task.state == TaskState.FAILED:
                self.failed.add(task)
            # At this point, the task is putatively COMPLETE
            else:
                if verify_tasks: # Verify whether it's actually complete
                    if task.verify_complete():
                        self.complete.add(task)
                    else:
                        task.update_state(TaskState.WAITING)
                        self.ready.append(task)
                else: # Just a shallow check of this task's state
                    if task.state == TaskState.COMPLETE:
                        self.complete.add(task)
                    else:
                        task.update_state(TaskState.WAITING)
                        self.ready.append(task)

        # Mark this task as visited
        visited.add(task)
        return

    def _get_finished_tasks(self, running_tasks):
        """
        Given a list of running tasks, return a list
        of those that are COMPLETE or FAILED.
        """
        finished = [t for t in running_tasks \
                    if self._get_running_task_state(t) in (TaskState.COMPLETE, 
                                                           TaskState.FAILED)]
        return finished

    def _wrapup_finished_tasks(self, finished_tasks):
        """
        Given a list of finished tasks, wrap them up
        and put them in the appropriate set (COMPLETE or FAILED).

        Transitions: 'running' -> 'complete' OR 'failed'
        """
        # Ensure the finished tasks' states
        # are up-to-date.
        for t in finished_tasks:
            # Sync up the task's state
            t_s = self._get_running_task_state(t)
            t.update_state(t_s)
            if t_s == TaskState.COMPLETE:
                self.complete.add(t)
            elif t_s == TaskState.FAILED:
                self.failed.add(t)
            else:
                t.update_state(TaskState.WAITING)
                self.waiting.add(t)
                raise ValueError("Task {t} with state {t_s} is neither COMPLETE nor FAILED.")

            # Perform any additional wrapup as necessary.
            self._wrapup_task(t)

            # Remove the task from running
            self.running.remove(t)
        return

    def _update_ready_tasks(self, finished_tasks):
        """
        Given a list of recently finished tasks, update
        the list of 'ready' tasks with their children

        transitions: 'waiting' -> 'ready'
        """
        # Whenever a finished task has a child whose
        # parents are all COMPLETE, then move the
        # child to the `ready` list.
        children = set()
        for ft in finished_tasks:
            children |= set(self.adj_list[ft])

        for c in list(children):
            parents = c.dependencies
            if all((p in self.complete for p in parents)):
                self.ready.append(c)
                self.waiting.remove(c)
        
    def _launch_ready_tasks(self, ready_tasks):
        """
        Given a list of ready tasks, choose
        a subset of them and launch them.

        transitions: 'ready' -> 'running'
        """
        chosen_tasks = self._choose_tasks(ready_tasks)
        for task in chosen_tasks:
            self.ready.remove(task)
            self._launch_task(task)
            self.running.append(task)

    @abstractmethod
    def _launch_task(self, task):
        """
        Launch a task. For nontrivial settings
        this involves some setup (e.g., creating machinery for 
        communication between processes)
        """
        raise NotImplementedError("Subclasses of `AbstractManager` must implement `_launch_task()`")

    @abstractmethod
    def _wrapup_task(self, task):
        """
        Wrap up a task that has finished.
        This may involve tearing down or updating some 
        implementation-specific auxiliary data associated with the task.

        It need not update the task's `state`; that's done in another 
        function that wraps this one (`wrapup_finished_tasks`).
        """
        raise NotImplementedError("Subclasses of `AbstractManager` must implement `_wrapup_task()`")
        
    @abstractmethod
    def _get_running_task_state(self, task):
        """
        Get the state of a running task.
        For nontrivial settings, this involves
        using Communicators etc.
        """
        raise NotImplementedError("Subclasses of `AbstractManager` must implement `_get_running_task_state()`")

    @abstractmethod
    def _choose_tasks(self, ready_tasks):
        """
        Given a list of ready tasks, choose a subset of them 
        to launch next.
        """
        raise NotImplementedError("Subclasses of AbstractManager must define `_choose_tasks()`")

    @abstractmethod
    def _interrupt(self):
        """
        Interrupt all RUNNING tasks in the DAG.
        """
        raise NotImplementedError("Subclasses of AbstractManager must implement `interrupt`")

