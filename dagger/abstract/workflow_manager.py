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
from dagger.abstract import TaskState

class AbstractManager(ABC):
    """
    Class representing a workflow manager -- an object
    that executes a DAG of Tasks.

    It accesses its DAG via a Task (`end_task`) 
    representing the "final" task in the DAG.

    It executes the workflow via the `run()` method.
    In the abstract, this must run a graph traversal algorithm
    that launches jobs while respecting DAG dependencies.

    It also has some convenience methods for assessing 
    the state of the workflow.
    """

    def __init__(self, end_task):

        self.end_task = end_task

        # These data structures contain state for 
        self.waiting = set()
        self.ready_tasks = []
        self.running = []
        self.failed = set()
        self.complete = set()


    def run(self):
        """
        Execute the DAG of tasks terminating at `end_task`.

        Runs a loop with the following logic:
        
        while there are running tasks:
        (a) check for any running tasks that have 'finished'
        (b) if there are any, update the 'ready' tasks
        (c) launch a subset of the 'ready' tasks
        """

        # Initialize the ready_tasks
        self._update_ready_tasks()
        # Launch an initial set of tasks
        self._launch_ready_tasks(self.ready_tasks)

        # While there are 'running' tasks...
        while self.running:
            
            # ...check if any of the running tasks are finished
            finished_tasks = self._get_finished_tasks(running)
            if finished_tasks:
                # If they are, then wrap them up. 
                self._wrapup_finished_tasks(finished_tasks)
                
                # Update the ready tasks
                self._update_ready_tasks(finished_tasks)
                # Launch some more tasks
                self._launch_ready_tasks(self.ready_tasks)


    def _get_finished_tasks(self, running_tasks):
        """
        Given a list of running tasks, return a list
        of those that are COMPLETE or FAILED.
        """
        finished = [t for t in running_tasks \
                    if self._running_task_state(t) in (TaskState.COMPLETE, 
                                                       TaskState.FAILED)]
        return finished

    def _wrapup_finished_tasks(self, finished_tasks):
        """
        Given a list of finished tasks, wrap them up
        and put them in the appropriate set (COMPLETE or FAILED).
        """
        # Ensure the finished tasks' states
        # are up-to-date.
        for t in finished_tasks:
            t_s = self._running_task_state(t)
            t.update_state(t_s)
            if t_s == TaskState.COMPLETE:
                self.complete.add(t_s)
            elif t_s == TaskState.FAILED:
                self.failed.add(t_s)

            # Perform any additional wrapup as necessary.
            self._wrapup_task(t)

            # Remove the task from running
            self.running.remove(t)
        return

    def _launch_ready_tasks(self, ready_tasks):
        """
        Given a list of ready tasks, choose
        a subset of them and launch them.
        """
        chosen_tasks = self._choose_tasks(ready_tasks)
        for task in chosen_tasks:
            self.ready_tasks.remove(task)
            self._launch_task(task)
            self.running.append(task)

    @abstractmethod
    def _launch_task(self, task):
        """
        Launch a task. For nontrivial settings
        this involves some setup (e.g., machinery for 
        communication between processes)
        """
        raise NotImplementedError("Subclasses of `AbstractManager` must implement `_launch_task()`")

    @abstractmethod
    def _wrapup_task(self, task):
        """
        Wrap up a task that has finished.
        This may involve tearing down or updating some 
        implementation-specific auxiliary data associated with the task.

        It doesn't need to update the task's `state`; that's done elsewhere.
        """
        raise NotImplementedError("Subclasses of `AbstractManager` must implement `_wrapup_task()`")
        
    @abstractmethod
    def _running_task_state(self, task):
        """
        Get the state of a running task.
        For nontrivial settings, this involves
        using Communicators etc.
        """
        raise NotImplementedError("Subclasses of `AbstractManager` must implement `_running_task_state()`")

    @abstractmethod
    def _running_task_output(self, task):
        """
        Get the state of a running task.
        For nontrivial settings, this involves some
        additional machinery beyond Task.
        """
        raise NotImplementedError("Subclasses of `AbstractManager` must implement `_running_task_output()`")
        
    @abstractmethod
    def _choose_tasks(self, ready_tasks):
        """
        Given a list of ready tasks, choose a subset of them 
        to launch next.
        """
        raise NotImplementedError("Subclasses of AbstractManager must define `_choose_tasks()`")

    @abstractmethod
    def _update_ready_tasks(self):
        pass

    @abstractmethod
    def interrupt(self):
        """
        Interrupt all RUNNING tasks in the DAG.
        """
        raise NotImplementedError("Subclasses of AbstractManager must implement `interrupt`")

    def validate_dag(self):
        """
        Check that the tasks + dependencies
        actually form a DAG. I.e., there
        are no circular dependencies.
        """
        if _rec_cycle_exists(self.end_task, [], set()):
            raise ValueError("Workflow rooted at {self.end_task.identifier} is not a DAG")

    def _enforce_incomplete(self):
        """
        Enforce the following rule:
        
        If a Task is not COMPLETE, then none
        of its downstream Tasks can be COMPLETE.
        
        Any Tasks that are erroneously COMPLETE should
        be changed to WAITING.
        """
        _rec_enforce_incomplete(self.end_task, set())

    def ready_tasks(self):
        """
        Return a list of the tasks in this
        workflow that are ready to run
        """
        return _ready_tasks(self.end_task, set())



def _rec_interrupt(task, visited):
    """
    Traverse the DAG rooted at `task`
    and interrupt all of its RUNNING Tasks. 
    """
    if task.identifier in visited:
        return

    visited.add(task.identifier)
    if task.state == TaskState.RUNNING:
        task.interrupt()

    for d in task.dependencies:
        _interrupt(d, visited)

    return


def _rec_cycle_exists(task, ancestors, visited):
    """
    Use DFS to detect cycles in task dependencies.
    ancestors: list of task IDs.
    visited: set of task IDs.
    """
    # Base case: check if this task is in
    # its own ancestors (cycle exists)
    if task.identifier in ancestors:
        return True

    # Base case: check if this task has already
    # been visited/explored
    if task.identifier in visited:
        return False

    # Recursive case: add this task to the
    # ancestor stack and explore its
    # dependencies.
    ancestors.append(task.identifier)
    for d in task.dependencies:
        if _rec_cycle_exists(d, ancestors, visited):
            return True
    # If none of the dependencies led to 
    # a cycle, remove this task from ancestors
    # and mark it as visited/explored
    ancestors.pop()
    visited.add(task.identifier)
    return False


def _ready_tasks(task, visited):
    """
    Return a list of the ready tasks
    "at or beneath" this task.
    """
    if task.identifier in visited:
        return []

    visited.add(task.identifier)
    if task.is_ready():
        return [task]
    else:
        result = []
        for d in task.dependencies:
            if d.identifier not in visited:
                result += _ready_tasks(d, visited)
        return result 

def _rec_enforce_incomplete(task, visited):
    """
    Exhaustively visit all Tasks upstream
    of this one and determine if any of them
    are not COMPLETE. 

    If so, if this one is
    COMPLETE then reset it to WAITING.
    """
    # Base case: this task has already been
    # exhaustively explored
    if task.identifier in visited:
        return task.state != TaskState.COMPLETE

    # Recurrent case: exhaustively explore upstream
    #                 tasks and relabel this one if
    #                 necessary
    upstream_incomplete = False
    for d in task.dependencies:
        upstream_incomplete |= _rec_enforce_incomplete(d, visited)
    
    visited.add(task.identifier)

    if upstream_incomplete: # something incomplete upstream
        if (task.state == TaskState.COMPLETE): # --> need to update
            task.state = TaskState.WAITING
        return True
    else: # Everything complete upstream
        return (task.state != TaskState.COMPLETE)


