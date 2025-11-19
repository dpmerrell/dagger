"""
    workflow_manager.py
    (c) 2025 David Merrell

    Implementation of a WorkflowManager abstract base class. 

    Represents a workflow manager -- an object
    that executes a DAG of Tasks.

    It accesses its DAG via a root Task (`root_task`) 
    which represents the "final" task in the DAG.

    It executes the workflow via the `run()` method,
    and terminates it via `terminate()`

    It also has some convenience methods for assessing
    and validating the workflow. 
"""

from abc import ABC, abstractmethod

def _terminate(task, visited):
    """
    Traverse the DAG rooted at `task`
    and terminate all of its RUNNING Tasks. 
    """
    if task.identifier in visited:
        return

    visited.add(task.identifier)
    if task.state == TaskState.RUNNING:
        task.terminate()

    for d in task.dependencies:
        _terminate(d, visited)

    return

def _cycle_exists(task, ancestors, visited):
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
        if _cycle_exists(d, ancestors):
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

def _enforce_incomplete(task, visited):
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
        upstream_incomplete |= _enforce_incomplete(d, visited)
    
    visited.add(task.identifier)

    if upstream_incomplete: # something incomplete upstream
        if (task.state == TaskState.COMPLETE): # --> need to update
            task.state = TaskState.WAITING
        return True
    else: # Everything complete upstream
        return (task.state != TaskState.COMPLETE)


class WorkflowManager:
    """
    Class representing a workflow manager -- an object
    that executes a DAG of Tasks.

    It accesses its DAG via a root Task (`root_task`) 
    which represents the "final" task in the DAG.

    It executes the workflow via the `run()` method.

    It also has some convenience methods for assessing 
    the state of the workflow.
    """

    def __init__(self, root_task):

        self.root_task = root_task
    
    def validate_dag(self):
        """
        Check that the tasks + dependencies
        actually form a DAG. I.e., there
        are no circular dependencies.
        """
        return not _cycle_exists(self.root_task, [], set())

    @abstractmethod
    def run(self):
        """
        Execute the DAG of tasks terminating at `root_node`.
        """
        raise NotImplementedError("Subclasses of WorkflowManager must implement `run`")

    def terminate(self):
        """
        Terminate all RUNNING tasks in the DAG.
        """
        _terminate(self.root_task, set()) 

    def enforce_incomplete(self):
        """
        Enforce the following rule:
        
        If a Task is not COMPLETE, then none
        of its downstream Tasks can be COMPLETE.
        
        Any Tasks that are erroneously COMPLETE should
        be changed to WAITING.
        """
        _enforce_incomplete(self.root_task, set())

    def ready_tasks(self):
        """
        Return a list of the tasks in this
        workflow that are ready to run
        """
        return _ready_tasks(self.root_task, set())


