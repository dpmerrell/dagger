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

        # Validate that this is a DAG
        self.end_task = end_task
        self.validate_dag()

        # Construct and store an adjacency list
        # representation of the DAG
        adj_list, start_task = construct_adj_list(end_task)
        self.adj_list = adj_list
        self.start_task = start_task

        # These data structures contain state for
        # the DAG execution algorithm (i.e., `run`).
        self.waiting = set()
        self.ready_tasks = []
        self.running = []
        self.failed = set()
        self.complete = set()

    def validate_dag(self):
        """
        Check that the tasks + dependencies
        actually form a DAG. I.e., there
        are no circular dependencies.
        """
        if _rec_cycle_exists(self.end_task, [], set()):
            raise ValueError("Workflow rooted at {self.end_task.identifier} is not a DAG")

    def run(self):
        """
        Execute the DAG of tasks terminating at `end_task`.

        Runs a loop with the following logic:
        
        while there are running tasks:
        (a) check for any running tasks that have 'finished'
        (b) if there are any, update the 'ready' tasks
        (c) launch a subset of the 'ready' tasks
        """
        # Initialize the running_tasks
        self.running.append(self.start_task)

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
            t_s = self._get_running_task_state(t)
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


    def _update_ready_tasks(self, finished_tasks):
        """
        Given a list of recently finished tasks, update
        the list of 'ready' tasks with their children

        transitions: 'waiting' -> 'ready'
        """
        # Whenever a finished task has a child whose
        # parents are all COMPLETE, then move the
        # child to the `ready` list.
        for ft in finished_tasks:
            children = self.adj_list[ft]
            for c in children:
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
    def _get_running_task_state(self, task):
        """
        Get the state of a running task.
        For nontrivial settings, this involves
        using Communicators etc.
        """
        raise NotImplementedError("Subclasses of `AbstractManager` must implement `_get_running_task_state()`")

    @abstractmethod
    def _get_running_task_output(self, task):
        """
        Get the output of a running task (assuming it's complete).
        For nontrivial settings, this involves some
        additional machinery beyond Task.
        """
        raise NotImplementedError("Subclasses of `AbstractManager` must implement `_get_running_task_output()`")
        
    @abstractmethod
    def _choose_tasks(self, ready_tasks):
        """
        Given a list of ready tasks, choose a subset of them 
        to launch next.
        """
        raise NotImplementedError("Subclasses of AbstractManager must define `_choose_tasks()`")

    @abstractmethod
    def interrupt(self):
        """
        Interrupt all RUNNING tasks in the DAG.
        """
        raise NotImplementedError("Subclasses of AbstractManager must implement `interrupt`")



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


def construct_adj_list(task, start_node=None):
    """
    Construct an 'adjacency list' representation of 
    the task DAG; include an artificial StartTask
    node
    """
    adj_list = defaultdict(set)

    if start_node is None:
        start_node = StartTask()

    _rec_construct_adj_list(task, adj_list, start_node, visited=set())
    adj_list = {k: list(v) for k, v in adj_list.items()}
    return adj_list, start_node


def _rec_construct_adj_list(task, adj_list, start_node, visited=set()):
    """
    Recursive core of `construct_adj_list`
    """
    # base case: no dependencies
    if len(task.dependencies) == 0:
        adj_list[start_node].add(task)

    # Recurrent case: has dependencies
    for dep in task.dependencies:
        if dep not in visited:
            adj_list[dep].add(task)
            _rec_construct_adj_list(dep, adj_list, visited=visited)

    visited.add(task)


