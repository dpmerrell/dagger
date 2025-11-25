"""
    task.py
    (c) 2025 David Merrell

    Implementation of the `AbstractTask` abstract base class.
    Represents a unit of computational work.
    
    A Task exists in exactly one of four states:
    WAITING, RUNNING, COMPLETE, or FAILED.

    A Task may require other Tasks to be complete before
    it is allowed to run. These are called the task's
    `dependencies`.
"""

from abc import ABC, abstractmethod
from enum import Enum


class TaskState(Enum):
    """
    A Task exists in exactly one of these states
    at any given time:

           ------------> --> COMPLETE
          /             / 
    WAITING --> RUNNING 
                        \
                         --> FAILED

    WAITING can transition to RUNNING via .run()
    WAITING can transition to COMPLETE via .check_complete()
    RUNNING transitions to COMPLETE if .run() finishes
    RUNNING transitions to FAILED if .run() raises an exception
    RUNNING transitions to WAITING via .interrupt()
    """
    WAITING = 0
    RUNNING = 1
    COMPLETE = 2
    FAILED = 3


class AbstractTask(ABC):
    """
    A class representing some unit of computational work
    to be carried out.

    A `Task` possesses a unique identifier.
    Must be hashable.

    A `Task` possesses a list of `dependencies`.
    This is itself a list of `Tasks` that must be
    complete before the `Task` itself can run.

    A task can be in exactly one of four states:
    waiting, running, complete, and failed.
    """

    def __init__(self, identifier, dependencies=[]):
        """
        Construct a new Task object with
        given identifier and dependencies.

        On construction, its state is `WAITING`.
        """

        self.identifier = identifier
        self.dependencies = dependencies
        self.state = TaskState.WAITING
    
    @abstractmethod
    def _run_logic(self):
        """
        Core logic for executing the computational work.
        """
        raise NotImplementedError("Subclasses of Task must implement `_run_logic`")
    
    @abstractmethod
    def _interrupt_cleanup(self):
        """
        Reset the Task's internal data
        such that it can be attempted again
        after an interrupt.
        """
        raise NotImplementedError("Subclasses of Task must implement `_interrupt_cleanup`")

    @abstractmethod
    def _fail_cleanup(self):
        """
        Perform any necessary cleanup after a Task fails.
        """
        raise NotImplementedError("Subclasses of Task must implement `_fail_cleanup`")

    @abstractmethod
    def _check_complete_logic(self):
        """
        Check whether a task is complete.
        Details will depend on the Task's 
        specific implementation.
        Returns a bool, and does not modify the Task's state.
        """
        raise NotImplementedError("Subclasses of Task must implement `_check_complete_logic`")

    def check_complete(self):
        """
        Check whether a task is complete;
        if it is, update the Task's state.
        Return a bool indicating whether the 
        task is complete.
        """
        if self._check_complete_logic():
            self.state = TaskState.COMPLETE

        return self.state == TaskState.COMPLETE

    def is_ready(self):
        """
        A Task is ready to run iff all of its dependencies
        are complete
        """
        return all((d.state == TaskState.COMPLETE for d in self.dependencies))

    def run(self):
        """
        Run the Task.

        Executes the _run_logic() while (A) keeping the
        Task's state up-to-date and (B) catching exceptions.
        """
        self.state = TaskState.RUNNING
        try:
            self._run_logic()
        except KeyboardInterrupt:
            self.interrupt()
        except Exception as e:
            self.fail()
            raise e
        else:
            # Assuming the function ran to completion
            # and the outputs are valid, mark this 
            # task as COMPLETE
            self.state = TaskState.COMPLETE

    def interrupt(self):
        """
        Interrupt execution of a RUNNING Task.
        
        also set self.state = TaskState.WAITING.
        """
        self.state = TaskState.WAITING
        self._interrupt_cleanup()

    def fail(self):
        """
        Transition a RUNNING Task into a FAILED state.
        """
        self.state = TaskState.FAILED
        self._fail_cleanup()

