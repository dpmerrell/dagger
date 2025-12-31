"""
    abstract/communicator.py
    (c) David Merrell 2025

    Definition of a `Communicator` class.
    Each Task contains a Communicator, which controls
    how Task state (and other data) gets communicated
    back to the WorkflowManager.
"""

from abc import ABC, abstractmethod

class AbstractCommunicator(ABC):
    """
    Each Task contains a Communicator, which controls
    how Task state (and other data) gets communicated
    back to the WorkflowManager.
    """

    def __init__(self):
        """
        Initialize the Communicator.
        For nontrivial subclasses, this should
        be updated s.t. communication machinery
        (e.g., pipes, shared memory, etc.) get
        passed as arguments and stored as members.
        """
        return

    @abstractmethod
    def report_state(self, task_state):
        """
        Use the Communicator's internal machinery to
        make task_state accessible to the WorkflowManager.
        For nontrivial subclasses, this may involve:
        * setting a multiprocessing.Value,
        * sending it down one end of a Pipe,
        * etc.
        """
        raise NotImplementedError("Subclasses of `AbstractCommunicator` must implement `report_state(...)`")


class DefaultCommunicator(AbstractCommunicator):
    """
    A do-nothing Communicator class.
    Typically created when constructing a Task,
    and then replaced by some other (nontrivial)
    Communicator object when the WorkflowManager 
    prepares to execute the Task.
    """
    
    def report_state(self, task_state):
        """
        For DefaultCommunicator, this method does nothing
        """
        return


