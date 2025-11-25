"""
    core/task.py
    (c) David Merrell 2025

    A subclass of AbstractTask that has
    important additional features 
    * Task inputs and outputs
    * Task resources
"""

from dagger.abstract import AbstractTask
from dagger.core import Datum

class Task(AbstractTask):
    """
    A subclass of AbstractTask that has
    important additional features 
    * Task inputs and outputs
    * Task resources
    """

    def __init__(self, identifier, inputs={}, outputs={}, dependencies=[]):
        """
            Construct a Task with the given identifier,
            inputs, outputs, and other dependencies.

            The task's full set of dependencies is the union
            of those given directly (via `dependencies`) and
            those inferred from `inputs`.
        """
        super().__init__(identifier, dependencies)

        
