"""
    core/task.py
    (c) David Merrell 2025

    A subclass of AbstractTask that has
    important additional features. 

    It also contains a `quickhash` hashable computed in a
    way that satisfies these rules:
    * Identification: different Task instances should have
                      different quickhashes
    * Modification: a Task's quickhash should be different
                    whenever the Task is modified.
    This need not be a full hash; it just needs to satisfy
    these rules and be inexpensive to compute.
"""

from dagger.abstract import AbstractTask, TaskState
from dagger.core import helpers
from abc import abstractmethod

#class Task(AbstractTask):
#    """
#    A subclass of AbstractTask that has
#    important additional features:
#    * A dictionary of resource requirements (`resources`)
#
#    `Task` implements the following abstractmethods:
#    * some additional run() logic
#        - _verify_outputs()
#    * _verify_complete_logic()
#
#    `Task` imposes the following abstractmethods on
#    its subclasses:
#    * _quickhash()
#    * _initialize_outputs()
#    """
#
#    def __init__(self, identifier, inputs={}, outputs={}, dependencies=[],
#                                   resources={}):
#        """
#        Construct a Task with the given identifier,
#        inputs, outputs, other dependencies, and resource requirements.
#
#        The task's full set of dependencies is the union
#        of those given directly (via `dependencies`) and
#        those inferred from `inputs`.
#        """
#        # Call the AbstractTask constructor
#        super().__init__(identifier, inputs=inputs, outputs=outputs, 
#                         dependencies=dependencies)
#
#        # Store this Task's resource requirements
#        self.resources = resources
#
#        return
    


