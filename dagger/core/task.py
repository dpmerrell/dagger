"""
    core/task.py
    (c) David Merrell 2025

    A subclass of AbstractTask that has
    important additional features. 

    Namely, it has `inputs` and `outputs` 
    represented by dicts of Datum objects.

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

class Task(AbstractTask):
    """
    A subclass of AbstractTask that has
    important additional features. 

    Namely, it has `inputs` and `outputs` 
    represented by dicts of Datum objects.
    
    It also contains a `quickhash` hashable computed in a
    way that satisfies these rules:
    * Identification: different Task instances should have
                      different quickhashes
    * Modification: a Task's quickhash should be different
                    whenever the Task is modified in a way
                    that 'matters'.
    This need not be a full hash of the object; it just needs to satisfy
    these rules and be inexpensive to compute.

    It implements the following abstractmethods:
    * some additional run() logic
        - _verify_outputs()
    * _verify_complete_logic()
    """

    def __init__(self, identifier, inputs={}, outputs={}, dependencies=[]):
        """
        Construct a Task with the given identifier,
        inputs, outputs, other dependencies, and resource requirements.

        The task's full set of dependencies is the union
        of those given directly (via `dependencies`) and
        those inferred from `inputs`.
        """
        # Call the AbstractTask constructor
        super().__init__(identifier, dependencies=dependencies)

        # Collect additional dependencies from `inputs`
        deps = helpers.collect_dependencies(inputs, dependencies)
        self.dependencies = deps
        self.inputs = inputs

        # Construct the output Datums
        self.outputs = self._initialize_outputs(outputs)
        
        # Compute this Task's quickhash
        self.quickhash = self._quickhash()

        return

    def run(self):
        """
        Run the Task.

        Executes the _run_logic() while (A) keeping the
        Task's state up-to-date and (B) catching exceptions
        and failures to complete.

        Also verifies that the outputs exist and are available.
        """
        super().run()
        if not self._verify_outputs():
            self.state = TaskState.FAILED
            raise RuntimeError("Task {self.identifier} ran, but is missing outputs.")

    def _verify_complete_logic(self):
        """
        A Task is complete if: 
        * its dependency tasks are all complete (shallow check)
        * its own quickhash is up-to-date
        * its inputs are all up-to-date
        * its outputs are all available
        """
        deps_complete = all((d.state == TaskState.COMPLETE for d in self.dependencies))
        task_uptodate = self._verify_quickhash()
        inp_uptodate = all((inp._verify_quickhash() for inp in self.inputs.values())) 
        outputs_complete = self._verify_outputs() 
        return all((deps_complete, task_uptodate, inp_uptodate, outputs_complete))     

    def _verify_outputs(self):
        """
        Verify that all the task's outputs are AVAILABLE.
        """
        return all((out._verify_available() for out in self.outputs.values()))

    @abstractmethod
    def _initialize_outputs(self, output_dict):
        """
        A Task needs to specify how it initializes output
        Datums from a dict of name=>pointer
        """
        raise NotImplementedError("Subclasses of Task must implement `_initialize_outputs(output_dict)`")

    def _verify_quickhash(self):
        """
        Compute this Task's quickhash and check whether it
        matches the Task's stored quickhash.
        If they match, return True.
        If they don't match, update the quickhash and return False.
        """
        new_hash = self._quickhash()
        if new_hash == self.quickhash:
            return True
        else:
            self.quickhash = new_hash
            return False

    @abstractmethod
    def _quickhash(self):
        """
        A Task subclass needs to specify a `quickhash`
        function satisfying the following rules:
        * Identification: different Task instances should have
                          different quickhashes
        * Modification: a Task's quickhash should be different
                        whenever the Task is modified in a way
                        that 'matters'.
        This need not be a full hash; it just needs to satisfy
        these rules and be inexpensive to compute.
        """
        raise NotImplementedError("Subclasses of Task must implement `_quickhash()`")



