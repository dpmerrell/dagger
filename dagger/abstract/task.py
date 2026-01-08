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

    Meta-comment: 
    * Methods named '_is_...' are meant to return
      a bool while doing practically no work.
      Simply report properties as they exist.
    * Methods named '_verify...' are meant to 
      do some work. They (A) check the underlying data,
      (B) update the relevant attribute, and then (C) return a bool.

"""

from dagger.abstract.communicator import DefaultCommunicator
from dagger.abstract.datum import DatumState
from dagger.abstract import helpers

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

    WAITING may transition to COMPLETE via .verify_complete()
    WAITING transitions to RUNNING via .run()
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

    A task generally contains some machinery for
    communicating its state to the WorkflowManager.
    But it's initialized with "do-nothing" default
    communication machinery.
    
    A task has a `quickhash` that exposes modifications to it.
    The `quickhash` is a hashable computed in a
    way that satisfies these rules:
    * Identification: different Task instances should have
                      different quickhashes
    * Modification: a Task's quickhash should be different
                    whenever the Task is modified in a way
                    that 'matters'.
    This need not be a full hash of the object; it just needs to satisfy
    these rules and be inexpensive to compute.
    """

    def __init__(self, identifier, inputs=None, outputs=None, 
                                   dependencies=None, resources=None):
        """
        Construct a new Task object with
        given identifier and dependencies.

        On construction, its state is `WAITING`.
        """

        self.identifier = identifier
        self.state = TaskState.WAITING
        self.communicator = DefaultCommunicator()
        
        # Store inputs
        self.inputs = {} if inputs is None else inputs
        
        # Collect additional dependencies from `inputs`
        self.dependencies = [] if dependencies is None else dependencies
        self.dependencies = helpers.collect_dependencies(self.inputs, 
                                                         self.dependencies)

        # Construct the output Datums
        outputs = {} if outputs is None else outputs
        self.outputs = self._initialize_outputs(outputs)
        
        # Store the Task's resource requirements
        self.resources = {} if resources is None else resources
        
        # Compute this Task's quickhash
        self.quickhash = self._quickhash()

    @abstractmethod
    def _initialize_outputs(self, output_dict):
        """
        A Task needs to specify how it initializes output
        Datums from a dict of name=>pointer
        """
        raise NotImplementedError("Subclasses of AbstractTask must implement `_initialize_outputs(output_dict)`")

    def run(self):
        """
        Run the Task.

        Executes the _run_logic() while (A) keeping the
        Task's state up-to-date and (B) catching exceptions
        and failures to complete.
        """
        if not self.is_ready():
            raise RuntimeError(f"Task {self.identifier} is not ready to run.")
        self.update_state(TaskState.RUNNING)
        try:
            self._run_logic()
            if not self._verify_outputs():
                raise RuntimeError(f"Task {self.identifier} ran, but is missing outputs.")
        except KeyboardInterrupt:
            self.interrupt()
        except Exception as e:
            self.fail()
            raise e
        else:
            self.update_state(TaskState.COMPLETE)
    
    def is_ready(self):
        """
        A Task is ready to run iff all of its dependencies
        are complete
        """
        return all((d.state == TaskState.COMPLETE for d in self.dependencies))
    
    @abstractmethod
    def _run_logic(self):
        """
        Core logic for executing the computational work.
        """
        raise NotImplementedError("Subclasses of AbstractTask must implement `_run_logic`")
    
    def update_state(self, new_state):
        """
        Assign a new value to self.state
        """
        self.state = new_state
        self.communicator.report_state(self)
    
    def _verify_outputs(self):
        """
        Verify that all the task's outputs are AVAILABLE.
        """
        return all((out.verify_available(update=True) for out in self.outputs.values()))

    def verify_complete(self):
        """
        Verify whether a task is complete;
        if it is, update the Task's state.
        Return a bool indicating whether the 
        task is complete.
        """
        deps_complete = all((d.state == TaskState.COMPLETE for d in self.dependencies))
        task_uptodate = self._update_quickhash()
        inp_uptodate = all((inp._update_quickhash() for inp in self.inputs.values())) 
        outputs_complete = self._verify_outputs() 
        complete = all((deps_complete, task_uptodate, inp_uptodate, outputs_complete))

        if complete:
            self.update_state(TaskState.COMPLETE)
        return self.state == TaskState.COMPLETE
   
    def _verify_quickhash(self, update=False):
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
            if update:
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
        raise NotImplementedError("Subclasses of AbstractTask must implement `_quickhash()`")

    def interrupt(self):
        """
        Interrupt execution of a RUNNING Task.
        
        also set self.state = TaskState.WAITING.
        """
        self.update_state(TaskState.WAITING)
        self._interrupt_cleanup()
   
    @abstractmethod
    def _interrupt_cleanup(self):
        """
        Reset the Task's internal data
        such that it can be attempted again
        after an interrupt.
        """
        raise NotImplementedError("Subclasses of AbstractTask must implement `_interrupt_cleanup`")

    def fail(self):
        """
        Transition a RUNNING Task into a FAILED state.
        """
        self.update_state(TaskState.FAILED)
        self._fail_cleanup()
    
    @abstractmethod
    def _fail_cleanup(self):
        """
        Perform any necessary cleanup after a Task fails.
        """
        raise NotImplementedError("Subclasses of AbstractTask must implement `_fail_cleanup`")

    def __getitem__(self, key):
        """
        Get a Task output by name.
        """
        return self.outputs[key]

    def sync(self, recursive=True, visited=None):
        """
        Enforce this Task's state to be consistent with
        (a) the state of its dependencies,
        (b) the state of its input Datums, and
        (c) the state of its output Datums
            (IF this is the final task.)

        In particular:
        * a Task is COMPLETE iff 
          - its quickhash is up-to-date,
          - all of its deps are COMPLETE,
          - all of its inputs are AVAILABLE and up-to-date, and 
          - all of its outputs are AVAILABLE and up-to-date.
        * a Task is FAILED iff its state has already been marked 'FAILED'
        * a Task is WAITING otherwise 
        """
        complete = True
        visited = set() if visited is None else visited

        # Sync this task's inputs
        for datum in self.inputs.values():
            datum.sync()
        inputs_available = all((i.state == DatumState.AVAILABLE for i in self.inputs.values()))
        complete &= inputs_available

        # Sync this task's outputs
        for datum in self.outputs.values():
            datum.sync()
        outputs_available = all((d.state == DatumState.AVAILABLE for d in self.outputs.values()))
        complete &= outputs_available 

        # Sync this task's dependencies recursively
        if recursive: 
            for d in self.dependencies:
                if d not in visited:
                    d.sync(recursive=True, visited=visited)
        
        # Set this task's state
        deps_complete = all((d.state == TaskState.COMPLETE for d in self.dependencies))
        complete &= deps_complete
        print(f"{self.identifier} complete?: {complete}")
        print("\tinputs available:", inputs_available)
        print("\t\tinput states:", [i.state for i in self.inputs.values()])
        print("\toutputs available:", outputs_available)
        print("\tdeps complete:", deps_complete)
        if self.state == TaskState.FAILED:
            self.update_state(TaskState.FAILED)
        elif complete and self._verify_quickhash(update=True):
            self.update_state(TaskState.COMPLETE)
        else:
            self.update_state(TaskState.WAITING)

        # Mark this task as visited
        print("SYNCED", self.identifier)
        visited.add(self)

