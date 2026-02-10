"""
    abstract/datum.py
    (c) David Merrell 2025

    A class representing a piece of data passed
    to or from a Task -- that is, an input or output.
    
    It is often used as an "IOU" for data that does
    not yet exist. For example, a Datum representing
    a Task output can be constructed and passed to
    other tasks before that Task has even completed.
    
    Here we include an abstract base class.
    See `core/datum.py` for basic concrete implementations.

    A Datum goes through the following lifecycle:

    1. Initialized (state=EMPTY; it's an IOU)
        A) Occurs when constructor is called
        B) This usually happens when a Task is constructed;
           its outputs are initialized as empty Datums
        C) It may also happen for workflow input data, which 
           may be supplied to tasks as 'raw' Datums
    2. Populated   (state=POPULATED; it contains a pointer to data which may or may not exist)
        A) Depending on the type of task, its output Datums
           may be POPULATED on construction.
           For example, tasks that may have data that
           persists in files on disk between runs
        B) May also happen for workflow input data, which
           are constructed as Datums and then populated 
           as necessary
    3. Available (state=AVAILABLE; the data referenced by the pointer is known to exist)
        A) A Datum can enter this state only as a result of
           `_verify_available()` method call.
           `_verify_available()` is typically called on each of a Task's output Datums
           (A) when that Task is constructed (to determine if the Task is already complete)
           and (B) when that Task completes, to determine if its outputs exist.
    
    EMPTY <--> POPULATED <--> AVAILABLE

    A Datum goes through the following transitions:
    * EMPTY -> POPULATED via `populate(...)`
    * POPULATED -> EMPTY via `_validate_format()`
    * POPULATED -> AVAILABLE via `_verify_available()`
    * AVAILABLE -> POPULATED via `clear()`

    Once the Datum is AVAILABLE, a downstream Task may `collect` the data
    it points to, as an input. The `collect` logic depends on the type 
    of the downstream Task, as well as the precise type of this Datum.
    Which is an awkward part of this design: `collect` logic needs
    to be defined for each pair (task, datum) of types that appears in
    a workflow.

    The Datum's `parents` play basically no role in any of this
    lifecycle. `parents` just enable bookkeeping; they help downstream tasks
    identify upstream tasks from their inputs.

    The Datum stores a `quickhash`, which is a hashable representing the
    underlying data. A Datum's 'quickhash' should follow these rules:
    1. Identification: different Datums should have different quickhashes
    2. Modification: a Datum's quickhash should be different whenever its
                     underlying data gets modified.

    Certainly, a full-blown hash of the underlying data would work.
    But simpler and less expensive operations also satisfy these rules.
    For example, if a Datum represents a file on disk, then a quickhash computed
    from the pair (filename, last-modified-timestamp) would also satisfy these 
    requirements. We really don't want dagger to burn CPU on costly hashes. 
    We just need something 'good enough' to alert dagger whenever a Task's 
    inputs have changed, and to keep Datums distinct from each other.

    The quickhash is computed and set whenever the Datum enters an 
    AVAILABLE state. It is set to None whenever the Datum is not
    in an AVAILABLE state.
"""

from abc import ABC, abstractmethod
from enum import Enum

class DatumState(Enum):
    """
    A Datum can be in exactly one of the following states:
    * EMPTY
    * POPULATED
    * AVAILABLE
    """
    EMPTY = 0
    POPULATED = 1
    AVAILABLE = 2


_DATUM_TRANSITIONS = {
    DatumState.EMPTY:     {DatumState.POPULATED},
    DatumState.POPULATED: {DatumState.EMPTY, DatumState.AVAILABLE},
    DatumState.AVAILABLE: {DatumState.EMPTY, DatumState.POPULATED},
}


class AbstractDatum(ABC):
    """
    A class representing a piece of data passed
    to or from a Task -- that is, an input or output.
    
    It is often used as an "IOU" for data that does
    not yet exist. For example, a Datum representing
    a Task output can be constructed and passed to
    other tasks before that Task has even completed.
    """

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state):
        if hasattr(self, '_state') and new_state != self._state:
            if new_state not in _DATUM_TRANSITIONS[self._state]:
                raise ValueError(
                    f"Invalid DatumState transition: "
                    f"{self._state.name} → {new_state.name}"
                )
        self._state = new_state

    def __init__(self, parent=None, **kwargs):
        """
        Construct a Datum. By default, it's empty and
        has no parent task.

        If an optional `parent` Task kwarg is provided,
        the datum stores that as its parent Task. Otherwise,
        the Datum is regarded as having no parent Task.

        If an optional `pointer` kwarg is provided, 
        the Datum is populated with that pointer to 
        the underlying data. And then we check whether
        the underlying data is already available.
        """
        self.state = DatumState.EMPTY
        self.parents = [parent]
        self.pointer = None
        self.quickhash = None
        if "pointer" in kwargs.keys():
            self.populate(kwargs["pointer"])
            self.verify_available(update=True)

    def populate(self, pointer):
        """
        Populate the Datum with sufficient information
        to retrieve the associated data. For some
        implementations this is essentially a "pointer"
        to the data; for other implementations this is
        a copy of the data itself.
        """
        self.pointer = pointer
        self.state = DatumState.POPULATED 
        self._validate_format()
    
    def _validate_format(self):
        """
        Throw an exception if the Datum's `pointer`
        is not well-formed. If it isn't, set the
        state to EMPTY and the pointer to None
        """
        if not self._validate_format_logic():
            self.pointer = None
            self.state = DatumState.EMPTY
            raise ValueError(f"Datum's pointer is not well-formed: {self.pointer}")
    
    @abstractmethod
    def _validate_format_logic(self) -> bool:
        """
        Return a bool indicating whether the Datum's
        pointer is well-formed
        """
        raise NotImplementedError("Subclasses of `AbstractDatum` must implement `_validate_format_logic()`")

    def verify_available(self, update=True) -> bool:
        """
        Verify whether the Datum points to data that is 
        available for use. If it is, then set
        the state to AVAILABLE. Return a bool indicating
        whether the data is available.

        I.e., the Datum is POPULATED and its `data` points to
        data that exists.
        """
        if (self.state != DatumState.EMPTY) and self._verify_available_logic():
            if update:
                self.state = DatumState.AVAILABLE
                self.quickhash = self._quickhash()
            return True
        else:
            return False
    
    @abstractmethod
    def _verify_available_logic(self) -> bool:
        """
        Return a bool indicating whether the Datum
        points to data that exists.
        """
        raise NotImplementedError("Subclasses of `AbstractDatum` must implement `_verify_available_logic()`")

    def clear(self):
        """
        If the Datum is AVAILABLE, clear away any persistent data
        and set its state to POPULATED.
        """
        if self.state != DatumState.EMPTY:
            self._clear_logic()
            self.state = DatumState.POPULATED
            self.quickhash = None

    @abstractmethod
    def _clear_logic(self):
        """
        Clear away any persistent data pointed to by this Datum.
        E.g., delete associated files, etc.
        """
        raise NotImplementedError("Subclasses of `AbstractDatum` must implement `_clear_logic()`")

    def _verify_quickhash(self, update=False) -> bool:
        """
        Compute this Datum's quickhash and check whether it
        matches the Datum's stored quickhash.
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
    def _quickhash(self) -> int:
        """
        Return an int satisfying the following:
        1. Identification: each Datum has a distinct quickhash.
        2. Modification: a Datum's quickhash changes whenever its underlying
                         data is modified.
        """
        raise NotImplementedError("Subclasses of `AbstractDatum` must implement `_quickhash()`")

    def sync(self):
        """
        Sync up the state of a Datum based on 
        (i) its pointer value
        (ii) whether its underlying data is available 
        (iii) its quickhash
        """

        if not self._validate_format_logic():
            self.pointer = None
            self.state = DatumState.EMPTY
        else: # Datum is populated
            if self._verify_available_logic():
                # Datum is available. Is it up-to-date?
                if self._verify_quickhash(update=True):
                    # If so, then mark this available
                    self.state = DatumState.AVAILABLE
                else:
                    # If not up-to-date, then we clear the Datum
                    # (this also sets the state to populated)
                    self.clear()
            else:
                self.state = DatumState.POPULATED

