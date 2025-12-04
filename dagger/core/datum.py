"""
    core/datum.py
    (c) David Merrell 2025

    A class representing a piece of data passed
    to or from a Task -- that is, an input or output.
    
    It is often used as an "IOU" for data that does
    not yet exist. For example, a Datum representing
    a Task output can be constructed and passed to
    other tasks before that Task has even completed.
    
    We include an abstract base class as well as some
    basic concrete implementations.

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
        A) A Datum can only enter this state as a result of
           a `check_available()` method call.
    
    EMPTY <--> POPULATED <--> AVAILABLE

    A Datum goes through the following transitions:
    * EMPTY -> POPULATED via `populate(...)`
    * POPULATED -> EMPTY via `validate_format()`
    * POPULATED -> AVAILABLE via `check_available()`
    * AVAILABLE -> POPULATED via `clear()`

    Once the Datum is AVAILABLE, a downstream Task may `collect` the data
    it points to, as an input. The `collect` logic depends on the type 
    of the downstream Task, as well as the precise type of this Datum.
    Which is an awkward part of this design: `collect` logic needs
    to be defined for each pair (task, datum) of types that appears in
    a workflow.
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

class AbstractDatum(ABC):
    """
    A class representing a piece of data passed
    to or from a Task -- that is, an input or output.
    
    It is often used as an "IOU" for data that does
    not yet exist. For example, a Datum representing
    a Task output can be constructed and passed to
    other tasks before that Task has even completed.
    """

    def __init__(self, **kwargs):
        """
        Construct a Datum. By default, it's empty. 

        But if an optional `pointer` kwarg is provided, 
        the Datum is populated with that pointer.
        """
        self.state = DatumState.EMPTY
        self.pointer = None
        if "pointer" in kwargs.keys():
            self.populate(kwargs["pointer"])

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
        self.validate_format()
    
    def validate_format(self):
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
    def _validate_format_logic(self):
        """
        Return a bool indicating whether the Datum's
        pointer is well-formed
        """
        raise NotImplementedError("Subclasses of `AbstractDatum` must implement `_validate_format_logic()`")

    def check_available(self):
        """
        Check whether the Datum points to data that is 
        available for use. If it is, then set
        the state to AVAILABLE. Return a bool indicating
        whether the data is available.

        I.e., the Datum is POPULATED and its `data` points to
        data that exists.
        """
        if (self.state == DatumState.POPULATED) and self._check_available_logic():
            self.state = DatumState.AVAILABLE
            return True
        else:
            return False
    
    @abstractmethod
    def _check_available_logic(self):
        """
        Return a bool indicating whether the Datum
        points to data that exists.
        """
        raise NotImplementedError("Subclasses of `AbstractDatum` must implement `_check_available_logic()`")


    def clear(self):
        """
        If the Datum is AVAILABLE, clear away any persistent data
        and set its state to POPULATED.
        """
        if self.state == DatumState.AVAILABLE:
            self._clear_logic()
            self.state = DatumState.POPULATED

    @abstractmethod
    def _clear_logic(self):
        """
        Clear away any persistent data pointed to by this Datum.
        E.g., delete associated files, etc.
        """
        raise NotImplementedError("Subclasses of `AbstractDatum` must implement `_clear_logic()`")


class MemoryDatum(AbstractDatum):
    """
    A class representing a python object stored in local memory.

    In this case, the Datum's `pointer` is simply the python object
    (which is itself usually a pointer, for non-primitive types.)
    """

    def _check_available_logic(self):
        """
        Since the pointer is itself the data, 
        it is available as long as the Datum
        is POPULATED. The `check_available()` function
        already checks this, so we return True always.
        """
        return True

    def _validate_format_logic(self):
        """
        Since any python object (including `None`) is valid data
        for this Datum, return True always.
        """
        return True

    def _clear_logic(self):
        """
        For this Datum, there is no persistent data.
        So we don't have to clear anything.
        """
        return

import os
from os import path
from pathvalidate import is_valid_filepath

class DiskDatum(AbstractDatum):
    """
    A class representing a file accessible
    in the local filesystem.
    """

    def _check_available_logic(self):
        """
        Check whether a file exists
        """
        return path.exists(self.pointer)

    def _validate_format_logic(self):
        """
        Check whether the Datum's pointer is a string containing
        a valid filepath
        """
        return is_valid_filepath(self.pointer)

    def _clear_logic(self):
        """
        When this Datum is available, it points to a file
        that exists. So we delete that file.
        """
        try:
            os.remove(self.pointer)
        except FileNotFoundError:
            pass


