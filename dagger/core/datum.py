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
    2. Populated   (state=POPULATED; it contains a pointer to data which exists)
        A) This usually happens when a Task is completed;
           its output Datums are populated with the results
           of the Task.
        B) May also happen for workflow input data, which
           are constructed as Datums and then populated 
           as necessary

    Once the Datum is populated, a downstream Task may `collect` the data
    it points to, as an input. The `collect` logic depends on the type 
    of the downstream Task, as well as the type of this Datum.
"""

from abc import ABC, abstractmethod
from enum import Enum


class DatumState(Enum):
    """
    A Datum can be in exactly one of the following states:
    * EMPTY
    * POPULATED
    """
    EMPTY = 0
    POPULATED = 1


class AbstractDatum(ABC):
    """
    A class representing a piece of data passed
    to or from a Task -- that is, an input or output.
    
    It is often used as an "IOU" for data that does
    not yet exist. For example, a Datum representing
    a Task output can be constructed and passed to
    other tasks before that Task has even completed.
    """

    def __init__(self):
        """
        Construct an empty Datum.
        """
        self.data = None
        self.state = DatumState.EMPTY

    def populate(self, data):
        """
        Populate the Datum with sufficient information
        to retrieve the associated data. For some
        implementations this is essentially a "pointer"
        to the data; for other implementations this is
        a copy of the data itself.
        """
        self.data = data
        self.state = DatumState.POPULATED 
        self._validate_format()
        self._data_exists()
    
    def data_available(self):
        """
        Return a bool indicating whether the data pointed to
        by this Datum is available for collection and use.
        
        I.e., the Datum is POPULATED and its `data` points to
        data that exists.
        """
        if self.state != DatumState.POPULATED:
            return False
        else:
            return self._data_exists()

    @abstractmethod
    def _validate_format(self):
        """
        Throw an exception if the Datum's `data`
        member is not well-formed.
        """
        raise NotImplementedError("Subclasses of `AbstractDatum` must implement `_validate_format`")

    @abstractmethod
    def _data_exists(self):
        """
        Return a bool indicating whether the 
        data exists
        """
        raise NotImplementedError("Subclasses of `AbstractDatum` must implement `_data_exists`")


class MemoryDatum(AbstractDatum):
    """
    A class representing a python object
    stored in local memory.
    """
    
    def validate_data(self, data):
        """
        Populate the Datum with a python object.

        In this case, `data` is simply the python object.
        """
        self.data = data

    def data_available(self, data):
        """
        Make sure the Datum is populated with a python object.

        In this case, we're just checking whether 
        """

class DiskDatum(AbstractDatum):
    """
    A class representing a file accessible
    in the local filesystem.
    """

    def populate(self, data):
        """
        Populate the Datum with a string representing
        a path to a file.
        """
        if not isinstance(data, str):
            raise ValueError("`DiskDatum` can only be `populate`d with str (representing a file path)")
        self.data = data


