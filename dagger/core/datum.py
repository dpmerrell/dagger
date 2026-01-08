"""
    core/datum.py
    (c) David Merrell 2025

    Some basic implementations of the 
    AbstractDatum class.
"""

from dagger.abstract.datum import AbstractDatum, DatumState

from pathvalidate import is_valid_filepath
from os import path
import platform
import os

import time

class MemoryDatum(AbstractDatum):
    """
    A class representing a python object stored in local memory.

    In this case, the Datum's `pointer` is simply the python object
    (which is itself usually a pointer, for non-primitive types.)
    """

    def _verify_available_logic(self):
        """
        Since the pointer is itself the data, 
        it is available as long as the Datum
        is not EMPTY. The `verify_available()` method
        already checks this, so the 
        `_verify_available_logic()` method returns True always.
        """
        return self.state != DatumState.EMPTY

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

    def _quickhash(self):
        """
        For a python object in memory, we use
        its string representation (from __str__()).
        Should attain 'good enough' modification properties.
        """
        return hash(str(self.pointer))



class FileDatum(AbstractDatum):
    """
    A Datum representing a file accessible
    in the local filesystem.
    """

    def _verify_available_logic(self):
        """
        verify whether a file exists
        """
        return path.exists(self.pointer)

    def _validate_format_logic(self):
        """
        Check whether the Datum's pointer is a string containing
        a valid filepath
        """
        return is_valid_filepath(self.pointer, platform=platform.system())

    def _clear_logic(self):
        """
        When this Datum is available, it points to a file
        that exists. So we delete that file.
        """
        try:
            os.remove(self.pointer)
        except FileNotFoundError:
            pass

    def _quickhash(self):
        """
        For a file in memory, compute the quickhash
        from (A) the filename and (B) its last-modified
        timestamp.
        """
        return hash((self.pointer, os.stat(self.pointer).st_mtime_ns))


