"""
    datum.py
    David Merrell (c) 2023

    Implementation of Datum class.
"""

from uuid import uuid4
from util import min_timestamp, now_timestamp

"""
    A Datum is a lightweight placeholder 
    representing an individual task input or output.
    Inheritors may denote, e.g., file paths, URLs, or 
    arbitrary python objects.

    A Datum has the following attributes:
        * dag: the DAG object containing this Datum.
        * uid: UID of this Datum 
        * parent_uid: UID of the Task that outputs this Datum.
        * timestamp: Timestamp of the most recent modification to the data. 

    A Datum has the following methods:
        * get_data
        * set_data  (IMPORTANT: this should set the timestamp to `now_timestamp()`.)
        * sync_timestamp
"""
class Datum:

    def __init__(self, dag, parent_task):

        self.dag = dag
        self.uid = uuid4()
        self.parent_uid = parent_task.uid
        self.timestamp = min_timestamp()
        return

    def get_data(self):
        raise NotImplementedError()

    def set_data(self, data):
        raise NotImplementedError()

    """
        Check from an independent source whether the
        timestamp needs to be updated;
        and then update it if necessary.
        For example: if the Datum represents a file, 
        this would get the file's timestamp of latest
        modification from the filesystem. For other
        objects, this could be a do-nothing operation.
    """
    def sync_timestamp(self):
        raise NotImplementedError()


"""
    NullDatum is used to connect the DaggerStartTask
    to Tasks that have no inputs.
"""
class NullDatum(Datum):
    
    def get_data(self):
        return None

    def set_data(self):
        return

    def sync_timestamp(self):
        return # keep the min_timestamp default

