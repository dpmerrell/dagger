"""
    datum.py
    David Merrell (c) 2023

    Implementation of Datum base class.
"""


"""
    A Datum is a lightweight placeholder 
    representing an individual task input or output.
    Inheritors may denote, e.g., file paths, URLs, or 
    arbitrary python objects.

    A Datum has the following attributes:
        * dag: the DAG object containing this Datum.
        * uid: UID of this Datum 
        * parent_uid: UID of the Task that outputs this Datum.
    
    Concrete implementations may also contain additional
    state information about the data, necessary to assess the 
    DAG's execution state. For example, a timestamp of the data's 
    most recent modification.

    A Datum has the following methods:
        * get_data()         : access the underlying data
        * set_data(obj)      : modify the underlying data
        * sync()             : ensure any execution state is up-to-date
        * compute_uid(): Return a string (or hash, or other object) UID such that
                         two `Datum`s are considered redundant IFF their
                         UIDs are identical. Mathematically, these UIDs amount to
                         equivalence classes on the `Datum`s.
                         `compute_uid()` is called during DAG compilation and should ONLY 
                         rely on information available at DAG compilation.
"""
class Datum:

    """
        Datum(dag, parent_task, name=None, **kwargs)
    """
    def __init__(self, dag, parent_task, 
                       name=None, **kwargs):
        
        self.dag = dag
        self.parent_uid = parent_task.uid

        # Set the name to the class name by default        
        self.name = name
        if name is None:
            self.name = type(self).__name__
       
        # Just do a setattr for any remaining kwargs 
        for k, v in kwargs.items():
            setattr(self, k, v)

        return

    def get_data(self):
        raise NotImplementedError()

    def set_data(self, data):
        raise NotImplementedError()

    """
        Sync the Datum's state from the underlying data.
    """
    def sync(self):
        raise NotImplementedError()

    def compute_uid(self):
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

    def sync(self):
        return 

