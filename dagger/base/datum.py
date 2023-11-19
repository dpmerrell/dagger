"""
    datum.py
    David Merrell (c) 2023

    Implementation of Datum class.
"""

"""
    A Datum represents an individual task input or output.
    Inheritors may wrap, e.g., file paths, URLs, or 
    arbitrary python objects.

    A Datum has the following attributes:
        * parent_uid: UID of the Task that outputs this Datum.
        * _key:       A string (or other hashable object) representing
                      the location of the data. 
                      Generated automatically during DAG construction 
                      and used to detect redundant tasks.
                      **Should NOT be modified during DAG execution.**
        * content:    Some object containing sufficient information to 
                      (1) access the data and 
                      (2) indicate that the data has been changed.
                          E.g., may contain a file path and timestamp
"""
class Datum:

    def __init__(self, parent_uid, **kwargs):

        self.parent_uid = parent_uid
        self._key = self.generate_key(**kwargs) 
        self.content = self.initiate_content(**kwargs)

        return

    def generate_key(self, **kwargs):
        raise NotImplementedError(f"Need to implement `generate_key` for {type(self)}")
    
    def initiate_content(self, **kwargs):
        raise NotImplementedError(f"Need to implement `initiate_content` for {type(self)}")

    def update_content(self, **kwargs):
        raise NotImplementedError(f"Need to implement `update_content` for {type(self)}")

    def remove_content(self, **kwargs):
        raise NotImplementedError(f"Need to implement `remove_content` for {type(self)}")


