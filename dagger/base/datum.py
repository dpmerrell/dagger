"""
    datum.py
    David Merrell (c) 2023

    Implementation of Datum class.
"""

from uuid import uuid4

"""
    A Datum is a lightweight placeholder 
    representing an individual task input or output.
    Inheritors may denote, e.g., file paths, URLs, or 
    arbitrary python objects.

    A Datum has the following attributes:
        * dag: the DAG object containing this Datum.
        * uid: UID of this Datum 
        * parent_uid: UID of the Task that outputs this Datum.
"""
class Datum:

    def __init__(self, dag, parent_task):

        self.dag = dag
        self.uid = uuid4()
        self.parent_id = parent_task.uid

        return

    def get_data(self):
        return self.dag.data[self.uid]

    def set_data(self, data):
        self.dag.data[self.uid] = data

    def is_up_to_date(self):
        return self.dag.tasks[self.parent_uid].is_up_to_date()


