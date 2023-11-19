"""
    dag.py
    David Merrell (c) 2023
"""

"""
    A DAG object contains the (interconnected) Tasks and Datums.

    A DAG has the following attributes:
        * tasks:    a dictionary of Tasks, keyed by their UIDs
        * data:     a dictionary of Datum objects, keyed by their UIDs

    A DAG has the following methods:
        * add_task
"""
class DAG:

    def __init__(self, tasks):

        self.tasks = {}
        self.data = {}

        for task in tasks:
            self.add_task(task)

        return

    def add_task(self, task):
        raise NotImplementedError(f"Need to implement `add_task` for {class(self)}")


