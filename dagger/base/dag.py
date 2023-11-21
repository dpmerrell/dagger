"""
    dag.py
    David Merrell (c) 2023
"""

from dagger.base.task import DaggerStartTask, DaggerEndTask

"""
    A DAG object contains the (interconnected) Tasks and Datums.

    A DAG has the following attributes:
        * tasks: a dictionary-like collection of Tasks, keyed by their UIDs
        * data:  a dictionary-like collection of Datum objects, keyed by their UIDs
    
    (These may or may not actually be python dictionaries; in inheritors,
     they could be any abstract hash table or key-value store.)

    A DAG has the following methods:
        * _initialize_tasks_and_data
        * set_task
        * get_task
        * set_datum
        * get_datum
        * to_datum
"""
class DAG:

    """
    TODO: DAG CONSTRUCTION NEEDS MORE THOUGHT.
    """
    def __init__(self, tasks, inputs={}, outputs={}):

        self._initialize_tasks_and_data()

        # Translate the DAG inputs into Datum objects
        inputs = {k: self.to_datum(v) for k,v in inputs.items()}

        # Add a start task
        self.set_task(DaggerStartTask(self, dag_inputs=inputs
                                     )
                     )

        for task in tasks:
            self.add_task(task)

        return

    ####################################
    # These usually don't need to be 
    # modified by inheritors
    """
        Add or modify a Task
    """
    def set_task(self, uid, task):
        self.tasks[uid] = task
    
    """
        Fetch a Task by UID
    """
    def get_task(self, uid):
        return self.task[key]

    """
        Add or modify a Datum
    """
    def set_datum(self, uid, data):
        self.data[uid] = data

    """
        Fetch a Datum by UID
    """
    def get_datum(self, uid):
        return self.data[uid]


    ##############################################
    # These NEED to be modified by inheritors
    """
        _initialize_tasks_and_data(self)

        Set up the DAG's `tasks` and `data` attributes.
        They are initially empty.
    """
    def _initialize_tasks_and_data(self):
        raise NotImplementedError(f"Need to implement `_initialize_tasks_and_data` for {type(self)}")

    """
        to_datum(self, dag_input)

        Translates a DAG input (e.g., a file path or URL)
        into a Datum object.
    """
    def to_datum(self, dag_input):
        raise NotImplementedError(f"Need to implement `to_datum` for {type(self)}")


