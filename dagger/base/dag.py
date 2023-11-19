"""
    dag.py
    David Merrell (c) 2023
"""


"""
    A DAG object contains the (interconnected) Tasks and Datums.

    A DAG has the following attributes:
        * tasks:    a dictionary of Tasks, keyed by their UIDs
        * data:     a dictionary of Datum objects, keyed by their UIDs
    
    (These may or may not actually be python dictionaries; in inheritors,
     they could be any abstract hash table or key-value store.)

    A DAG has the following methods:
        * _initialize_tasks_and_data
        * add_task
        * input_to_datum
"""
class DAG:

    def __init__(self, tasks):

        self._initialize_tasks_and_data()

        for task in tasks:
            self.add_task(task)

        return


    """
        _initialize_tasks_and_data(self)

        Set up the DAG's `tasks` and `data` attributes.
        They are initially empty.
    """
    def _initialize_tasks_and_data(self):
        raise NotImplementedError(f"Need to implement `_initialize_tasks_and_data` for {type(self)}")


    """
        add_task(self, task)
    
        Add a task to the DAG. Must update the DAG's 
        `tasks` and `data` attributes appropriately.
    """
    def add_task(self, task):
        raise NotImplementedError(f"Need to implement `add_task` for {type(self)}")


    """
        input_to_datum(self, dag_input)

        Translates a DAG input (e.g., a file path or URL)
        into a Datum object.
    """
    def input_to_datum(self, dag_input):
        raise NotImplementedError(f"Need to implement `input_to_datum` for {type(self)}")


