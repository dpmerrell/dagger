"""
    dag.py
    David Merrell (c) 2023
"""


"""
    A DAG object contains the (interconnected) Tasks and Datums.

    A DAG has the following attributes:
        * tasks: a dictionary-like collection of Tasks, keyed by their UIDs
        * data:  a dictionary-like collection of Datum objects, keyed by their UIDs
        * edgesets: a dictionary: task UIDs -> sets of task UIDs.
                                  This is an alternative encoding of the DAG
                                  (the Tasks themselves already store their parents),
                                  much more convenient for certain algorithms.
    
    (These may or may not actually be python dictionaries; in inheritors,
     they could be any abstract hash table or key-value store.)

    A DAG has the following methods:
        * set_task
        * get_task
        * set_datum
        * get_datum
        * sync_data
        * dag_input_to_datum

    Properly constructed, a DAG should have the following properties:
        * Every task is downstream of the DaggerStartTask
        * Every task is upstream of the DaggerEndTask
"""
class DAG:

    """
        DAG(tasks, data)

        This minimalist constructor will probably not be used
        in practice. DAG construction will require some care, 
        and will entail a somewhat more complex interface.
    """
    def __init__(self, tasks, data):

        self.tasks = tasks
        self.data = data
        self.edgesets = self.compute_task_edgesets()
        return

    ####################################
    # These usually don't need to be 
    # modified by inheritors
    """
        Add (or update) a Task
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

    """
        Sync all of the Datum objects in this DAG 
    """
    def sync_data(self):
        for datum in self.data.values():
            datum.sync()

    """
        Sync the state for all Task objects in this DAG 
    """
    def sync_tasks(self):
        for task in self.tasks.values():
            task.sync_state()
    """
        Create an edge-set representation of the tasks.
    """
    def compute_task_edgesets(self):
        edgesets = {}
        for task_uid, task in self.tasks.items():
            for parent_uid in task.get_parent_task_uids():
                if parent_uid in edgesets.keys():
                    edgesets[parent_uid].add(task_uid)
                else:
                    edgesets[parent_uid] = set([task_uid])

        return edgesets

    """
        dag_input_to_datum(self, dag_input)

        Translates a DAG input (e.g., a file path, URL, python object)
        into a Datum object.
    """
    def dag_input_to_datum(self, dag_input):
        raise NotImplementedError(f"Need to implement `dag_input_to_datum` for {type(self)}")


