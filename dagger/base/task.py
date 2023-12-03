"""
    task.py
    David Merrell (c) 2023

    Definition of Task base class
"""

from dagger.base.util import generate_uid, DAGGER_START_FLAG, DAGGER_END_FLAG
from dagger.base.util import TaskState 

"""
    A Task has the following attributes:
        * dag:       a pointer to the DAG containing this task
        * inputs:    a dictionary: name -> Datum UID 
        * outputs:   a dictionary: name -> Datum UID 
        * uid:       a unique identifier, generated automatically during
                     construction by `compute_uid`.
        * name:      a string (ONLY for human readability)
        * resources: a dictionary of arbitrary resource requirements for this task
        * state:     Exactly one of: 
                     {TaskState.COMPLETE, TaskState.RUNNING, TaskState.NOT_COMPLETE, TaskState.FAILED} 
        * (inheritors may have other attributes as well)
    
    A Task has the following methods:
        * start():        Starts the task, which runs asynchronously until 
                          (A) completion or (B) failure. 
                          In inheritors, this may include setup and teardown logic; restarts; etc.
        * kill():         Kills the task. In inheritors, this would perform the interrupt
                          and any teardown logic
        * sync_state():   Updates the task's `state` to exactly one of: 
                          {TaskState.COMPLETE, TaskState.RUNNING, TaskState.NOT_COMPLETE, TaskState.FAILED} 
        * compute_uid():  Generate a string (or hash, or other object) such that
                          two tasks are considered redundant IFF their
                          UIDs are identical. Mathematically, these UIDs amount to
                          equivalence classes on the tasks.
                          `compute_uid()` should only use information available during 
                          DAG compilation -- NOT any of the execution state.
                          For example, it could generate a string representation of the
                          task's class AND inputs, so that two instances of the same 
                          Task class that receive identical inputs would be deemed
                          redundant.
        * get_input(name):  Get an input Datum from its name
        * get_output(name): Get an output Datum from its name
        * __getindex__(self, name): A convenience wrapper for get_output
        * to_datum(obj):   Convert an object (typically a task output)
                           into a Datum object
        * get_parent_task_uids(self): Get a set containing the UIDs of tasks whose
                                      outputs are the input of this task. 
"""
class Task:

    """
        Task(dag, inputs, outputs,
             name=None, resources={})
    """    
    def __init__(self, dag, inputs, outputs, name=None, 
                                             resources={},
                                             **kwargs):
        self.dag = dag
        self.inputs = inputs
        self.outputs = outputs
        self.resources = resources
        self.state = TaskStates.NOT_RUN

        # Set the name to the class name by default
        self.name = name
        if name is None:
            self.name = type(self).__name__
 
        for k, v in kwargs.items():
            setattr(self, k, v)

        return

    def start(self, **kwargs):
        raise NotImplementedError(f"Need to implement `start` method for {type(self)}")

    def kill(self, **kwargs):
        raise NotImplementedError(f"Need to implement `kill` method for {type(self)}")
    
    def sync_state(self):
        raise NotImplementedError(f"Need to implement `sync_state` method for {type(self)}")

    def compute_uid(self):
        raise NotImplementedError(f"Need to implement `compute_uid` method for {type(self)}")
      
    def get_input(self, name):
        return self.dag.data[self.inputs[name]]

    def get_output(self, name):
        return self.dag.data[self.outputs[name]]

    def __getitem__(self, output_name):
        return self.get_output(output_name)

    """
        Get the set of tasks immediately upstream of this task.
        More precisely: get the UIDs of tasks that generate the
        inputs of this task.
    """
    def get_parent_task_uids(self):
        parent_uids = set()
        for ipt_uid in self.inputs.values():
            parent_uids.add(self.dag.data[ipt_uid].parent_uid)
        return parent_uids


"""
    DaggerStartTask is a "dummy" task indicating the start of a DAG.

    **You should NEVER need to subclass this.** 

    All DAG inputs are _outputs_ of the DaggerStartTask.

    The DaggerStartTask always has a special name given by DAGGER_START_FLAG.
"""
class DaggerStartTask(Task):

    def __init__(self, dag, dag_inputs={}):
        super(self).__init__(self, dag, {}, dag_inputs,
                                            name=DAGGER_START_FLAG, 
                                            resources={}) 
        return

    def start(self, **kwargs):
        return

    def kill(self, **kwargs):
        return

    def sync_state(self):
        self.state = TaskState.COMPLETE


"""
    DaggerEndTask is a "dummy" task indicating the end of a DAG.
    
    **You should NEVER need to subclass this.** 

    The DaggerEndTask has no outputs; and its inputs are the final outputs of the DAG.

    The DaggerEndTask always has a special name given by DAGGER_END_FLAG.
"""
class DaggerEndTask(Task):

    def __init__(self, dag, dag_outputs={}):
        super(self).__init__(self, dag, dag_outputs, {},
                                            name=DAGGER_END_FLAG, 
                                            resources={}) 

    def start(self, **kwargs):
        return

    def kill(self, **kwargs):
        return
    
    def sync_state(self):
        self.state = TaskState.COMPLETE


