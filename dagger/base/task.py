"""
    task.py
    David Merrell (c) 2023

    Definition of Task base class
"""

from util import generate_uid, DAGGER_START_FLAG, DAGGER_END_FLAG


"""
    A Task has the following attributes:
        * dag:       a pointer to the DAG containing this task
        * inputs:    a dictionary: name -> Datum UID 
        * outputs:   a dictionary: name -> Datum UID 
        * uid:       a unique identifier; for now, a UUID4
        * name:      a string (ONLY for human readability)
        * resources: a dictionary of arbitrary resource requirements for this task
        * (inheritors may have other attributes as well)
    
    A Task has the following methods:
        * start():         Starts the task, which runs asynchronously until 
                           (A) completion or (B) failure. 
                           In inheritors, this may include setup and teardown logic; restarts; etc.
        * kill():          Kills the task. In inheritors, this would perform the interrupt
                           and any teardown logic
        * is_running():    Returns a Bool indicating whether the task
                           is running.
        * get_input(name):  Get an input Datum from its name
        * get_output(name): Get an output Datum from its name
        * __getindex__(self, name): A convenience wrapper for get_output
        * to_datum(obj):   Convert an object (typically a task output)
                           into a Datum object
        * get_parent_task_uids(self)
"""
class Task:
    
    def __init__(self, dag, inputs, outputs, name="", resources={}, **kwargs):

        self.inputs = inputs
        self.outputs = outputs
        self.uid = generate_uid()
        self.name = name
        self.resources = resources
 
        for k, v in kwargs.items():
            setattr(self, k, v)
        return


    def start(self, **kwargs):
        raise NotImplementedError(f"Need to implement `start` method for {type(self)}")

    def kill(self, **kwargs):
        raise NotImplementedError(f"Need to implement `kill` method for {type(self)}")

    def is_running(self):
        raise NotImplementedError(f"Need to implement `is_running` method for {type(self)}")
      
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

    **You shouldn't ever need to subclass this.** 

    All DAG inputs are _outputs_ of the DaggerStartTask.

    The DaggerStartTask always has a special name and uid: "__DAGGER_START__".
"""
class DaggerStartTask(Task):

    def __init__(self, dag, dag_inputs={}):
        super(self).__init__(self, dag, {}, dag_inputs,
                                            resources={}, 
                                            name=DAGGER_START_FLAG, 
                                            uid=DAGGER_START_FLAG)
        return

    def start(self, **kwargs):
        return

    def kill(self, **kwargs):
        return

    def is_running(self):
        return False


"""
    DaggerEndTask is a "dummy" task indicating the end of a DAG.
    
    **You shouldn't ever need to subclass this.** 

    The DaggerEndTask has no outputs; and its inputs are the final outputs of the DAG.

    The DaggerEndTask always has a special name and uid: "__DAGGER_END__".
"""
class DaggerEndTask(Task):

    def __init__(self, dag, dag_outputs={}):
        super(self).__init__(self, dag, dag_outputs, {},
                                   resources={},
                                   name=DAGGER_END_FLAG, 
                                   uid=DAGGER_END_FLAG)

    def start(self, **kwargs):
        return

    def kill(self, **kwargs):
        return

    def is_running(self):
        return False


