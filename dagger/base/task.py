"""
    task.py
    David Merrell (c) 2023

    Definition of Task base class
"""

import uuid

"""
    A Task has the following attributes:
        * inputs (a dictionary string -> Datum uid)
        * outputs (a dictionary string -> Datum uid)
        * uid (a unique identifier; for now, a UUID)
        * name (a string)
        * resources (
        * (inheritors may have other attributes as well)
    
    A Task has the following methods:
        * is_current(): returns a Bool indicating whether
                        the task's outputs are up-to-date
        * start_task(): starts the task, which runs in the background until 
                        (A) completion or (B) failure. 
                        In inheritors, this may include setup and teardown logic; restarts; etc.
        * kill_task(): kills the task. In inheritors, this would perform the interrupt
                       and any teardown logic
        * is_running(): returns a Bool indicating whether the task
                        is running.
"""
class Task:
    
    def __init__(self, inputs, outputs, name="", resources={}, **kwargs):

        self.inputs = inputs
        self.outputs = outputs
        self.name = name
        self.uid = uuid.uuid4()
        
        for k, v in kwargs.items():
            setattr(self, k, v)
        return

    def is_current(self):
        raise NotImplementedError(f"Need to implement `is_current` method for {type(self)}")

    def start_task(self, **kwargs):
        raise NotImplementedError(f"Need to implement `start_task` method for {type(self)}")

    def kill_task(self, **kwargs):
        raise NotImplementedError(f"Need to implement `kill_task` method for {type(self)}")

    def is_running(self):
        raise NotImplementedError(f"Need to implement `is_running` method for {type(self)}")
       

"""
    DaggerStartTask is a "dummy" task indicating the start of a DAG.
    
    All DAG inputs are _outputs_ of the DaggerStartTask.

    The DaggerStartTask always has a special name and uid: "__DAGGER_START__".
"""
class DaggerStartTask(Task):

    def __init__(self, dag_inputs={}):
        super(self).__init__(self, {}, dag_inputs, 
                                       name="__DAGGER_START__", 
                                       uid="__DAGGER_START__")

    def is_current(self):
        return True

    def start_task(self, **kwargs):
        return

    def kill_task(self, **kwargs):
        return

    def is_running(self):
        return False


"""
    DaggerEndTask is a "dummy" task indicating the end of a DAG.
    
    The DaggerEndTask has no outputs; and its inputs are the final outputs of the DAG.

    The DaggerEndTask always has a special name and uid: "__DAGGER_END__".
"""
class DaggerEndTask(Task):

    def __init__(self, dag_outputs={}):
        super(self).__init__(self, dag_outputs, {} 
                                   name="__DAGGER_END__", 
                                   uid="__DAGGER_END__")

    def is_current(self):
        return False  # Will always need to check the DAG outputs,
                      # and the tasks that create them.

    def start_task(self, **kwargs):
        return

    def kill_task(self, **kwargs):
        return

    def is_running(self):
        return False


