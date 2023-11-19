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
        * (inheritors may have other attributes as well)
    
    A Task has the following methods:
        * is_current(): returns a Bool indicating whether
                        the task's outputs are up-to-date
        * run_task(): runs the task. In inheritors, this may include 
                      setup and teardown logic; restarts; etc.
        * kill_task(): kills the task. In inheritors,
                       this would perform the interrupt
                       and any teardown logic
"""
class Task:
    
    def __init__(self, inputs, outputs, name="", **kwargs):

        self.inputs = inputs
        self.outputs = outputs
        self.name = name
        self.uid = uuid.uuid4()
        
        for k, v in kwargs.items():
            setattr(self, k, v)
        return

    def is_current(self):
        raise NotImplementedError(f"Need to implement `is_current` method for {type(self)}")

    def run_task(self, **kwargs):
        raise NotImplementedError(f"Need to implement `run_task` method for {type(self)}")

    def kill_task(self, **kwargs):
        raise NotImplementedError(f"Need to implement `kill_task` method for {type(self)")

    
