"""
    core/task.py
    (c) David Merrell 2025

    Core implementations of AbstractTask:
    * FunctionTask: executes a python function.
                    Inputs and outputs are objects in memory.
    * PklTask: executes a python function.
               Inputs and outputs are local PKL files.
    * ScriptTask: executes a command line script.
                  Inputs and outputs are local files.

"""

from dagger.abstract import AbstractTask, TaskState
from dagger.core import FileDatum, MemoryDatum
from pathvalidate import is_valid_filepath
import platform

import inspect

class FunctionTask(AbstractTask):
    """
    A task that executes a python function.
    Outputs are objects in memory, by default.

    The function must receive kwargs with keys matching 
    those of the Task's `inputs` dictionary.

    The function must return a dictionary with keys
    matching those of the Task's `outputs` dictionary.
    """

    def __init__(self, identifier, function, **kwargs):
        
        if not callable(function):
            raise ValueError(f"`function` argument {function} is not callable")
        self.function = function

        super().__init__(identifier, **kwargs)


    def _initialize_outputs(self, output_dict):
        result = {}
        os_name = platform.system() 
        for k, v in output_dict.items():

            # Check if this should be a MemoryDatum
            if v is None:
                result[k] = MemoryDatum(parent=self)
            elif isinstance(v, MemoryDatum):
                v.parent = self 
                result[k] = v
            # Can override default behavior by
            # explicitly providing a FileDatum
            elif isinstance(v, FileDatum):
                v.parent = self 
                result[k] = v
            else:
                raise ValueError(f"Can't construct output '{k}': {v} for FunctionTask {self.identifier}")
        return result 

    def _quickhash(self):
        return hash((id(self), inspect.getsource(self.function)))

    def _run_logic(self):
        inputs = collect_inputs(self, self.inputs)
        output_dict = self.function(**inputs)
        for k, datum in self.outputs.items():
            if isinstance(datum, MemoryDatum):
                datum.populate(output_dict[k])
            elif isinstance(datum, FileDatum):
                datum.populate
        return

    def _interrupt_cleanup(self):
        for k, datum in self.outputs:
            datum.clear()
        return

    def _fail_cleanup(self):
        for k, datum in self.outputs.items():
            datum.clear()
        return
    

class PklTask(FunctionTask):
    """
    A task that executes a python function.
    Outputs are local Pkl files by default.
    
    The function must receive kwargs with keys matching 
    those of the Task's `inputs` dictionary.

    The function must return a dictionary with keys
    matching those of the Task's `outputs` dictionary.
    """
    def _initialize_outputs(self, output_dict):
        result = {}
        os_name = platform.system() 
        for k, v in output_dict.items():
            # Check if this is the path for a pkl file
            if isinstance(v, str) and \
              is_valid_pathname(v, platform=os_name) and \
              Path(v).suffix.lower() == "pkl":
                result[k] = FileDatum(parent=self,
                                      pointer=v)

            # Check if this is a FileDatum
            if isinstance(v, FileDatum):
                v.parent = self 
                result[k] = v
            # Can override default behavior by
            # explicitly providing a MemoryDatum
            elif isinstance(v, MemoryDatum):
                v.parent = self 
                result[k] = v
            else:
                raise ValueError(f"Can't construct output '{k}': {v} for FunctionTask {self.identifier}")
        return result 


class ScriptTask(AbstractTask):
    """
    A task that executes a command line script.
    Outputs are local files.
    """
    def _initialize_outputs(self, output_dict):
        return {"output": MemoryDatum(parent=self)}

    def _quickhash(self):
        return self.identifier

    def _run_logic(self):
        self.outputs["output"].populate(f"result of task {self.identifier}")
        return

    def _interrupt_cleanup(self):
        return

    def _fail_cleanup(self):
        return
    

def collect_inputs(task, input_dict):
    """
    Given a Task and its dictionary of input Datums,
    return a dictionary of objects appropriate for the
    execution of that Task.

    For example, collect_inputs for a FunctionTask 
    should return a dictionary of python objects;
    and collect_inputs for a ScriptTask should return
    a dictionary of strings.
    """
    result = {}
    os_name = platform.system()

    # Behavior for FunctionTasks
    if isinstance(task, FunctionTask):
        for k, v in input_dict.items():
            # Input is a MemoryDatum
            if isinstance(v, MemoryDatum):
                result[k] = v.pointer
            # Input is a FileDatum
            elif isinstance(v, FileDatum):
                if is_valid_filepath(v.pointer, platform=os_name):
                    # If it points to a pickle, then
                    # load the pickle
                    if Path(v.pointer).suffix.lower() == "pkl":
                        with open(v.pointer, "r") as f:
                            result[k] = pickle.load(f)
                    # Otherwise, simply return the filepath.
                    else:
                        result[k] = v.pointer
                # Raise an error if the input is not a valid
                # filepath
                else:
                    raise ValueError(f"Input filepath {k}: {v.pointer} is ill-formed")
            # If the input is not a Datum at all, then
            # we simply provide the python object
            elif not isinstance(v, AbstractDatum):
                result[k] = v

    return result
