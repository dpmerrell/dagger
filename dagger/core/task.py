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

from pathlib import Path
import subprocess
import platform
import inspect
import pickle

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

    def _collect_inputs(self):
        """
        Given a FunctionTask's dictionary of input Datums,
        return a dictionary of python objects to be passed as
        kwargs to the function.
    
        TODO handle *list* inputs (i.e., for a `reduce`-like function)
        """
        result = {}
        os_name = platform.system()
    
        for k, v in self.inputs.items():
            # Input is a MemoryDatum
            if isinstance(v, MemoryDatum):
                result[k] = v.pointer
            # Input is a FileDatum
            elif isinstance(v, FileDatum):
                if is_valid_filepath(v.pointer, platform=os_name):
                    # If it points to a pickle, then
                    # load the pickle
                    if Path(v.pointer).suffix.lower() == ".pkl":
                        with open(v.pointer, "rb") as f:
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
                raise ValueError(f"Input {k}: {v} is not a Datum")
    
        return result

    def _run_logic(self, collected_inputs):
        output_dict = self.function(**collected_inputs)
        for k, datum in self.outputs.items():
            if isinstance(datum, MemoryDatum):
                datum.populate(output_dict[k])
            # For FunctionTask, whenever an output is a 
            # FileDatum, we treat it as a pkl file.
            elif isinstance(datum, FileDatum):
                with open(datum.pointer, "wb") as f:
                    pickle.dump(output_dict[k], f)
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
              is_valid_filepath(v, platform=os_name) and \
              Path(v).suffix.lower() == ".pkl":
                result[k] = FileDatum(parent=self,
                                      pointer=v)
            # Check if this is a FileDatum
            elif isinstance(v, FileDatum):
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

    def __init__(self, identifier, script, **kwargs):

        if not isinstance(script, str):
            raise ValueError("`script` must be a string")
        self.script = script

        super().__init__(identifier, **kwargs)
        return


    def _initialize_outputs(self, output_dict):
        """
        Convert the values of output_dict to Datum objects.
        The values must always be strings containing
        valid filepaths.

        TODO: handle a more arbitrary class of filepath glob *patterns*
              matching one or more files
        """
        result = {}
        os_name = platform.system() 
        for k, v in output_dict.items():
            if isinstance(v, str) and is_valid_filepath(v, platform=os_name):
                result[k] = FileDatum(parent=self,
                                      pointer=v)
            else:
                raise ValueError(f"Can't construct output '{k}': {v} for ScriptTask {self.identifer}. {v} is not a valid filepath.")

        return result

    def _quickhash(self):
        return hash((id(self), self.script))

    def _run_logic(self, collected_inputs):
        """
        Substitute the input and output strings into the
        script and execute it.

        By assumption, all inputs and outputs are FileDatums;
        the inputs are known to exist;
        and the outputs' existence will be checked later in the
        `Task.run()` method. So this is the complete logic.

        TODO: handle a more arbitrary class of filepath glob *patterns*
              for task outputs. In the general case this will involve
              (i) `find`ing the outputs after execution; and 
              (ii) implementing a `DatumList` Datum type that enables
                   collecting multiple files AND map operations
        """
        outputs = {k: v.pointer for k, v in self.outputs.items()}
        script = self.script.format(**{**collected_inputs, **outputs})
        subprocess.run(script, shell=True, check=True)

        return

    def _interrupt_cleanup(self):
        return

    def _fail_cleanup(self):
        return
    
    def collect_inputs(self):
        """
        Given a ScriptTask's dictionary of `inputs`,
        return a dictionary of strings to be substituted
        into the script.
        
        For now, we require all inputs to be FileDatums.
    
        TODO handle *list* inputs (i.e., for a `reduce`-like task)
        """
        result = {}
        os_name = platform.system()
    
        for k, v in self.inputs.items():
            # Input is a FileDatum
            if isinstance(v, FileDatum):
                if is_valid_filepath(v.pointer, platform=os_name):
                    result[k] = v.pointer
                else:
                    raise ValueError(f"Input filepath {k}: {v.pointer} is ill-formed")
            elif isinstance(v, MemoryDatum):
                raise ValueError(f"Input {k}: {v} is not a FileDatum")
            elif not isinstance(v, AbstractDatum):
                raise ValueError(f"Input {k}: {v} is not a FileDatum")
    
        return result


