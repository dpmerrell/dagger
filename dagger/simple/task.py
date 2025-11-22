"""
    simple/task.py
    (c) David Merrell 2025

    Perhaps the simplest possible implementation
    of dagger's Task abstract base class.

    This isn't meant to be used in practice. Rather,
    it's a minimal working implementation that is used
    as an illustrative example.

    Each SimpleTask simply executes a python function.
    We require the function to return a dictionary
    containing its outputs; keys are output names and
    values are the outputs themselves. On construction,
    the SimpleTask must receive a dictionary mapping
    function inputs to (a) the outputs of other tasks
    or (b) other data; as well as a collection of 
    the function's output names.

    A SimpleTask's data (input, output) is stored
    in memory as attributes.
"""

from dagger.base import Task

def _collect_dependencies(input_mapping):
    """
        Collect all tasks found in the input mapping.

        Additionally, raise an error if input specifies
        a task output that shouldn't exist.
    """
    dependencies = []
    dependency_names = set()
    for k, v in input_mapping.items():
        # input maps to a specific task output whenever
        # v = (task, output_name)
        if isinstance(v, tuple) and (len(v) == 2):
            task = v[0]
            output = v[1]
            if isinstance(task, SimpleTask):
                if output not in task.outputs:
                    raise ValueError(f"{output} is not the name of an output from {task}")
                else:
                    if task.identifier not in dependency_names:
                        dependencies.append(task)
                        dependency_names.add(task.identifier)

    return dependencies


class SimpleTask(Task):
    
    def __init__(self, identifier, fn, input_mapping={}, output_names=set()):

        # Populate the identifier and dependencies
        self.identifier = identifier
        self.dependencies = _collect_dependencies(input_mapping)

        # Populate the function and set up its
        # inputs and outputs
        self.fn = fn
        self.input_mapping = input_mapping
        self.inputs = None
        self.output_names
        self.outputs = None
        return

    def _run_logic(self):
        """
        Collect the Task's inputs;
        execute the Task's python function;
        and validate the Task's outputs.
        """
        self._collect_inputs()
        self.outputs = self.fn(self.inputs)
        self._validate_outputs()

    def _interrupt_cleanup(self):
        """
        On interrupt, clear the Task's inputs and outputs.
        """
        self.inputs = None
        self.outputs = None

    def _fail_cleanup(self):
        """
        No particular cleanup necessary on failure
        """
        return

    def _collect_inputs(self):
        """
        At runtime, collect the inputs of this task.
        (They should all exist at this point.)
        """
        inputs = {}
        for k, v in self.input_mapping.items():
            if isinstance(v, tuple) and (len(v) == 2):
                if isinstance(v[0], SimpleTask):
                    inputs[k] = v[0].outputs[v[1]]
                    continue
            inputs[k] = v
        self.inputs = inputs


