"""
    simple/task.py
    (c) David Merrell 2025

    Perhaps the simplest possible implementation
    of dagger's Task abstract base class.

    This isn't meant to be used in practice. Rather,
    it's a minimal working implementation that is used
    for testing, and as an illustrative example.

    A SimpleTask simply executes a python function.
    A SimpleTask's data (inputs, outputs) are stored
    in memory.
"""

from dagger.base import Task

class SimpleTask(Task):
    
    def run(self):
        return

    def terminate(self):
        return


