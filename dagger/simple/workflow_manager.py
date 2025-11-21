"""
    simple/workflow_manager.py
    (c) David Merrell 2025

    Perhaps the simplest possible implementation
    of dagger's WorkflowManager abstract base class.

    This isn't meant to be used in practice. Rather,
    it's a minimal working implementation that is used
    for testing, and as an illustrative example.

    A SimpleManager simply executes its workflow
    one task at a time, without any parallelism.
    It performs a topological sort of the
    SimpleTasks and then runs them one at a time.
"""

from dagger.base import WorkflowManager

class SimpleManager(WorkflowManager):
    
    def run(self):
        return


