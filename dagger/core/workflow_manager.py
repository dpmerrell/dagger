"""
    core/workflow_manager.py
    (c) David Merrell 2025

    
"""

from dagger.core.helpers import construct_edge_lists
from dagger.abstract import AbstractManager

class WorkflowManager(AbstractManager):
    """
    Implementation of a WorkflowManager
    """

    def __init__(self, root_task, resources={}, start_node="START"):
        super().__init__(root_task)
        self.resources = resources

        # Keep an edge-list representation of the DAG
        self.edge_lists = construct_edge_lists(root_task, start_node=start_node) 

        # Maintain collections of Tasks in each state.
        self.waiting = set()
        self.running = []
        self.completed = set([start_node])
        self.failed = set()

    def run(self):
        """
        A loop that continually checks for available
        jobs and launches them, subject to resource
        constraints.
        """
        while len(self.waiting) > 0:
            # Check tasks in 'running' list to see if
            # any of them finished

            # If a task is COMPLETE, then 
            # move it to `completed` and check whether
            # any of its children are ready to run.
            
            # If a child is ready to run, move it to 
            # the `running` list and run it.
            # TODO figure out loky "reusable executor"
            pass

