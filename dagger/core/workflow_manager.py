"""
    core/workflow_manager.py
    (c) David Merrell 2025

    
"""

from dagger.abstract import AbstractManager

class WorkflowManager(AbstractManager):
    """
    Implementation of a WorkflowManager
    """

    def __init__(self, root_task, resources={}):
        super().__init__(root_task)
        self.resources = resources
        
        # Maintain collections of Tasks in each state.
        self.waiting = set()
        self.running = []
        self.completed = set()
        self.failed = set()

    def run(self):
        """
        A loop that continually checks for available
        jobs and launches them, subject to resource
        constraints.
        """
        return
