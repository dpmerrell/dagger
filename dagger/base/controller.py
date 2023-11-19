"""
    controller.py
    David Merrell (c) 2023
"""


"""
    A Controller manages the execution of a workflow.
    It performs all of the job scheduling and enforces
    resource constraints (if any).

    A Controller should have the following attributes:
        * dag
        * resources
    
    A Controller must have the following methods:
        * run_workflow
"""
class Controller:

    def __init__(self, dag, resources={}):

        self.dag = dag
        self.resources = resources

        return

    def run_workflow(self):
        raise NotImplementedError(f"Need to implement `run_workflow` method for {type(self)}")


