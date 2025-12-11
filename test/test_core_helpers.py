
from dagger.abstract import AbstractTask, TaskState, AbstractManager
from dagger.core.helpers import construct_edge_lists
from collections import defaultdict

class MinimalTask(AbstractTask):
    """
    A minimal/do-nothing implementation of base.AbstractTask,
    strictly for testing purposes.
    """
    def _run_logic(self):
        return

    def _interrupt_cleanup(self):
        return

    def _fail_cleanup(self):
        return

    def _verify_complete_logic(self):
        return False

    def _finished_successfully(self):
        return True

def test_edge_lists():

    # Construct a workflow of 10 tasks,
    # chained together (i.e., linear graph)
    t0 = MinimalTask("t0")
    t = t0
    for i in range(9):
        t_new = MinimalTask(f"t{i+1}",
                            dependencies=[t]
                            )
        t = t_new

    edge_lists = construct_edge_lists(t)
    print(edge_lists)


