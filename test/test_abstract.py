
from dagger.abstract import AbstractTask, TaskState, AbstractManager
from dagger.core import MemoryDatum

class MinimalTask(AbstractTask):
    """
    A minimal/do-nothing implementation of base.AbstractTask,
    strictly for testing purposes.
    """
    def _initialize_outputs(self, output_dict):
        return {"output": MemoryDatum(parent=self)}

    def _quickhash(self):
        return self.identifier

    def _collect_inputs(self):
        return {k: v.pointer for k, v in self.inputs.items()}

    def _run_logic(self, collected_inputs):
        self["output"].populate(f"result of task {self.identifier}")
        return

    def _interrupt_cleanup(self):
        return

    def _fail_cleanup(self):
        return

####################################
# TASK CONSTRUCTION AND COMPLETION
####################################
def test_base_task():
    # Construct three tasks; 
    # * t2 `depends` on t1
    # * t3 uses the `output` of t1; and `depends` on t2
    t1 = MinimalTask("t1")
    t2 = MinimalTask("t2", dependencies=[t1])
    t3 = MinimalTask("t3", inputs={"input": t1["output"]},
                           dependencies=[t2])

    # Check basic properties of the constructed tasks
    assert t2.identifier == "t2"
    assert t2.dependencies[0].identifier == "t1"

    assert t3.identifier == "t3"
    print([d.identifier for d in t3.dependencies])
    assert set(t3.dependencies) == set([t1, t2])

    # If we run t1, then t2 should be ready
    assert not t2.is_ready()
    t1.run()
    assert t1.state == TaskState.COMPLETE
    assert t2.is_ready()
    
    # t3 should not be ready until t2 has run;
    # and should raise an error if we try running it.
    assert not t3.is_ready()
    try:
        t3.run()
    except RuntimeError:
        assert True
    else:
        assert False
    assert t3.state == TaskState.WAITING

    # Finally, run t2 and then t3.
    t2.run()
    assert t3.is_ready()
    t3.run()
    assert t3["output"].pointer == "result of task t3"

#####################
# TASK FAILURE 
#####################
class ExceptionTask(MinimalTask):
    """
    This task simply raises an exception
    when we run it.
    """
    def _run_logic(self, collected_inputs):
        raise Exception("Exception Task!")
        return

    def _fail_cleanup(self):
        self.failed = True

def test_base_task_failure():
    # When we run this task, it should go into
    # a FAILED state
    te = ExceptionTask("te")
    try:
        te.run()
    except:
        assert te.failed
        assert te.state == TaskState.FAILED
    else:
        assert False

#####################
# TASK INTERRUPTION
#####################
class InterruptTask(MinimalTask):
    """
    This task simply raises a KeyboardInterrupt
    when we run it.
    """
    def _run_logic(self, collected_inputs):
        raise KeyboardInterrupt("Keyboard interrupt!")
        return

    def _interrupt_cleanup(self):
        self.interrupted = True
        return
    
def test_base_task_interrupt():
    # When we run this task, it should go back into
    # a WAITING state
    ti = InterruptTask("ti")
    ti.run()
    assert ti.interrupted
    assert ti.state == TaskState.WAITING


###############################################
# WORKFLOW MANAGER
###############################################
class MinimalManager(AbstractManager):
    """
    A minimal/do-nothing implementation of base.AbstractManager,
    strictly for testing purposes.
    """

    def _choose_tasks(self, ready_tasks):
        return ready_tasks[:]

    def _get_running_task_state(self, task):
        return task.state

    def _interrupt(self):
        return

    def _launch_task(self, task):
        print(f"Task {task.identifier} got launched!")
        task.run()
        return

    def _wrapup_task(self, task):
        return


def test_abstract_manager():

    # Construct a workflow of 10 tasks,
    # chained together (i.e., linear graph)
    t0 = MinimalTask("t0")
    t = t0
    for i in range(9):
        t_new = MinimalTask(f"t{i+1}",
                            inputs={"input": t["output"]}
                            )
        t = t_new
    m = MinimalManager(t)

    # Check basic properties of the constructed DAG
    assert m.end_task.identifier == "t9"
    try:
        m.validate_dag()
    except:
        assert False
    
    # Introduce a loop into the workflow.
    # Ensure it fails to validate.
    t0.dependencies = [t]
    try:
        m.validate_dag()
    except ValueError:
        assert True
    else:
        assert False

    # Remove the loop. Try running
    # the DAG.
    t0.dependencies = []
    m.run()
    
def test_manager_initialization():

    # Create a "diamond" DAG
    t0 = MinimalTask("t0")
    t1 = MinimalTask("t1", inputs={"input": t0["output"]})
    t2 = MinimalTask("t2", inputs={"input": t0["output"]})
    t3 = MinimalTask("t3", inputs={"input1": t1["output"],
                                   "input2": t2["output"]}
                     )
    # Create a WorkflowManager
    m = MinimalManager(t3)

    # Running it should set all tasks to COMPLETE
    m.run()
    assert t0.state == TaskState.COMPLETE
    assert t1.state == TaskState.COMPLETE
    assert t2.state == TaskState.COMPLETE
    assert t3.state == TaskState.COMPLETE

    # Forcibly mark one task as FAILED (bypassing the state machine,
    # since COMPLETE → FAILED is not a valid transition).
    # This simulates inspecting behaviour of initialize_workflow_state
    # when a task is already in FAILED state.
    t2._state = TaskState.FAILED
    m.initialize_workflow_state()
    assert t0.state == TaskState.COMPLETE
    assert t1.state == TaskState.COMPLETE
    assert t2.state == TaskState.FAILED
    assert t3.state == TaskState.WAITING

    # The state of the WorkflowManager should
    # reflect this situation
    assert m.waiting == set([t3])
    print([t.identifier for t in m.complete])
    print([t0.identifier, t1.identifier])
    assert m.complete == set([t0, t1])
    assert m.failed == set([t2])

    # 

