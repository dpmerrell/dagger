
from dagger.abstract.task import TaskState 
from dagger.core.task import Task
from dagger.core.datum import MemoryDatum

class MinimalCoreTask(Task):
    """
    This minimal implementation of Task executes a python
    function on python objects kept in memory.

    The user specifies a function on construction:
    * The function must receive kwargs corresponding
      to the task's inputs
    * The function must return a dictionary corresponding
      to the task's outputs
    """

    def __init__(self, *args, **kwargs):
        if "function" in kwargs:
            self.function = kwargs.pop("function")
        super().__init__(*args, **kwargs)

    def _fail_cleanup(self):
        return 

    def _initialize_outputs(self, outputs):
        result = {}
        for k in outputs:
            result[k] = MemoryDatum(parent=self)
        return result

    def _interrupt_cleanup(self):
        self.got_interrupted = True
        return

    def _quickhash(self):
        return id(self)

    def _execute(self):
        # Collect inputs
        func_inputs = {k: v.pointer for k,v in self.inputs.items()}
        # Run the function
        fn_outputs = self.function(**func_inputs)

        # Store the results in the output Datums
        for k in self.outputs:
            self.outputs[k].populate(fn_outputs[k])


def test_core_task():

    input_datum = MemoryDatum(pointer=[1,2,3,4])
   
    def sum_function(mylist=[]):
        return {"sum": sum(mylist)}

    def p1_function(mynumber=0):
        return {"p1": mynumber+1}

    def double_function(mynumber=0):
        return {"doubled": mynumber*2}

    def error_function(thing=None):
        raise ValueError("This function has an error!")
    
    def interrupt_function(thing=None):
        raise KeyboardInterrupt("This function got interrupted!")

    sum_task = MinimalCoreTask("sumtask", inputs={"mylist": input_datum},
                                          outputs={"sum": None},
                                          function=sum_function
                               )
    p1_task = MinimalCoreTask("p1task", inputs={"mynumber": sum_task.outputs["sum"]},
                                        outputs={"p1": None},
                                        function=p1_function
                             )
    double_task = MinimalCoreTask("double", inputs={"mynumber": p1_task.outputs["p1"]},
                                            outputs={"doubled": None},
                                            function=double_function
                                 )
    bad_task = MinimalCoreTask("bad_task", dependencies=[sum_task], 
                                           function=error_function
                              )

    interrupt_task = MinimalCoreTask("interrupt_task", dependencies=[sum_task], 
                                                       function=interrupt_function
                                    ) 

    assert sum_task.state == TaskState.WAITING
    sum_task.run()
    assert sum_task.state == TaskState.COMPLETE
    p1_task.run()
    double_task.run()

    true_sum = sum([1,2,3,4])
    true_p1 = true_sum + 1
    true_double = true_p1 * 2
    assert sum_task.outputs["sum"].pointer == true_sum
    assert p1_task.outputs["p1"].pointer == true_p1
    assert double_task.outputs["doubled"].pointer == true_double

    assert bad_task.state == TaskState.WAITING
    # This task should raise an error
    try:
        bad_task.run()
    except ValueError:
        assert True
    else:
        assert False
    assert bad_task.state == TaskState.FAILED

    # This task should be interrupted
    interrupt_task.run()
    assert interrupt_task.got_interrupted
    assert interrupt_task.state == TaskState.WAITING

