
from dagger.core import FunctionTask, PklTask,\
                        FileDatum, MemoryDatum

from dagger.abstract import AbstractDatum, DatumState, TaskState
from pathlib import Path

###########################################
# Some simple functions
###########################################
def plus1(x=0):
    return {"output": x + 1}

def double(x=0):
    return {"output": 2 * x}

def multiply(x=0, y=0):
    return {"output": x * y}

def sum_and_diff(x=0, y=0):
    return {"sum": x + y,
            "diff": x - y
            }

def sum_and_diff2(x=0, y=0):
    # Slightly different
    return {"sum": x + y,
            "diff": x - y
            }

###########################################
# TESTS: FunctionTask
########################################### 

def test_functiontask_constructor():

    x = 3
    y = 2
    xdatum = MemoryDatum(pointer=x)
    ydatum = MemoryDatum(pointer=y)

    # Construct some FunctionTasks and check their properties
    t0 = FunctionTask("t0", sum_and_diff, inputs={"x": xdatum,
                                                  "y": ydatum
                                                  },
                                          outputs={"sum": None,
                                                   "diff": None
                                                   }
                     )
    # Output initialization
    assert isinstance(t0.outputs["sum"], MemoryDatum)
    assert t0.outputs["sum"].state == DatumState.EMPTY 
    assert isinstance(t0.outputs["diff"], MemoryDatum)
    assert t0.outputs["diff"].state == DatumState.EMPTY 
    
    # Check quickhash properties
    hash1 = t0.quickhash
    t0.function = sum_and_diff2
    hash2 = t0._quickhash()
    assert hash2 != hash1
    t0.function = sum_and_diff
    assert t0._quickhash() == hash1

    # What happens when we do this?
    t0.run()

from test.test_abstract import MinimalManager

def test_functiontask_workflow():

    x = 3
    xdatum = MemoryDatum(pointer=x)

    # Create a "diamond" DAG
    t0 = FunctionTask("t0", plus1, inputs={"x": xdatum},
                                   outputs={"output": None}
                      )
    t1 = FunctionTask("t1", plus1, inputs={"x": t0.outputs["output"]},
                                   outputs={"output": None}
                      )
    t2 = FunctionTask("t2", double, inputs={"x": t0.outputs["output"]},
                                    outputs={"output": None}
                      )
    t3 = FunctionTask("t3", multiply, inputs={"x": t1.outputs["output"],
                                              "y": t2.outputs["output"]
                                             },
                                      outputs={"output": None}
                     )

    m = MinimalManager(t3)
    m.run()
    assert t0.outputs["output"].pointer == 4
    assert t1.outputs["output"].pointer == 5
    assert t2.outputs["output"].pointer == 8
    assert t3.outputs["output"].pointer == 40

    return

###########################################
# TESTS: PklTask
########################################### 

import pickle

def test_pkltask_constructor():

    x = 3
    y = 2
    xdatum = MemoryDatum(pointer=x)
    ydatum = MemoryDatum(pointer=y)

    # Construct some FunctionTasks and check their properties
    t0 = PklTask("t0", sum_and_diff, inputs={"x": xdatum,
                                             "y": ydatum
                                             },
                                     outputs={"sum": "test/sum.pkl",
                                              "diff": "test/diff.pkl"
                                              }
                )
    # Output initialization
    assert isinstance(t0.outputs["sum"], FileDatum)
    assert t0.outputs["sum"].state == DatumState.POPULATED
    assert t0.outputs["sum"].pointer == "test/sum.pkl"
    assert isinstance(t0.outputs["diff"], FileDatum)
    assert t0.outputs["diff"].state == DatumState.POPULATED
    assert t0.outputs["diff"].pointer == "test/diff.pkl"
    
    # Check quickhash properties
    hash1 = t0.quickhash
    t0.function = sum_and_diff2
    hash2 = t0._quickhash()
    assert hash2 != hash1
    t0.function = sum_and_diff
    assert t0._quickhash() == hash1

    # What happens when we do this?
    t0.run()

    assert Path("test/sum.pkl").exists()
    assert Path("test/diff.pkl").exists()

    assert isinstance(t0.outputs["sum"], FileDatum)
    assert t0.outputs["sum"].state == DatumState.AVAILABLE
    assert t0.outputs["sum"].pointer == "test/sum.pkl"
    assert isinstance(t0.outputs["diff"], FileDatum)
    assert t0.outputs["diff"].state == DatumState.AVAILABLE
    assert t0.outputs["diff"].pointer == "test/diff.pkl"
   
    # Clean up
    Path("test/sum.pkl").unlink()
    Path("test/diff.pkl").unlink()


def test_pkltask_workflow():

    x = 3
    xdatum = MemoryDatum(pointer=x)
    #xdatum.verify_available()

    # Create a "diamond" DAG
    t0 = PklTask("t0", plus1, inputs={"x": xdatum},
                              outputs={"output": "test/t0_output.pkl"}
              )
    t1 = PklTask("t1", plus1, inputs={"x": t0.outputs["output"]},
                              outputs={"output": "test/t1_output.pkl"}
              )
    t2 = PklTask("t2", double, inputs={"x": t0.outputs["output"]},
                               outputs={"output": "test/t2_output.pkl"}
              ) 
    t3 = PklTask("t3", multiply, inputs={"x": t1.outputs["output"],
                                         "y": t2.outputs["output"]
                                        },
                                 outputs={"output": "test/t3_output.pkl"}
                     )

    m = MinimalManager(t3)
    m.run()
    assert pickle.load(open(t0.outputs["output"].pointer, "rb")) == 4
    assert pickle.load(open(t1.outputs["output"].pointer, "rb")) == 5
    assert pickle.load(open(t2.outputs["output"].pointer, "rb")) == 8
    assert pickle.load(open(t3.outputs["output"].pointer, "rb")) == 40

    Path(t2.outputs["output"].pointer).unlink()
    Path(t3.outputs["output"].pointer).unlink()

    m.end_task.sync()
    assert t0.state == TaskState.COMPLETE
    assert t1.state == TaskState.COMPLETE
    assert t2.state == TaskState.WAITING
    assert t3.state == TaskState.WAITING

    Path(t0.outputs["output"].pointer).unlink()
    Path(t1.outputs["output"].pointer).unlink()

    return


