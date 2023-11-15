"""
    task.py
    David Merrell (c) 2023

    A Task has the following attributes:
        * uid
        * state {Complete, Incomplete, Failed}
        * inputs
        * outputs
        * (inheritors may have other attributes as well)
    
    A Task has the following methods:
        * run_task (in inheritors, this may include 
                    setup and teardown logic; 
                    restarts; etc.)
        * kill_task 

"""

class Task:
    def __init__(self):
        return
