"""
    workflow.py
    David Merrell (c) 2023

    A Workflow object represents a computational workflow;
    a DAG of tasks that pass inputs/outputs to each other.

    A Workflow has the following attributes:
        * parents:  a dictionary (task UID) -> (task UID) list of UID representing the DAG
        * children: a dictionary UID -> list of UID representing the DAG
            - NOTE: these ^^^ are equivalent representations of the DAG;
                    care must be taken to properly maintain them.
        * tasks:    a dictionary of Tasks, keyed by their UIDs
        * data:     a dictionary of Datum objects, keyed by their UIDs

    A Workflow has the following methods:
        * add_task
"""
