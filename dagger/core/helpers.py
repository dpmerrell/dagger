"""
    core/helpers.py
    (c) David Merrell 2025

    Helper functions for the "core" implementation.

    Many of these functions are meant to be used in 
    methods for Task, WorkflowManager, and Datum
    implementations.
"""

from collections import defaultdict

def collect_dependencies(input_dict, dependency_ls):
    """
    Collect all dependencies of a task, given 
    (A) a dictionary of input Datums and
    (B) a list of explicit Task dependencies.
    """
    parents = [inp.parent for inp in input_dict.values()]
    deps = set(dependency_ls) | set((p for p in parents if p is not None))
    return list(deps)


#########################################
# RESOURCE LOGIC
#########################################

def resources_available(res, query):
    """
    Return a bool indicating whether available resources (`res`)
    are sufficient to satisfy a demand (`query`).

    Whenever `res` is missing a key, we assume it has *infinite*
    supply of that key.

    Whenever `query` is missing a key, we assume it requires *zero* of
    that resource.
    """
    # If any of the query resources exceed
    # the available resources, then return False
    for qk, qv in query.items():
        if qk in res.keys():
            if res[qk] < qv:
                return False

    return True

def decrement_resources(res, decrement):
    """
    Update the resources dictionary with a 'decrement'.
    Return the updated resources dictionary.
    """
    for dk, dv in decrement.items():
        if dk in res.keys():
            res[dk] -= dv
    return res

def increment_resources(res, increment):
    """
    Update the resources dictionary with an 'increment'.
    Return the updated resources dictionary.
    """
    for ik, iv in increment.items():
        if ik in res.keys():
            res[ik] += iv
    return res


