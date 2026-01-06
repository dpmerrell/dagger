
from dagger.abstract import AbstractTask, TaskState, AbstractManager
from dagger.abstract.helpers import resources_available, \
                                    decrement_resources, increment_resources
from collections import defaultdict
from copy import copy

###################################
# RESOURCE HELPERS
###################################

def test_resources():

    supply = {"processes": 10,
              "threads": 20,
              "mem_GB": 20,
              "connections": 4
              }

    d1 = {"processes": 1,
          "threads": 2,
          }
    d2 = {"processes": 20
          }

    # Resources available
    assert resources_available(supply, d1)
    assert not resources_available(supply, d2)

    # Decrement resources
    supply_dec = copy(supply)
    supply_dec = decrement_resources(supply_dec, d1)
    assert supply_dec["processes"] == 9
    assert supply_dec["threads"] == 18
    assert supply_dec["mem_GB"] == 20
    assert supply_dec["connections"] == 4

    # Increment resources
    supply_dec = increment_resources(supply_dec, d1)
    assert supply_dec == supply


