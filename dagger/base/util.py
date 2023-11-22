

from uuid import uuid4
import datetime

DAGGER_START_FLAG = "__DAGGER_START__"
DAGGER_END_FLAG = "__DAGGER_END__"

def now_timestamp():
    return datetime.datetime.now().isoformat()

def min_timestamp():
    return datetime.datetime.min.isoformat()

def generate_uid():
    return uuid4()


