import uuid
import re

uuid1 = uuid.uuid1
node = uuid.getnode()

def uuidgen():
    return str(uuid1(node=node))

def setnode(x):
    global node
    prev = node
    try:
        node = int(re.sub(r'[:.-]', '', x), 16)
        # make sure it was valid
        uuidgen()
    except:
        node = prev
        raise
