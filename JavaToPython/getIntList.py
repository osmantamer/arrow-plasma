import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np
from numpy import array
import random
import time


def getInt():
	start = time.time()

	f = open("/home/osman/Desktop/pyjava/objid_fromJava","rb")
	byte = f.read()
	client = plasma.connect("/tmp/plasma", "", 0)
	object_id = plasma.ObjectID(byte)
	
	[buffer2] = client.get_buffers([object_id]) # buffer

	dt = np.dtype("uint32").newbyteorder('>') # array's item types
	data = np.frombuffer(buffer2, dtype=dt) # the array from java. This line takes 97% of the time

	end = time.time() - start
	
	print("Time: "+str(end))

getInt()
