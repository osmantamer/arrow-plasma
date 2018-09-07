import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np
from numpy import array
import random
import time

def getInt():
	start = time.time()

	f = open("<path_to_object_id_file>","rb")
	byte = f.read() # bytes for object_id
	client = plasma.connect("/tmp/plasma", "", 0)
	object_id = plasma.ObjectID(byte) # object_id
	[buffer2] = client.get_buffers([object_id])

	data = np.frombuffer(buffer2, dtype="uint32") # the array from c++. This line takes 97% of the time
	
	end = time.time() - start
	print("total time for ( %d ): %f" %(len(data),end))
