import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np
from numpy import array
import random
import time

def getStr():
	start = time.time()

	f = open("<path_to_object_id_file>","rb")
	byte = f.read()[:20]
	client = plasma.connect("/tmp/plasma", "", 0)
	object_id = plasma.ObjectID(byte)
	[buffer2] = client.get_buffers([object_id])

	data = buffer2.to_pybytes().decode("utf-8").split("\x00")
	del data[-1] # since we split by "\x00" last item will be "" so we remove it
	end = time.time() - start

	print("total time for ( %d ): %f" %(len(data),end))
