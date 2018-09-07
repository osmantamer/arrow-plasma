import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np
from numpy import array
import random
import time
import math

def getStr():
	start = time.time()
	
	f = open("/home/osman/Desktop/pyjava/objid_fromJava","rb")
	byte = f.read()
	client = plasma.connect("/tmp/plasma", "", 0)
	object_id_data = plasma.ObjectID(byte)
	[buffer1] = client.get_buffers([object_id_data])

	data = buffer1.to_pybytes().decode('utf-8').split("\n")
	del data[-1] # since we split by "\n", last item might be empty string.
	
	end = time.time() - start
	print("Time for (%d): %f"%(len(data),end))
	print(data[:5])
	print(data[len(data)-5:len(data)])

getStr()

