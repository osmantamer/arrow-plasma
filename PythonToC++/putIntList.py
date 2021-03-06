import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np
from numpy import array
from random import randint
import time

def generateInput(power):
	f = open("<path_to_file_to_generate_input>")
	fileData = f.read().split("\n")
	del fileData[-1]
	fileData = [int(s) for s in fileData]
	return fileData

def putInt(size):
	data = generateInput(size)
	start = time.time()
	client = plasma.connect("/tmp/plasma", "", 0)

	random_bytes = np.random.bytes(20)
	object_id = plasma.ObjectID(random_bytes)
	membuf = client.create(object_id,len(data)*8)
#.newbyteorder('>')
	dt = np.dtype("int")
	array = np.frombuffer(membuf, dtype=dt)

	array.setfield(data,dtype=dt) #--> %97 of the time

	client.seal(object_id)
	end = time.time() - start
	f = open("<path_for_object_id_file_of_your_data_in_plasma>", "wb")
	f.write(object_id.binary())
	print("Time for %d: %f"%(size,end))


putInt(23)
