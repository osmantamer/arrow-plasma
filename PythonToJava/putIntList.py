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
	for i in range(len(fileData)):
		fileData[i] = int(fileData[i])
	return fileData
	

def putInt(pw):	
	data = generateInput(pw)
	start = time.time()

	client = plasma.connect("/tmp/plasma", "", 0)

	random_bytes = np.random.bytes(20)
	object_id = plasma.ObjectID(random_bytes)
	membuf = client.create(object_id,len(data)*8)

	dt = np.dtype(int)
	array = np.frombuffer(membuf, dtype=dt)
	array.setfield(data,dtype=dt) #--> %97 of the time consumed by this line
	
	client.seal(object_id)
	end = time.time() - start

	print("Time for %d: %f"%(pw,end))

	f = open("<path_for_object_id_file_of_your_data_in_plasma>", "wb")
	f.write(object_id.binary())

putInt(17)
