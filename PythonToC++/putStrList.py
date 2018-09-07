import pyarrow as pa
import pyarrow.plasma as plasma
import numpy as np
from numpy import array
import math
import time


def generateInput(pw):
	f = open("<path_to_file_to_generate_input>","r")
	data = f.read().split("\n")
	del data[-1]
	return data


def putStr(size):
	data = generateInput(size)
	len_max = 0
	for i in data:
		if (len_max < len(i)):
			len_max = len(i)
	client = plasma.connect("/tmp/plasma", "", 0)

	start = time.time()
	"""
	Put longest item's length
	"""
	len_max = [len(max(data,key=len))]
	random_bytes_lenmax = np.random.bytes(20)
	object_id_lenmax = plasma.ObjectID(random_bytes_lenmax)
	membuf_lenmax = client.create(object_id_lenmax,8)
	dt = np.dtype(int)
	array = np.frombuffer(membuf_lenmax, dtype=dt)
	array.setfield(len_max,dtype=dt) 
	client.seal(object_id_lenmax)
	"""
	Put Data Array
	"""
	random_bytes = np.random.bytes(20)
	object_id = plasma.ObjectID(random_bytes)
	membuf = client.create(object_id,len(data)*len_max[0])

	dt = np.dtype("|S"+str(len_max[0]))
	array = np.frombuffer(membuf, dtype=dt)
	array.setfield(data,dtype=dt)

	client.seal(object_id)

	end = time.time() - start
	print("Total time ( %d ): %f"%(len(array),end))

	f = open("<path_for_object_id_file_of_your_data_in_plasma>", "wb")
	f.write(object_id.binary())
	f.close()
	f = open("<path_for_object_id_file_of_your_lenmax_in_plasma>","wb")
	f.write(object_id_lenmax.binary())
	f.close()

putStr(22)
