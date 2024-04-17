import grpc
from grpc import StatusCode
from concurrent import futures
import concurrent.futures
from enum import Enum
import raft_pb2
import raft_pb2_grpc
import mapper
import master
import reducer
import random
import os
# import mapper_pb2
# from mapper_pb2_grpc import MapReduceStub

class Node:
	def __init__(self, id, port):
		self.id = id
		self.port = port
	
	def dumpMaper(self, mapperID, message):
		if not os.path.exists('Data/Mapper'):
			os.mkdir('Data/Mapper')
		if not os.path.exists('Data/Mapper/M' + str(mapperID)):
			os.mkdir('Data/Mapper/M' + str(mapperID))
		if not os.path.exists(f'Data/Mapper/M{mapperID}/mapperDump.txt'):
			open(f'Data/Mapper/M{mapperID}/mapperDump.txt', 'w').close()
		with open(f'Data/Mapper/M{mapperID}/mapperDump.txt', 'a') as f:
			f.write(message + '\n')
	def dumpReducer(self, reducerID, message):
		if not os.path.exists('Data/Reducers'):
			os.mkdir('Data/Reducers')
		if not os.path.exists(f'Data/Reducers/{reducerID}dump.txt'):
			open(f'Data/Reducers/{reducerID}dump.txt', 'w').close()
		with open(f'Data/Reducers/{reducerID}dump.txt', 'a') as f:
			f.write(message + '\n')

	def RequestPartitionData(self, request, context):
		self.dumpMaper(self.id, f"Request received for reducer {request.reducerID}")
		print(f"Request received for reducer {request.reducerID}")
		
		reducerID = request.reducerID
		data = mapper.getData(reducerID=reducerID, mapperID=self.id)
		return raft_pb2.RequestPartitionDataResponse(data=data)

	def map(self, request, context):
		try:
			print(f"Request received for mapper {self.id}")
			self.dumpMaper(self.id, f"Request received for mapper {self.id}")
			# # have a probability flag of 0.5 to simulate failure
			if random.random() < 0.5:
				print(f"Mapper {self.id} failed")
				self.dumpMaper(self.id, f"Mapper {self.id} failing")
				return raft_pb2.MapperOutput(success=False)
			oldCentroids = request.oldCentroids
			startIndex = request.startIndex
			endIndex = request.endIndex
			numReducer = request.numReducer
			self.dumpMaper(self.id, f"Mapper {self.id} received request with start index {startIndex} and end index {endIndex} with array of old centroids {oldCentroids} and number of reducer {numReducer}")
			# print(f"Mapper {self.id} received request with start index {startIndex} and end index {endIndex} with array of old centroids {oldCentroids} and number of reducer {numReducer}")
			mapper.mapper(readIndiceA=int(startIndex), readIndiceB=int(endIndex), oldCentroids=oldCentroids, mapperID=int(self.id))
			print(f"Mapper {self.id} done")
			self.dumpMaper(self.id, f"Mapper {self.id} done")
			mapper.partitionData(numReducer=int(numReducer), mapperID=int(self.id))
			self.dumpMaper(self.id, f"Partitioning done by mapper {self.id}")
			response = raft_pb2.MapperOutput()
			response.success = True
			print(f"Mapper {response} done")
			self.dumpMaper(self.id, f"Mapper {response} done")
			return response
		except Exception as e:
			print(e)
			response = raft_pb2.MapperOutput()
			response.success = False
			return response

	def reduce(self, request, context):
		try:
			self.dumpReducer(self.id, f"Request received for reducer {request.reducerId}")
			if random.random() < 0.5:
				print(f"Reducer {self.id} failed")
				self.dumpReducer(self.id, f"Reducer {self.id} failing")
				return raft_pb2.ReduceResponse(updated_centroid="Failed")
			reducerID = request.reducerId
			numMapper = request.numMapper
			print(f"Request received for reducer {reducerID}")
			self.dumpReducer(self.id, f"Request received for reducer {reducerID}")
			self.dumpReducer(self.id, f"Reducer {reducerID} starting")
			reducer.reduce(reducerID=reducerID, numMapper=numMapper)
			response = raft_pb2.ReduceResponse()
			response.updated_centroid = reducer.getCentroids(self.id)
			self.dumpReducer(self.id, f"Reducer {reducerID} done successfully")
			return response
		except Exception as e:
			print(e)
			response = raft_pb2.ReduceResponse()
			response.updated_centroid = ""
			return response

def startNode(nodeId:int, nodeIp:str, nodePort:int):
	# making grpc connections
	server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
	raft_pb2_grpc.add_MapReduceServicer_to_server(Node(nodeId, nodePort), server)
	server.add_insecure_port(f"[::]:{nodePort}")
	server.start()
	server.wait_for_termination()

if __name__ == '__main__':
	id = int(input("Enter node id: "))
	port = int(input("Enter port: "))
	startNode(id, "localhost", port)

# python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto