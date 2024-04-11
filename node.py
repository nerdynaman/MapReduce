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

class Node:
	def __init__(self, id, port):
		self.id = id
		self.port = port
	
	def map(self, request, context):
		oldCentroids = request.oldCentroids
		startIndex = request.startIndex
		endIndex = request.endIndex
		numReducer = request.numReducer
		mapper.map(readIndiceA=startIndex, readIndiceB=endIndex, oldCentroids=oldCentroids, mapperID=self.id)
		mapper.partitionData(numReducer=numReducer, mapperID=self.id)
		response = raft_pb2.MapResponse()
		response.success = True
		return response

def startNode(nodeId:int, nodeIp:str, nodePort:int):
	# making grpc connections
	server = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=10))
	raft_pb2_grpc.add_RaftServicer_to_server(Node(nodeId, nodePort), server)
	server.add_insecure_port(f"[::]:{nodePort}")
	server.start()
	server.wait_for_termination()

if __name__ == '__main__':
	startNode(1, 'localhost', 50051)

# python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. raft.proto