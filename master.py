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
import node
import math
import threading
import time


class Master:
    def __init__(self, no_of_mapper, no_of_reducer, no_of_centroids, no_of_iterations) -> None:
        Mapper_count = no_of_mapper
        Reducer_count = no_of_reducer
        Centroid_count = no_of_centroids
        Iteration_count = no_of_iterations
        mapper_dict = {} #mapper_id: active_status
        mapper_channel_mapping = {} #mapper_id: channel
        reducer_dict = {} #reducer_id: active_status
        reducer_channel_mapping = {} #reducer_id: channel
        current_centroid_list = ""

    def send_request(self, channel, requests_list, index):
        channel_stub = raft_pb2_grpc.NodeStub(channel)
        response = channel_stub.map(requests_list[index])
        print(f"Mapper work for the mapper id {index} conducted successfully:", response.success)
        return response.success

    def split_input(self, file):
        threader = []
        requests_list = []
        pending_mappers = []
        
        with open(file, "r") as f:
            data = f.readlines()
            line_division = math.ceil(len(data)/self.Mapper_count)

        for i in range(self.Mapper_count):
            if(i*(line_division + 1) > len(data)):
                requests_list.append(raft_pb2.MapperInput(str(i * line_division), str(len(data)), self.current_centroid_list, str(self.Reducer_count)))
                break    
            requests_list.append(raft_pb2.MapperInput(str(i * line_division), str(i * (line_division + 1)), self.current_centroid_list, str(self.Reducer_count)))
                                    
        for i in range(self.Mapper_count):
            thread = threading.Thread(target=self.send_request, args=(self.mapper_channel_mapping[i], requests_list, i))
            thread.start()
            threader.append(thread)
        
        for i in range(len(threader)):
            if threader[i].join() == False:
                pending_mappers.append(i) 

        while len(pending_mappers) != 0:
            threader = []
            counter = 0

            for i in range(self.Mapper_count):
                if counter < len(pending_mappers) and i not in pending_mappers:
                    thread = threading.Thread(target=self.send_request, args=(self.mapper_channel_mapping[i], requests_list, pending_mappers[counter]))
                    thread.start()
                    counter += 1
                    threader.append(thread)
                elif counter >= len(pending_mappers):
                    break

            for i in range(len(threader)):
                if threader[i].join() == False:
                    pending_mappers.append(i) 
                else:
                    pending_mappers.pop(i)
        