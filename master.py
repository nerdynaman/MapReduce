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
        data_length = None
        line_division_length = None
        prev_centroid_list = ""
        current_centroid_list = ""

    def send_request(self, channel, requests_list, index):
        channel_stub = raft_pb2_grpc.NodeStub(channel)
        response = channel_stub.map(requests_list[index])
        print(f"Mapper work for the mapper id {index} conducted successfully:", response.success)
        return response.success

    def mapper_task(self):
        threader = []
        requests_list = []
        pending_mappers = []

        for i in range(self.Mapper_count):
            if(i*(self.line_division + 1) > self.data_length):
                requests_list.append(raft_pb2.MapperInput(str(i * self.line_division), str(self.data_length), self.current_centroid_list, str(self.Reducer_count)))
                break    
            requests_list.append(raft_pb2.MapperInput(str(i * self.line_division), str(i * (self.line_division + 1)), self.current_centroid_list, str(self.Reducer_count)))
                                    
        for i in range(self.Mapper_count):
            thread = threading.Thread(target=self.send_request, args=(self.mapper_channel_mapping[i], requests_list, i))
            thread.start()
            threader.append(thread)
        
        for i in range(len(threader)):
            if threader[i].join() == False or threader[i].join() == None:
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
                if threader[i].join() == False or threader[i].join() == None:
                    pending_mappers.append(i) 
                else:
                    pending_mappers.pop(i)
        
        return
    
    def reducer_task(self):
        self.prev_centroid_list = self.current_centroid_list
        self.current_centroid_list = ""
        threader = []
        requests_list = []
        pending_reducers = []

        for i in range(self.Reducer_count):  
            requests_list.append(raft_pb2.ReduceRequest(i, self.Mapper_count))
                                    
        for i in range(self.Reducer_count):
            thread = threading.Thread(target=self.send_request, args=(self.reducer_channel_mapping[i], requests_list, i))
            thread.start()
            threader.append(thread)
        
        for i in range(len(threader)):
            output_threader = threader[i].join()
            if len(output_threader) != 0 and output_threader != None:
                if len(self.current_centroid_list) == 0:
                    self.current_centroid_list = output_threader
                else:
                    self.current_centroid_list += ","+ output_threader
            else:
                pending_reducers.append(i)

        while len(pending_reducers) != 0:
            threader = []
            counter = 0

            for i in range(self.Reducer_count):
                if counter < len(pending_reducers) and i not in pending_reducers:
                    thread = threading.Thread(target=self.send_request, args=(self.reducer_channel_mapping[i], requests_list, pending_reducers[counter]))
                    thread.start()
                    counter += 1
                    threader.append(thread)
                elif counter >= len(pending_reducers):
                    break

            for i in range(len(threader)):
                output_threader = threader[i].join()
                if len(output_threader) != 0 and output_threader != None:
                    if len(self.current_centroid_list) == 0:
                        self.current_centroid_list = output_threader
                    else:
                        self.current_centroid_list += "," + output_threader
                else:
                    pending_reducers.pop(i)
        return

    def split_input(self, file):
        with open(file, "r") as f:
            data = f.readlines()
            self.data_length = len(data)
            self.line_division = math.ceil(self.data_length/self.Mapper_count)
            for i in range(len(data)):
                if i >= self.Centroid_count:
                    break
                self.current_centroid_list += "(" + data[i].strip() + ")" +","
            self.current_centroid_list = self.current_centroid_list[:-1]

    
    def List_creator(self, list_string):
        centroid_list = list_string.split("),(")
        centroid_list[0] = centroid_list[0][1:]
        centroid_list[-1] = centroid_list[-1][1:]
        for i in range(len(centroid_list)):
            centroid_list[i] = centroid_list[i].split(",")
        return centroid_list
    
    def Master_in_action(self, file):
        self.split_input(file)
        final_output = []
        prev_centroid_list = []
        current_centroid_list = []
        for i in range(self.Iteration_count):
            prev_centroid_list = self.List_creator(self.prev_centroid_list)
            current_centroid_list = self.List_creator(self.current_centroid_list)
            if len(self.prev_centroid_list) != 0 and (prev_centroid_list in current_centroid_list) and (current_centroid_list in prev_centroid_list):
                break
            self.mapper_task()
            self.reducer_task()
        
        for i in current_centroid_list:
            final_output.append(i[0] + ", " + i[1] + "\n")
        
        with open("centroid.txt", "w") as file:
            file.writelines(final_output)
        

if __name__ == '__main__':
    Mapper_count = int(input("No. of Mappers: "))
    Reducer_count = int(input("No. of Reducers: "))
    Centroid_count = int(input("No. of Centroids: "))
    Iteration_count = int(input("No. of Iterations: "))
    Master_init = Master(Mapper_count, Reducer_count, Centroid_count, Iteration_count)
    file_path = "" #add file path
    Master_init.Master_in_action(file_path)
