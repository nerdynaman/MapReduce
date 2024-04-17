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
    def __init__(self, no_of_mapper, no_of_reducer, no_of_centroids, no_of_iterations):
        self.Mapper_count = no_of_mapper
        self.Reducer_count = no_of_reducer
        self.Centroid_count = no_of_centroids
        self.Iteration_count = no_of_iterations
        self.mapper_dict = {} #mapper_id: active_status
        self.mapper_channel_mapping = {} #mapper_id: channel
        self.reducer_dict = {} #reducer_id: active_status
        self.reducer_channel_mapping = {} #reducer_id: channel
        self.data_length = None
        self.line_division_length = None
        self.prev_centroid_list = ""
        self.current_centroid_list = ""
        self.threader = []

    def send_request(self, channel, requests_list, index, Reducer = False):
        try:
            if Reducer == False:
                channel_stub = raft_pb2_grpc.MapReduceStub(channel)
                print("Mapper Stub Created")
                response = channel_stub.map(requests_list[index])
                print(f"Mapper work for the mapper id {index} conducted successfully:", response.success)
                if response.success == True:
                    self.threader.remove(index)
                return response.success
            else:
                channel_stub = raft_pb2_grpc.MapReduceStub(channel)
                print("Reducer Stub Created")
                reponse = channel_stub.reduce(requests_list[index])
                if len(response.updated_centroid) != 0:
                    self.threader.remove(index)
                    if len(self.current_centroid_list) == 0:
                        self.current_centroid_list = response.updated_centroid
                    else:
                        self.current_centroid_list += ","+ response.updated_centroid
            
        except grpc.RpcError as e:
            print(f"Error: {e.code()}")
            return False

    def mapper_task(self):
        print("Mapper task started")
        threader = []
        requests_list = []
        pending_mappers = []

        for i in range(self.Mapper_count):
            if((i + 1)*(self.line_division) > self.data_length):
                a = str(i * self.line_division)
                b = str(self.data_length)
                c = self.current_centroid_list
                d = str(self.Reducer_count)
                requests_list.append(raft_pb2.MapperInput(a, b, c, d))
                break   
            a = str(i * self.line_division)
            b = str((i + 1) * (self.line_division))
            c = self.current_centroid_list
            d = str(self.Reducer_count) 
            requests_list.append(raft_pb2.MapperInput(startIndex=a, endIndex=b, oldCentroids=c, numReducer=d))
            print(f"Mapper {i} request created successfully with start index as {a} and end index as {b} and old centroids as {c} and number of reducer as {d}")
                                    
        for i in range(self.Mapper_count):
            thread = threading.Thread(target=self.send_request, args=(grpc.insecure_channel(self.mapper_channel_mapping[i]), requests_list, i))
            thread.start()
            threader.append(thread)
            self.threader.append(i)
            print(f"Mapper {i} request sent successfully")
        
        print("Length of threader", len(threader))
        for i in range(len(threader)):
            ll = threader[i].join()
            if i in self.threader:
                pending_mappers.append(i) 
                self.mapper_dict[i] = False
                print(f"Mapper{i} request failed")

        while len(pending_mappers) != 0:
            print("Pending mappers: ", pending_mappers)
            threader = []
            counter = 0
            dictionn = {}

            for i in range(self.Mapper_count):
                if counter < len(pending_mappers) and i not in pending_mappers and self.mapper_dict[i] == True:
                    thread = threading.Thread(target=self.send_request, args=(grpc.insecure_channel(self.mapper_channel_mapping[i]), requests_list, pending_mappers[counter]))
                    thread.start()
                    counter += 1
                    threader.append(thread)
                    dictionn[thread] = pending_mappers[counter]
                elif counter >= len(pending_mappers):
                    break

            for i in range(len(threader)):
                threader[i].join()
                if dictionn[threader[i]] in self.threader:
                    self.mapper_dict[i] = False 
                else:
                    del dictionn[threader[i]]
                    pending_mappers.pop(dictionn[threader[i]])
        print("Mapper task completed successfully")
        return
    
    def reducer_task(self):
        print("Reducer task started")
        self.prev_centroid_list = self.current_centroid_list
        self.current_centroid_list = ""
        threader = []
        requests_list = []
        pending_reducers = []
        self.threader = []

        for i in range(self.Reducer_count):  
            requests_list.append(raft_pb2.ReduceRequest(reducerId=str(i), numMapper=str(self.Mapper_count)))
            print("Reducer request created successfully")
                                    
        for i in range(self.Reducer_count):
            thread = threading.Thread(target=self.send_request, args=(grpc.insecure_channel(self.reducer_channel_mapping[i]), requests_list, i, True))
            thread.start()
            threader.append(thread)
            self.threader.append(i)
            print("Reducer request sent successfully")
        
        for i in range(len(threader)):
            ll = threader[i].join()
            if i in self.threader:
                pending_reducers.append(i) 
                self.reducer_dict[i] = False
                print(f"Reducer{i} request failed")

        while len(pending_reducers) != 0:
            print("Pending reducers: ", pending_reducers)
            threader = []
            counter = 0
            dictionn = {}
            
            for i in range(self.Reducer_count):
                if counter < len(pending_reducers) and i not in pending_reducers:
                    thread = threading.Thread(target=self.send_request, args=(grpc.insecure_channel(self.reducer_channel_mapping[i]), requests_list, pending_reducers[counter], True))
                    thread.start()
                    counter += 1
                    threader.append(thread)
                    dictionn[thread] = pending_reducers[counter]
                elif counter >= len(pending_reducers):
                    break

            for i in range(len(threader)):
                threader[i].join()
                if dictionn[threader[i]] in self.threader:
                    self.reducer_dict[i] = False 
                else:
                    del dictionn[threader[i]]
                    pending_reducers.pop(dictionn[threader[i]])
            print("Pending reducers: ", pending_reducers)
        print("Reducer task completed successfully")
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
            print("Splitting of input data completed successfully with line division as ", self.line_division, "and cenroid list as ", self.current_centroid_list)
    
    def List_creator(self, list_string):
        centroid_list = list_string.split("),(")
        centroid_list[0] = centroid_list[0][1:]
        centroid_list[-1] = centroid_list[-1][1:]
        for i in range(len(centroid_list)):
            centroid_list[i] = centroid_list[i].split(",")
        print("List creation completed successfully")
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
    # Mapper_count = int(input("No. of Mappers: "))
    # Reducer_count = int(input("No. of Reducers: "))
    # Centroid_count = int(input("No. of Centroids: "))
    # Iteration_count = int(input("No. of Iterations: "))
    Mapper_count = 1
    Reducer_count = 1
    Centroid_count = 4
    Iteration_count = 5
    Master_init = Master(Mapper_count, Reducer_count, Centroid_count, Iteration_count)
    for i in range(Mapper_count):
        Master_init.mapper_channel_mapping[i] = ("localhost:50001")
        # Master_init.mapper_channel_mapping[i] = ("localhost:"+input(f"Enter Mapper {i} ip address with port number:"))
        Master_init.mapper_dict[i] = True
    for i in range(Reducer_count):
        Master_init.reducer_dict[i] = True
        Master_init.reducer_channel_mapping[i] = ("localhost:50002")
        # Master_init.reducer_channel_mapping[i] = ("localhost:"+input(f"Enter Reducer {i} ip address with port number:"))
    file_path = "Data/Input/points.txt"
    print("Master in action")
    Master_init.Master_in_action(file_path)
