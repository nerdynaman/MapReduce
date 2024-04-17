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

dump_array = []

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

    def dump_file(self):
        with open("masterDump.txt", "w") as f:
            f.writelines(dump_array)
        
    def send_request(self, channel, requests_list, index, Reducer = False):
        try:
            if Reducer == False:
                channel_stub = raft_pb2_grpc.MapReduceStub(channel)
                print("Mapper Stub Created")
                dump_array.append("Mapper Stub Created\n")
                self.dump_file()
                response = channel_stub.map(requests_list[index])
                print(f"Mapper work for the mapper id {index} conducted successfully:", response.success)
                dump_array.append(f"Mapper work for the mapper id {index} conducted successfully: {response.success}\n")
                self.dump_file()
                if response.success == True:
                    self.threader.remove(index)
                return response.success
            else:
                channel_stub = raft_pb2_grpc.MapReduceStub(channel)
                print("Reducer Stub Created")
                dump_array.append("Reducer Stub Created\n")
                self.dump_file()
                response = channel_stub.reduce(requests_list[index])
                print(f"Reducer work for the reducer id {index} conducted successfully:", response.updated_centroid)
                dump_array.append(f"Reducer work for the reducer id {index} conducted successfully: {response.updated_centroid}\n")
                self.dump_file()
                if response.updated_centroid != "Failed":
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
        dump_array.append("Mapper task started\n")
        self.dump_file()
        for i in self.mapper_dict.keys():
            self.mapper_dict[i] = True
        threader = []
        requests_list = []
        pending_mappers = []

        for i in range(self.Mapper_count):
            if((i + 1)*(self.line_division) > self.data_length):
                a = str(i * self.line_division)
                b = str(self.data_length)
                c = self.current_centroid_list
                d = str(self.Reducer_count)
                requests_list.append(raft_pb2.MapperInput(startIndex=a, endIndex=b, oldCentroids=c, numReducer=d))
                break   
            a = str(i * self.line_division)
            b = str((i + 1) * (self.line_division))
            c = self.current_centroid_list
            d = str(self.Reducer_count) 
            requests_list.append(raft_pb2.MapperInput(startIndex=a, endIndex=b, oldCentroids=c, numReducer=d))
            print(f"Mapper {i} request created successfully with start index as {a} and end index as {b} and old centroids as {c} and number of reducer as {d}")
            dump_array.append(f"Mapper {i} request created successfully with start index as {a} and end index as {b} and old centroids as {c} and number of reducer as {d}\n")
            self.dump_file()
                                    
        for i in range(self.Mapper_count):
            thread = threading.Thread(target=self.send_request, args=(grpc.insecure_channel(self.mapper_channel_mapping[i]), requests_list, i))
            thread.start()
            threader.append(thread)
            self.threader.append(i)
            print(f"Mapper {i} request sent successfully")
            dump_array.append(f"Mapper {i} request sent successfully\n")
            self.dump_file()
        
        print("Length of threader", len(threader))
        dump_array.append(f"Length of threader {len(threader)}\n")
        self.dump_file()
        for i in range(len(threader)):
            ll = threader[i].join()
            if i in self.threader:
                pending_mappers.append(i) 
                self.mapper_dict[i] = False
                print(f"Mapper{i} request failed")
                dump_array.append(f"Mapper{i} request failed\n")
                self.dump_file()

        while len(pending_mappers) != 0 :
            falseFlag = False
            for i in self.mapper_dict.keys():
                if self.mapper_dict[i] == True:
                    falseFlag = True
                    break
            if falseFlag == False:
                for i in self.mapper_dict.keys():
                    self.mapper_dict[i] = True
            print("Pending mappers: ", pending_mappers)
            dump_array.append(f"Pending mappers: {pending_mappers}\n")
            self.dump_file()
            threader = []
            counter = 0
            dictionn = {}

            for i in range(self.Mapper_count):
                if counter < len(pending_mappers) and self.mapper_dict[i] == True:
                    thread = threading.Thread(target=self.send_request, args=(grpc.insecure_channel(self.mapper_channel_mapping[i]), requests_list, pending_mappers[counter]))
                    thread.start()
                    threader.append(thread)
                    dictionn[thread] = pending_mappers[counter]
                    counter += 1
                    counter = counter % self.Mapper_count
                elif counter >= len(pending_mappers):
                    break

            for i in range(len(threader)):
                threader[i].join()
                if dictionn[threader[i]] in self.threader:
                    self.mapper_dict[i] = False 
                else:
                    temp = dictionn[threader[i]]
                    print("dict", temp)
                    print("pending mappers", pending_mappers)
                    pending_mappers.remove(dictionn[threader[i]])
                del dictionn[threader[i]]
        print("Mapper task completed successfully")
        dump_array.append("Mapper task completed successfully\n")
        self.dump_file()
        return
    
    def reducer_task(self):
        print("Reducer task started")
        dump_array.append("Reducer task started\n")
        self.dump_file()
        for i in self.reducer_dict.keys():
            self.reducer_dict[i] = True
        self.prev_centroid_list = self.current_centroid_list
        self.current_centroid_list = ""
        threader = []
        requests_list = []
        pending_reducers = []
        self.threader = []

        for i in range(self.Reducer_count):  
            requests_list.append(raft_pb2.ReduceRequest(reducerId=str(i), numMapper=str(self.Mapper_count)))
            print("Reducer request created successfully")
            dump_array.append("Reducer request created successfully\n")
            self.dump_file()
                                    
        for i in range(self.Reducer_count):
            print("reducer",self.reducer_channel_mapping)
            print("index", i)
            print("Reducer channel mapping", self.reducer_channel_mapping[i])
            thread = threading.Thread(target=self.send_request, args=(grpc.insecure_channel(self.reducer_channel_mapping[i]), requests_list, i, True))
            thread.start()
            threader.append(thread)
            self.threader.append(i)
            print("Reducer request sent successfully")
            dump_array.append("Reducer request sent successfully\n")
            self.dump_file()
        
        for i in range(len(threader)):
            ll = threader[i].join()
            if i in self.threader:
                pending_reducers.append(i) 
                self.reducer_dict[i] = False
                print(f"Reducer{i} request failed")
                dump_array.append(f"Reducer{i} request failed\n")
                self.dump_file()

        while len(pending_reducers) != 0:
            falseFlag = False
            for i in self.reducer_dict.keys():
                if self.reducer_dict[i] == True:
                    falseFlag = True
                    break
            if falseFlag == False:
                for i in self.reducer_dict.keys():
                    self.reducer_dict[i] = True
            print("Initial Pending reducers: ", pending_reducers)
            dump_array.append(f"Pending reducers: {pending_reducers}\n")
            self.dump_file()
            threader = []
            counter = 0
            dictionn = {}
            
            for i in range(self.Reducer_count):
                if counter < len(pending_reducers) and self.reducer_dict[i] == True:
                    print(f"Sending Request to Reducer {i} now")
                    requests_list[pending_reducers[counter]].reducerId = str(i)
                    thread = threading.Thread(target=self.send_request, args=(grpc.insecure_channel(self.reducer_channel_mapping[i]), requests_list, pending_reducers[counter], True))
                    thread.start()
                    threader.append(thread)
                    dictionn[thread] = pending_reducers[counter]
                    counter += 1
                    counter = counter % self.Reducer_count
                elif counter >= len(pending_reducers):
                    break

            for i in range(len(threader)):
                threader[i].join()
                print(f"Reducer {i} completed its task")
                if dictionn[threader[i]] in self.threader:
                    self.reducer_dict[i] = False 
                else:
                    pending_reducers.remove(dictionn[threader[i]])
                del dictionn[threader[i]]
                    
            print("Final Pending reducers: ", pending_reducers)
            dump_array.append(f"Pending reducers: {pending_reducers}\n")
            self.dump_file()
        print("Reducer task completed successfully")
        dump_array.append(f"Reducer task completed successfully\n")
        self.dump_file()
        return

    def split_input(self, file):
        try:
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
                dump_array.append(f"Splitting of input data completed successfully with line division as {self.line_division} and cenroid list as {self.current_centroid_list}\n")
                self.dump_file()
        except Exception as e:
            print("Error in splitting the input data", e)
            dump_array.append(f"Error in splitting the input data {e}\n")
            self.dump_file()
    
    def List_creator(self, list_string):
        if list_string == "":
            return [], []
        centroid_list = list_string.split("),(")
        float_listy = []
        # print(len(list_string))
        for i in range(len(centroid_list)):
            centroid_list[i] = centroid_list[i].strip("(")
            centroid_list[i] = centroid_list[i].strip(")")
        for i in range(len(centroid_list)):
            centroid_list[i] = centroid_list[i].split(",")
            # print("X and Y are", centroid_list)
            x = float(centroid_list[i][0].strip("("))
            y = float(centroid_list[i][1].strip(")"))
            formatted_x = round(x, 4)
            formatted_y = round(y, 4)
            float_listy.append([formatted_x, formatted_y])
        print("List creation completed successfully")
        dump_array.append("List creation completed successfully\n")
        self.dump_file()
        return centroid_list, float_listy
    
    def Master_in_action(self, file):
        self.split_input(file)
        final_output = []
        prev_centroid_list = []
        current_centroid_list = []
        # print("Starting:", self.current_centroid_list)
        self.mapper_task()
        self.reducer_task()
        for i in range(self.Iteration_count - 1):
            print(f"Iteration {i+1} started")
            dump_array.append(f"Iteration {i+1} started\n")
            self.dump_file()
            temp_converge = True
            prev_centroid_list, prev_centroid_list_float = self.List_creator(self.prev_centroid_list)
            current_centroid_list, current_centroid_list_float = self.List_creator(self.current_centroid_list)
            # print("PRev list:", prev_centroid_list_float)
            # print("current list:",current_centroid_list_float)
            # print("prev centroid list string", self.prev_centroid_list)
            for i in prev_centroid_list_float:
                if i not in current_centroid_list_float:
                    temp_converge = False
                    # print("HEY")
            for i in current_centroid_list_float:
                if i not in prev_centroid_list_float:
                    temp_converge = False
                    # print("HAHA")
            if (len(self.prev_centroid_list) != 0) and temp_converge:
                print("The centroids have converged, Exiting!")
                dump_array.append("The centroids have converged, Exiting!\n")
                self.dump_file()
                break
            dump_array.append(f"Current centroid list: {current_centroid_list}\n")
            self.dump_file()
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
    Mapper_count = 2
    Reducer_count = 2
    Centroid_count = 4
    Iteration_count = 50
    Master_init = Master(Mapper_count, Reducer_count, Centroid_count, Iteration_count)
    for i in range(Mapper_count):
        # Master_init.mapper_channel_mapping[i] = ("localhost:40001")
        # Master_init.mapper_channel_mapping[i] = ("localhost:"+input(f"Enter Mapper {i} ip address with port number:"))
        Master_init.mapper_dict[i] = True
    for i in range(Reducer_count):
        Master_init.reducer_dict[i] = True
        # Master_init.reducer_channel_mapping[i] = ("localhost:40002")
        # Master_init.reducer_channel_mapping[i] = ("localhost:"+input(f"Enter Reducer {i} ip address with port number:"))
    Master_init.mapper_channel_mapping[0] = ("localhost:50001")
    Master_init.mapper_channel_mapping[1] = ("localhost:50002")
    Master_init.reducer_channel_mapping[0] = ("localhost:50003")
    Master_init.reducer_channel_mapping[1] = ("localhost:50004")
    file_path = "Data/Input/points.txt"
    print("Master in action")
    dump_array.append("Master in action\n")
    Master_init.Master_in_action(file_path)
