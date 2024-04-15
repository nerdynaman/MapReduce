import os
import grpc
from mapper_pb2 import RequestPartitionDataRequest
from mapper_pb2_grpc import MapReduceStub

def convertBack(output):
    # output = "(1,(0.4,7.2)),(2,(0.8,9.8)),(1,(-1.5,7.3)),(2,(8.1,3.4)),(2,(7.3,2.3))"

    # Split the output string by '),(' to separate individual items
    items = output.split('),(')

    original_list = []
    for item in items:
        # Remove parentheses and split into key and coordinates
        item = item.replace('(', '')
        item = item.replace(')', '')
        key, x, y = item.split(',')
        x = float(x)
        y = float(y)
        key = int(key)
        # print(item)
        # x, y = 
        original_list.append(f"{key} {x},{y}")

    print(original_list)


def RequestPartitionData(reducerID, numMapper):
    '''
    Sends a request to all the Mapper nodes to get the data.
    '''
    data = []
    for i in range(numMapper):
        channel = grpc.insecure_channel(f'localhost:5005{i}')
        # Create a stub for the mapper service
        stub = MapReduceStub(channel)
        request = RequestPartitionDataRequest(reducerID=reducerID)
        try:
            response = stub.RequestPartitionData(request)
            tempData = convertBack(response.data)
            data.extend(tempData)
            # print(response.data)
        except grpc.RpcError:
            print(f"FAIL: Mapper {i}")
    
    return data

def shuffleSort(reducerID, numMapper):
    '''
    Shuffles and sorts the data based on the key.
    '''
    # Check if directory Reducers exists, otherwise create it
    if not os.path.exists('Data/Reducers'):
        os.mkdir('Data/Reducers')
    
    # Read intermediate data from mapper outputs
    intermediate_data = []
    # for mapper_id in range(numMapper):  # Assuming there are 3 mappers (M1, M2, M3)
    #     for filename in os.listdir(f'Data/Mapper/M{mapper_id}'):
    #         if filename.startswith(f"reducer{reducerID}"):
    #             with open(f'Data/Mapper/M{mapper_id}/{filename}', 'r') as f:
    #                 intermediate_data.extend(f.readlines())
    
    intermediate_data = RequestPartitionData(reducerID, numMapper)
    print(intermediate_data)
    # Sort intermediate data by key
    intermediate_data.sort(key=lambda x: int(x.split()[0]))

    return intermediate_data

def reduce_logic(value, key):
    '''
    Applies the Reduce function to process intermediate data received from mappers.
    Generates final output with updated centroids.
    '''
    # Compute the updated centroid
    updated_centroid = [sum(x) / len(value) for x in zip(*value)]
    
    return updated_centroid, key

def reduce(reducerID, numMapper):
    '''
    Applies the Reduce function to process intermediate data received from mappers.
    Generates final output with updated centroids.
    '''
    reducerID = int(reducerID)
    numMapper = int(numMapper)
    
    # Shuffle and sort intermediate data
    intermediate_data = shuffleSort(reducerID, numMapper)
    print(intermediate_data)
    
    # Apply Reduce function and generate final output with updated centroids
    centroids = {}
    for line in intermediate_data:
        centroid_id, point = line.split()
        point = tuple(map(float, point.split(',')))
        if centroid_id not in centroids:
            centroids[centroid_id] = [point]
        else:
            centroids[centroid_id].append(point)
    
    print(centroids)
    
    # Compute updated centroids
    updated_centroids = {}
    for centroid_id, points in centroids.items():
        # updated_centroid = [sum(x) / len(points) for x in zip(*points)]
        updated_centroid, x = reduce_logic(points, centroid_id)
        updated_centroids[centroid_id] = updated_centroid
    
    print(updated_centroids)
    
    # Write final output with updated centroids
    with open(f'Data/Reducers/R{reducerID}.txt', 'w') as f:
        for centroid_id, centroid in updated_centroids.items():
            f.write(f"{centroid_id} {' '.join(map(str, centroid))}\n")
    
    final_string = ""
    for centroid_id, centroid in updated_centroids.items():
        final_string += f"({','.join(map(str, centroid))}),"
    final_string = final_string[:-1]
    
    print("final="+final_string)
    return final_string

# Example usage
if __name__ == "__main__":
    # reduce(2,1)  # Run reducer with ID 1
    convertBack()
