import grpc
from concurrent import futures
import message_pb2
import message_pb2_grpc
import numpy as np

class Reducer(message_pb2_grpc.ReducerServicer):
    def Reduce(self, request, context):
        centroid_id = request.key
        data_points = request.value

        # Update centroid
        updated_centroid = self.update_centroid(centroid_id, data_points)

        return message_pb2.ReduceResponse(updated_centroid=updated_centroid)

    def update_centroid(self, centroid_id, data_points):
        # Convert data points to numpy array for easy manipulation
        data_points_array = np.array([np.array(point.split(','), dtype=float) for point in data_points])

        # Calculate the mean of data points along each dimension
        updated_centroid = np.mean(data_points_array, axis=0)

        # Convert the updated centroid to string format for transmission
        updated_centroid_str = ','.join([str(coord) for coord in updated_centroid])

        return updated_centroid_str

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    message_pb2_grpc.add_ReducerServicer_to_server(Reducer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()

# python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. message.proto