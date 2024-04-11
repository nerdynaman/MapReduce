import grpc
from concurrent import futures
import message_pb2
import message_pb2_grpc

class Reducer(message_pb2_grpc.ReducerServicer):
    def __init__(self):
        pass

    def Reduce(self, request, context):
        # Implement your Reduce function logic here
        centroid_id = request.key
        data_points = request.value

        # Process data points to update centroid
        updated_centroid = self.update_centroid(centroid_id, data_points)

        return message_pb2.ReduceResponse(updated_centroid=updated_centroid)

    def update_centroid(self, centroid_id, data_points):
        # Implement logic to update centroid using data points
        pass

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    message_pb2_grpc.add_ReducerServicer_to_server(Reducer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    server.wait_for_termination()

if __name__ == '__main__':
    serve()

# python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. message.proto