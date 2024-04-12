import os

def shuffleSort(reducerID, numMapper):
    '''
    Shuffles and sorts the data based on the key.
    '''
    # Check if directory Reducers exists, otherwise create it
    if not os.path.exists('Data/Reducers'):
        os.mkdir('Data/Reducers')
    
    # Read intermediate data from mapper outputs
    intermediate_data = []
    for mapper_id in range(numMapper):  # Assuming there are 3 mappers (M1, M2, M3)
        for filename in os.listdir(f'Data/Mapper/M{mapper_id}'):
            if filename.startswith(f"reducer{reducerID}"):
                with open(f'Data/Mapper/M{mapper_id}/{filename}', 'r') as f:
                    intermediate_data.extend(f.readlines())
    
    # Sort intermediate data by key
    intermediate_data.sort(key=lambda x: int(x.split()[0]))

    return intermediate_data

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
        updated_centroid = [sum(x) / len(points) for x in zip(*points)]
        updated_centroids[centroid_id] = updated_centroid
    
    print(updated_centroids)
    
    # Write final output with updated centroids
    with open(f'Data/Reducers/R{reducerID}.txt', 'w') as f:
        for centroid_id, centroid in updated_centroids.items():
            f.write(f"{centroid_id} {' '.join(map(str, centroid))}\n")
    
    return

# Example usage
if __name__ == "__main__":
    reduce(2,1)  # Run reducer with ID 1
