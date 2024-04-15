import os

def getData(reducerID, mapperID):
	with open(f'Data/Mapper/M{mapperID}/reducer{reducerID}.txt', 'r') as f:
		data = f.readlines()
	# data = ['1 0.4,7.2\n', '2 0.8,9.8\n', '1 -1.5,7.3\n', '2 8.1,3.4\n', '2 7.3,2.3\n']

	result = []
	for item in data:
		parts = item.strip().split()
		key = parts[0]
		coordinates = tuple(map(float, parts[1].split(',')))
		result.append(f"({key},({coordinates[0]},{coordinates[1]}))")

	output = ",".join(result)
	return(output)

def distance(a, b):
	'''
	return the Euclidean distance between two data points a and b.
	i.e. a = (x,y), b = (u,v), distance = sqrt((x-u)^2 + (y-v)^2)
	'''
	x1, y1 = a[0], a[1]
	x2, y2 = b[0], b[1]
	return ((x1-x2)**2 + (y1-y2)**2)**0.5

def findNearestCentroid(data, oldCentroids):
	'''
	return index of the nearest centroid to which the data point belongs
	data: a string of data point i.e. 'x,y'
	'''
	# find the nearest centroid
	nearestCentroid = 0
	minDist = float('inf')
	for j in range(len(oldCentroids)):
		dist = distance(data, oldCentroids[j])
		if dist < minDist:
			minDist = dist
			nearestCentroid = j
	return nearestCentroid

def mapper(readIndiceA, readIndiceB, oldCentroids, mapperID):
	'''
	return a list of tuples, each tuple contains key, value pair.
	Key: index of the nearest centroid to which the data point belongs
	Value: value of the data point itself.
	'''
	# check if dir Mapper/M{mapperID} exists otherwise create it, 'Mapper is a directory'
	if not os.path.exists('Data/Mapper'):
		os.mkdir('Data/Mapper')
	if not os.path.exists('Data/Mapper/M' + str(mapperID)):
		os.mkdir('Data/Mapper/M' + str(mapperID))

	# read from input file from line number readIndiceA to readIndiceB
	dataRaw = []
	with open('Data/Input/points.txt', 'r') as f:
		dataRaw = f.readlines()
	dataRaw = dataRaw[readIndiceA:readIndiceB]
	data = []
	for i in range(len(dataRaw)):
		x = float(dataRaw[i].split(',')[0])
		y = float(dataRaw[i].split(',')[1])
		data.append((x,y))
	# find the nearest centroid for each data point and write in output file
	with open(f'Data/Mapper/M{mapperID}/mapperOutput.txt', 'w') as f:
		for i in range(len(data)):
			nearestCentroid = findNearestCentroid(data[i], oldCentroids)
			f.write(str(nearestCentroid) + ' ' + str(data[i][0]) + ',' + str(data[i][1]) + '\n')
	return

def partitionData(numReducer, mapperID):
	'''
	There is file corresponding to each reducer i.e. reducer{i}.txt in which partitioned data is stored for reducer{i}.
	All the data with same key should be sent to same reducer.
	'''
	with open(f'Data/Mapper/M{mapperID}/mapperOutput.txt', 'r') as f:
		data = f.readlines()
	dataLen = len(data)
	dataPerMapper = dataLen // numReducer
	# partition data for each reducer with partition function: key % numReducer
	for i in range(dataLen):
		key = int(data[i].split()[0])
		with open(f'Data/Mapper/M{mapperID}/reducer{key % numReducer}.txt', 'a') as f:
			f.write(data[i])
	return

# Example usage
if __name__ == "__main__":
    # map(0,15,[(1,2),(3,4),(5,6),(7,8)],0)  # Run reducer with ID 1
    # partitionData(3,0)  # Run reducer with ID 1
    getData(0,0)