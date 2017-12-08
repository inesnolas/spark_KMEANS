
from numpy import *
from copyMain1_12 import rdd_centroids


def compute_newCentroids(row, count): 
	c=[]
	print(count[row[0]][1])
	for x in row[-1]:
		t=x/count[row[0]][1]
		c.append(t)
	return c



def calculate_closest_centroid(row, centroids, k):
	dist = 100000
	index = 0
# 	row_parsed = [float(j) for j in row.split(",")]
# 	del row_parsed[0]
	for i in range(k):
# 		centroids_parsed = [float(j) for j in centroids[i].split(",")]
# 		new_distance = distance.euclidean(row_parsed,centroids_parsed)
#	   new_distance =linalg.norm(row_parsed-centroids_parsed)
		aa=array(row)
		bb = array(centroids[i])
		new_distance=linalg.norm(aa-bb)
		if new_distance < dist:
			dist = new_distance
			index = i
	return (index, dist)


def parsePoint_fromString(st):
	point=[float(j) for j in st.split(",")]
	del point[0]
	return point



rdd_file=[[2.0, 650.0, 68274.0, 333845.0], [29331.0, 132769.0, 3184.0, 1298.0], [1928.0, 14202.0, 568.0, 87.0], [8352.0, 16917.0, 4876.0, 84.0], [21522.0, 52795.0, 737.0, 94.0], [26663.0, 6461.0, 761.0, 35.0], [946.0, 3244.0, 13.0, 10.0], [7117.0, 2515.0, 37.0, 5.0], [102.0, 1900.0, 1.0, 1.0], [858.0, 931.0, 1.0, 1.0]]


#Choosing number of clusters
# for k in range(n_max_clusters):  #--->create loop over ks 
k=3

itr=0
delta_cost=10000.0
list_cost=[]
cost=100000
epsilon =100 # adjust-----> small value!!!!

#centroid initialization:
iniCentroids =[[9.0, 2.0, 1.0, 1.0], [2.0, 1.0, 1.0, 1.0], [9.0, 9.0, 1.0, 1.0]]


centroids=iniCentroids
print(" DELTA IS ", delta_cost)
itr=0

while (delta_cost>epsilon):
	
	itr=itr+1
	#DISTANCE COMPUTATION
	#Calculate closest centroid and add it to each point
	rdd_centroids=[]
	for row in rdd_file:
		rdd_centroids.append((row,calculate_closest_centroid(row,centroids, k)))
	
	#Cost Computation:
	cost_prev=cost
	cost=0
	for row in rdd_centroids:
		cost=cost+ row[-1][1]
	
	print("COST IS ", cost)
	list_cost.append(cost)
	delta_cost=abs(cost_prev-cost)
# 	print(" new DELTA IS ", delta_cost)
	
	#Computation of new centroids
	
	
	
	
	rdd_temp = rdd_centroids.map(lambda row : (int(row[-1][0]), row[0]))
	
	rdd_count = rdd_temp.map(lambda row : (row[0],1)).reduceByKey(lambda a,b : (a+b)).sortByKey()
	count=rdd_count.collect()
	rdd_sum_computation = rdd_temp.reduceByKey(lambda a,b :[x + y for x, y in zip(a, b)])
# 	
# 	rdd_mean_computation = rdd_sum_computation.map(lambda row : (row[0],row[-1] / count[row[0]][1]))
	rdd_mean_computation = rdd_sum_computation.map(lambda row : (row[0],compute_newCentroids(row, count))).sortByKey()
	centroids=rdd_mean_computation.map(lambda row: (row[1])).collect()
	if itr in arange(0, 10000, 10):
		print("COST IS::::::: ", cost)
		print("DELTA IS::::::: ", delta_cost)
	
	
	
# outputFile=open("outputkmeans.csv", "w")
# outputFile.writelines(["%s\n" % item for item in centroids])
print(list_cost)
# outputCost=open("costoverIterations.csv", "w")
# outputCost.writelines(["%s\n" %item for item in list_cost])
