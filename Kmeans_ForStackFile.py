
from numpy import *
import csv
import datetime
import sys
from pyspark import SparkContext



# def compute_newCentroids(row, dictCount): 
# 	c=[]
# 	for x in row[-1]:
# 		t=x/dictCount[row[0]]
# 		c.append(t)
# 	return c

def compute_newCentroids(sum_coord, count): 
	c=[]
	for x in sum_coord:
		t=x/count
		c.append(t)
	return c

def create_dict(table):
	dict_table={}	
	for t in table:
		dict_table[t[0]]=t[-1]
	return dict_table




def calculate_closest_centroid(row, dict_centroids, k):
	dist = 100000000000    #if this i not big enough k+1 clusters will be created!! verify this somehow!!
	index = k+1
	for i in range(k):
		aa=array(row)
		bb = array(dict_centroids[i])
		new_distance=linalg.norm(aa-bb)**2
		if new_distance < dist:
			dist = new_distance
			index = i
	return (index, dist)


def parsePoint_fromString(st):
	point=[float(j) for j in st.split(",")]
	return point






sc = SparkContext(appName = "Clustering")  # initialize the sc 'spark context'for  spark.
#Creating the RDD File
file = sc.textFile("/user/group-AI/output_user_clean.csv")
file_withoutHeader=file.filter(lambda row: row.find('reputation')!=0)
# CHANGE COMMENTed lines LINE TO TAKE ONLY A SUPSET OF POINTS:+++++++++++++++++++++++++++++++++++++++++++
rdd_file=file_withoutHeader.map(lambda row:  parsePoint_fromString(row) ).cache()
#rdd_part=file.sample(False, 0.00003, 0)
#file_withoutHeader=file.filter(lambda row: row.find('reputation')!=0)
#rdd_file=rdd_part.map(lambda row: parsePoint_fromString(row) )
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
count_points=rdd_file.count()
print("Number of Points being used:::::::::::::::!!!--->", count_points)

#TODO!!!!!!!!!!!!#Choosing number of clusters
# for k in range(n_max_clusters):  #--->create loop over ks 
k=3

itr=0
delta_cost=10000.0
list_cost=[]
cost=100000
epsilon =10 # adjust-----> small value!!!!


#CENTROIDs INITIALIZATION:
iniCentroids = rdd_file.takeSample(False,k)	
#verification to make sure we dont have repeated centroids!!!
mset = [list(x) for x in set(tuple(x) for x in iniCentroids)]
while (len(iniCentroids) is not len(mset)):
	print("will take another sample1")	
# 			#take new samples until length are the same!
	iniCentroids = rdd_file.takeSample(False,k)
	mset = [list(x) for x in set(tuple(x) for x in iniCentroids)]

centroids_table=[]  #Create dictionary of centroids from inicentroids
for c in range(k):
	centroids_table.append((c, iniCentroids[c]))
	print(centroids_table[c])
	
dict_centroids=create_dict(centroids_table)	  # hashtable of centroids {0: [x, y, z], 1:[, , , ]...}
	
itr=0

print("COST IS::::!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1::: ", cost)
print("DELTA IS:::!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!:::: ", delta_cost)

# KMEANS CYCLE:
while (delta_cost>epsilon ):
	
	itr=itr+1
#DISTANCE COMPUTATION---Assignment RDD
	#Calculate closest centroid and add it to each point
# 	rdd_centroids = rdd_file.map(lambda row : (row,calculate_closest_centroid(row,centroids, k)))
	rdd_centroids = rdd_file.map(lambda row : (row,calculate_closest_centroid(row,dict_centroids, k))).cache()
	
	
#COST COMPUTATION:
	cost_prev=cost
	
	rdd_dist= rdd_centroids.map(lambda row : (int(row[-1][0]),float(row[-1][1]))).reduceByKey(lambda a,b: (a+b))
	dist = rdd_dist.collect(); # dist= rdd_dist.reduce(lambda a ,b: a+b)
	cost=sum(x[1] for x in dist)/count_points  #cost=dist/count_points
	
# 	print("COST IS ", cost)
	list_cost.append(cost)
	delta_cost=abs(cost_prev-cost)
# 	print(" new DELTA IS ", delta_cost)
	
#UPDATE CENTROIDS:
	rdd_temp = rdd_centroids.map(lambda row : (int(row[-1][0]), row[0])).cache()
		
# 	rdd_count = rdd_temp.map(lambda row : (row[0],1)).reduceByKey(lambda a,b : (a+b)).sortByKey()
	rdd_count = rdd_temp.map(lambda row : (row[0],1)).reduceByKey(lambda a,b : (a+b))
# 	count=rdd_count.collect()
# 	dict_count=create_dict(count)
	rdd_sum_computation = rdd_temp.reduceByKey(lambda a,b :[x + y for x, y in zip(a, b)])
	rdd_index_sum_counts = rdd_sum_computation.join(rdd_count) #[(0, [sum centroid0], count)..]
# 	rdd_mean_computation = rdd_sum_computation.map(lambda row : (row[0],compute_newCentroids(row, count))).sortByKey()
# 	print("rdd_index_sum_counts:  ", rdd_index_sum_counts.collect())   
# 	sys.exit("centroid update FINISHED!!!!!!!!!!!!!!1")
	rdd_centroids_update = rdd_index_sum_counts.map(lambda row : (row[0],compute_newCentroids(row[-1][0], row[-1][-1])))
	
	centroids=rdd_centroids_update.collect()
	dict_centroids=create_dict(centroids)	
# 	centroids=rdd_mean_computation.map(lambda row: (row[1])).collect()	
	
	
	
# 	if itr in arange(0, 10000, 10):
	print("COST IS::::::: ", cost, "DELTA IS::::::: ", delta_cost)
	

# OUTPUT CREATION:
now = datetime.datetime.now()
nowsrt = now.strftime("%Y-%m-%d_%H%M")

# only USE FOR SMALL SUBSET OF DATA!!!!!!!!!!! writes assignment table to file .> makes COLLECT()+++++++++++++++++++++++++++++++++++++++++++++++
# #
#rdd_assign=rdd_temp.sortByKey()
#assign=rdd_assign.collect()
#output_pontos=open("pontosTeste_" +str(nowsrt)+ ".csv", "w")
#writer = csv.writer(output_pontos)
#writer.writerows(assign)
#
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# USE FOR WHOLE DATASET: writes assignment table to HDFS:++++++++++++++++++++++++++++++++++++++++
#
rdd_assign=rdd_temp.sortByKey()
st_path="pontos_output_"+ str(nowsrt)
rdd_assign.saveAsTextFile(st_path)
#
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

outputFile=open("outputkmeans_"+nowsrt+".csv", "w")
writer = csv.writer(outputFile)
for key, value in dict_centroids.items():
	writer.writerow([key, value])

print(list_cost)
outputCost=open("costoverIterations_"+nowsrt+".csv", "w")
outputCost.writelines(["%s\n" %item for item in list_cost])
