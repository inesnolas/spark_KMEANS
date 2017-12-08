from numpy import *
import csv
import datetime
import sys
from matplotlib import*
from pyspark import SparkContext



def compute_newCentroids(row, count):
	c = []
	for x in row:
		t = x / count
		c.append(t)
	return tuple(c)


def create_dict(table):
	dict_table = {}
	for t in table:
		dict_table[t[0]] = t[-1]
	return dict_table


def calculate_distance(point, centroid):

	aa = array(point[1:])
	bb = array(centroid[1:])
	dist = linalg.norm(aa - bb) **2
	return dist



def parsePoint_fromString(st):				#        "id, x, y, z...." ---->  (id, x,y,z,...)
	point = [float(j) for j in st.split(",")]
	return tuple(point)

counter = -1


def g(x):
	sum_dist.add(x)
	


sc = SparkContext(appName="Clustering")
file = sc.textFile("userDirectFeatures.csv")

# rdd_part=file.sample(False, 0.000003, 0)
# rdd_file=rdd_part.map(lambda row: parsePoint_fromString(row) )
rdd_file=file.map(lambda row: parsePoint_fromString(row) )
# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
count_points = rdd_file.count()
print("BAMOS TRABALHAR COM ESTES PONTINHOOOOOOOOOOS: ", count_points)



#



# sys.exit("Contamos os PONTOS")

# list_cost_overK=[]
# # TODO!!!!!!!!!!!!#Choosing number of clusters
# for k in range(10):  #--->create loop over ks
# 	print("COM ", k, " clusters!!! vai at+e 10")
k=3
itr = 0
delta_cost = 10000.0
list_cost = []
cost = 100000.0
epsilon = 0.00001  # adjust-----> small value!!!!

# CENTROIDs INITIALIZATION:

iniCentroids = rdd_file.takeSample(False, k)
# verification to make sure we dont have repeated centroids!!!
mset = [list(x) for x in set(tuple(x) for x in iniCentroids)]
while (len(iniCentroids) is not len(mset)):
	print("will take another sample1")
	# 			#take new samples until length are the same!
	iniCentroids = rdd_file.takeSample(False, k)
	mset = [list(x) for x in set(tuple(x) for x in iniCentroids)]

centroidstemp=sc.parallelize(iniCentroids).zipWithIndex()
centroidsRDD=centroidstemp.map(lambda row: (row[-1], row[0]))			#[(0, (idc, xc,yc,zc,...)), (1, (idc,xc, yc, zc...))...]
print("centroids AQQQQUIIIIIIIC------>    ", centroidsRDD.collect())
# if k==5:
# 	sys.exit("RDD JOINED___> CHECK CENTROIDS!!!!!!!!!!!!!!1")
#
# centroids_table = []  # Create dictionary of centroids from inicentroids
# for c in range(k):
#	 centroids_table.append((c, iniCentroids[c]))
#	 print(centroids_table[c])
#
# dict_centroids = create_dict(centroids_table)  # hashtable of centroids {0: [x, y, z], 1:[, , , ]...}



print("COST IS::::!!!!!!!!!!!!!!!!!co coc ococococococ	   cpc oieqhfoqfbd1j doi1h!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1::: ",
	  cost)
print("DELTA IS:::!!!!!!!!!!!!!!!!!!!!!!!!!!COCOCOCOCOCOCOCOCOCOC!!!!!!!!!!!!!!!!!!!!!!!!:::: ", delta_cost)
rdd_temp1=rdd_file.flatMap(lambda row:[(i, row) for i in range(k)]).cache()	# [ ( 0, (id1, x1, y1, z1, ...)),( 1, (id1, x1, y1, z1, ...),  (2, (p1),.....

# KMEANS CYCLE:
while (delta_cost > epsilon):
	itr = itr + 1

# DISTANCE COMPUTATION
	# Calculate closest centroid and add it to each point
	# 	rdd_centroids = rdd_file.map(lambda row : (row,calculate_closest_centroid(row,centroids, k)))
	rdd_joined=rdd_temp1.join(centroidsRDD)  #[ ( 0, (id1, x1, y1, z1, ...), (idc0, xc0, yc0, zc0)) ... (2, (id1, x1, y1, z1, ...), (idc2, xc2, yc2, zc2))...]
	# print("rdd_joined :  ", rdd_joined.collect())


	RDD_distances = rdd_joined.map(lambda row: (row[1][0], (row[0],calculate_distance(row[1][0], row[1][1]))))
	# print("RDD_distances :  ", RDD_distances.collect())
	rdd_closestCentroid = RDD_distances.reduceByKey(lambda a,b: min([a,b], key = lambda t: t[1]))
	# print("rdd_closestCentroid:  ", rdd_closestCentroid.collect())   #[((2698644.0, 2.0, 3.0, 1.0, 1.0), (1, 0.0)), ((1525642.0, 22.0, 5.0, 13.0, 1.0), (1, 548.0)), ((1405630.0, 7.0, 10.0, 1.0, 1.0), (1, 74.0)), ((2036988.0, 2.0, 1.0, 1.0, 1.0), (0, 0.0)), ((2679332.0, 2.0, 1.0, 1.0, 1.0), (0, 0.0)), ((1994680.0, 2.0, 1.0, 1.0, 1.0), (0, 0.0)), ((1310528.0, 57.0, 2.0, 15.0, 1.0), (0, 3222.0)), ((1299907.0, 2.0, 1.0, 1.0, 1.0), (0, 0.0))])

# 	sys.exit("DISTANCE COMPUTATION FINISHED!!!!!!!!!!!!!!1")

#
# # COST COMPUTATION:
#
	cost_prev = cost
# # 	cost_lista_test = rdd_closestCentroid.map(lambda a: a[1])
# # 	print(cost_lista_test.collect())			#[(1, 0.0), (1, 548.0), (1, 74.0), (0, 0.0), (0, 0.0), (0, 0.0), (0, 3222.0), (0, 0.0)]
# # 	cost_lista_test1 = rdd_closestCentroid.map(lambda a: a[1][1])
# # 	print(cost_lista_test1.collect())		#[0.0, 548.0, 74.0, 0.0, 0.0, 0.0, 3222.0, 0.0]
# # 	sum_dist = rdd_closestCentroid.reduce(lambda a,b : a[1][1]+b[1][1])  #invalid index to scalar variable.
# 	sum_dist = sc.accumulator(0)
	rdd_dist = rdd_closestCentroid.map(lambda row :  (row[-1][0], row[-1][-1])).reduceByKey(lambda a,b: (a+b))
# 	rdd_sum_dist.foreach(g)
#
# 	print("SUM OF DISTANCESSSS TO COMP COST ", sum_dist)
	dist = rdd_dist.collect();
	cost = sum(x[1] for x in dist) / count_points
	#sys.exit("DCOSTTTTTTD!!!!!!!!!!!!!!1")
#	 [((2698644.0, 2.0, 3.0, 1.0, 1.0), (2, 0.0)), ((1525642.0, 22.0, 5.0, 13.0, 1.0), (1, 0.0)), ((1405630.0, 7.0, 10.0, 1.0, 1.0), (2, 74.0)), ((2036988.0, 2.0, 1.0, 1.0, 1.0), (2, 4.0)), ((2679332.0, 2.0, 1.0, 1.0, 1.0), (2, 4.0)), ((1994680.0, 2.0, 1.0, 1.0, 1.0), (2, 4.0)), ((1310528.0, 57.0, 2.0, 15.0, 1.0), (0, 0.0)), ((1299907.0, 2.0, 1.0, 1.0, 1.0), (2, 4.0))])



	list_cost.append(cost)
	delta_cost = abs(cost_prev - cost)
	# # 	print(" new DELTA IS ", delta_cost)

# UPDATE CENTROIDS:
	rdd_temp = rdd_closestCentroid.map(lambda row: (row[1][0], row[0]))
	# print("rdd_temp ", rdd_temp.collect())
	# 	rdd_count = rdd_temp.map(lambda row : (row[0],1)).reduceByKey(lambda a,b : (a+b)).sortByKey()
	rdd_count = rdd_temp.map(lambda row: (row[0], 1)).reduceByKey(lambda a, b: (a + b))
	# print("N_pontos por cluster!!!   rdd_count ", rdd_count.collect())
#	count = rdd_count.collect()
#	dict_count = create_dict(count)
	rdd_sum_computation = rdd_temp.reduceByKey(lambda a, b: [x + y for x, y in zip(a, b)])  #[(0, (sumid, sumx, xumy, sumz, ...)]
	# print("Suma das coordenadas dos pontos de cada clister   rdd_sum ", rdd_sum_computation.collect())

	rdd_index_sum_counts = rdd_sum_computation.join(rdd_count)
	# 	rdd_mean_computation = rdd_sum_computation.map(lambda row : (row[0],compute_newCentroids(row, count))).sortByKey()
	centroidsRDD = rdd_index_sum_counts.map(lambda row: (row[0], compute_newCentroids(row[-1][0],row[-1][-1]))).cache()
	# print("!!!!!!!!*****   centroidsRDD: ",centroidsRDD.collect())
# 	sys.exit("check centroidsRDD   should be like [(0, (idc, xc,yc,zc,...)), (1, (idc,xc, yc, zc...))...]")

	# 	if itr in arange(0, 10000, 10):
	# print("COST IS::::::: ", cost)
	# print("DELTA IS::::::: ", delta_cost)





# OUTPUT CREATION:
now = datetime.datetime.now()
nowsrt = now.strftime("%Y-%m-%d_%H%M")

	# only USE FOR SMALL SUBSET OF DATA!!!!!!!!!!! writes assignment table to file .> makes COLLECT()+++++++++++++++++++++++++++++++++++++++++++++++
#
rdd_assign=rdd_temp.map(lambda row: (row[0], row[-1][1:])).sortByKey()
assign=rdd_assign.collect()
output_pontos=open("pontosTeste_" +str(nowsrt)+ ".csv", "w")
writer = csv.writer(output_pontos)
writer.writerows(assign)
	#
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# USE FOR WHOLE DATASET: writes assignment table to HDFS:++++++++++++++++++++++++++++++++++++++++
# 
# rdd_assign = rdd_temp.map(lambda row: (row[0], row[-1][1:])).sortByKey()
# st_path = "pontos_output_" + str(nowsrt)
# rdd_assign.saveAsTextFile(st_path)

# ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

centroids=centroidsRDD.map(lambda row: (row[0],row[-1][1:])).collect()
outputFile = open("outputkmeans_" + nowsrt + ".csv", "w")
writer = csv.writer(outputFile)
writer.writerow(centroids)

print(list_cost)
outputCost = open("costoverIterations_" + nowsrt + ".csv", "w")
outputCost.writelines(["%s\n" % item for item in list_cost])






