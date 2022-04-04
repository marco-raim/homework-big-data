from pyspark import SparkContext, SparkConf
import sys
import os
import random as rand

def main():

	# CHECKING NUMBER OF CMD LINE PARAMTERS
	assert len(sys.argv) == 5, "Usage: python WordCountExample.py <K> <file_name>"

	# SPARK SETUP
	conf = SparkConf().setAppName('HW1').setMaster("local[*]")
	sc = SparkContext(conf=conf)

	# INPUT READING

	# 1. Read number of partitions K
	K = sys.argv[1]
	assert K.isdigit(), "K must be an integer"
	K = int(K)
	
	# 2. Read H
	H = sys.argv[2]
	assert H.isdigit(), "H must be an integer"
	H = int(H)
	
	#3. Read string S
	S = sys.argv[3]
	assert isinstance(S, str), "S must be a string"
	S = str(S)

	# 4. Read input file and subdivide it into K random partitions
	data_path = sys.argv[4]
	assert os.path.isfile(data_path), "File or folder not found"
	rawData = sc.textFile(data_path,minPartitions=K).cache()
	rawData.repartition(numPartitions=K)

	# TASK 1.
	print('\n--> TASK 1')
	
	nRows = rawData.count()
	print("\nNumber of rows in the input file = ", nRows)

	# TASK 2.
	print('\n--> TASK 2')
	
	rawData = rawData.map(lambda line: line.split(','))
	
	if S == 'all':
		rawDataFiltered = rawData.filter(lambda x: int(x[3]) > 0)
	else:
		rawDataFiltered = rawData.filter(lambda x: int(x[3]) > 0)
		rawDataFiltered = rawDataFiltered.filter(lambda s: str(s[7]) == S)
	
	rawDataPC = rawDataFiltered.map(lambda x: (x[2], x[6]))
	rawDataGroup = rawDataPC.groupByKey().map(lambda x : (x[0], list(x[1])))
	rawDataGroupNoDuplicates = rawDataGroup.mapValues(lambda x: set(x))
	productCustomer = rawDataGroupNoDuplicates.flatMap(lambda l: [(l[0], v) for v in l[1]])		# <-- productCustomer without using distinct() method
	
	productCustomerUnfair = rawDataPC.distinct()		# <-- productCustomer using distinct() method
	
	# Printing the number of lines of each RDD
	
	nRowsDataPC = rawDataPC.count()
	print("\n- Number of rows in the filtered input file = ", nRowsDataPC)					# <-- It must be: (n. of lines of rawDataPC) <= (n. of lines of rawData)
	
	nRowsDataGroup = rawDataGroup.count()
	print("- Number of rows in the filtered and grouped input file = ", nRowsDataGroup)		# <-- It must be: (n. of lines of rawDataGroup) <= (n. of lines of rawDataPC)
	
	nRowsPC = productCustomer.count()
	print("- Number of rows in productCustomer = ", nRowsPC)					# <-- Print the n. of lines of productCustomer
	
	nRowsPCUnfair = productCustomerUnfair.count()
	print("- Number of rows in productCustomerUnfair = ", nRowsPCUnfair)		# <-- Verifying that the two productCustomer's n. of lines coincides
	
	# Printing the RDDs
	
	print('\n- rawData with only productID and customerID:\n')
	print(rawDataPC.collect())
	
	print('\n- rawData with only productID and customerID grouped by key:\n')
	print(rawDataGroup.collect())
	
	print('\n- productCustomer:\n')
	print(productCustomer.collect())				# <-- It prints the "fair" productCustomer
	
	print('\n- productCustomerUnfair:\n')
	print(productCustomerUnfair.collect())			# <-- Verifying that the two productCustomer coincides
	
	# TASK 3.
	print('\n--> TASK 3')
	
	def f(x):
		for i in x:
			yield(i)
	
	productPopularity1 = productCustomer.mapPartitions(f).groupByKey().mapValues(lambda x: len(x))
	
	print('\n- productPopularity1:\n')
	print(productPopularity1.collect())
	
	# TASK 4.
	print('\n--> TASK 4')
	
	productPopularity2 = productCustomer.map(lambda x : (x[0],1)).reduceByKey(lambda x, y: x + y)
	
	
	print('\n- productPopularity2:\n')
	print(productPopularity2.collect())
	
	# TASK 5.
	print('\n--> TASK 5')
	
	if H > 0:
		productHighestPopularity = productPopularity2.sortBy(lambda x: x[1], ascending=False).take(H)
	
		print('\n- PRODUCT WITH HIGHEST POPULARITY:\n')
		print(productHighestPopularity)
	else:
		print('\n- Nothing to see here: H = 0!')
	
	# TASK 6.
	print('\n--> TASK 6')
	if H == 0:
		productPopularity1List = productPopularity1.sortByKey().collect()
		productPopularity2List = productPopularity2.sortByKey().collect()
	
		print('\n- productPopularity1 in increasing lexicographic order:\n')
		print(productPopularity1List)
		print('\n- productPopularity2 in increasing lexicographic order:\n')
		print(productPopularity2List)
	else:
		print('\n- Nothing to see here: H > 0!\n')

if __name__ == "__main__":
	main()
