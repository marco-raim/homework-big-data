from pyspark import SparkContext, SparkConf
import sys
import os


# function that filters the country
def filter_country(row):
    if S != 'all':
        if new_row[7] == S:
            return ((new_row[1], new_row[6]), new_row[3]) # 1 product, 6 customer, 3 quantity
        else:
            return
    else:
        return ((new_row[1], new_row[6]), new_row[3])
        

# function that gathers (prod, cust) pairs
def gather_prod_cust(pairs):
    pairs_quant = dict()
    # sum all quantities
    for p in pairs[0]:
        if p not in pairs_quant.keys():
            pairs_quant[p] = pairs[1]
        else:
            pairs_quant[p] = pairs_quant[p] + pairs[1]
    return [(key[0], key[1]) for key in pairs_quant.keys() if pairs_quant[key] > 0]

# function for point 2
def point2(rdd):
    productCustomer = rdd.flatMap(filter_country).mapPartitions(gather_prod_cust)
    return productCustomer  
    
# function that for each product computes the number of customers that bought it
def gather_customers(pairs):
    product_dict = dict()
    product = pairs[0]
    if product not in product_dict:
        product_dict[product] = 1
    else:
        product_dict[product] = product_dict[product] + 1
    return ((prod, product_dict[prod]) for prod in product_dict.keys())
        
# function for point 4 (sums all 1's)
def sum_of_1s(pair):
    list_of_1 = list(pair[1])
    return (pair[0], len(list_of_1)
         

    
    





############## main

def main():
    # checking the number of parameters (K, H, S, path)
    assert len(sys.argv) == 5, 'Usage: python3 HM... K H S path'
    
    # Spark configuration
    conf = SparkConf().setAppName('homework1.py').setMaster('local[*]')
    sc = SparkContext(conf = conf)
    
    # input reading
    # read the number of partition
    K = sys.argv[1]
    assert K.isdigit(), 'Usage: the number of partitions K must be an integer'
    K = int(K) # isdigit() returns true also with float
    
    # read the desider number of products with higher popularity
    H = sys.argv[2]
    assert H.isdigit(), 'Usage: the desired number of products with higher popularity must be an integer'
    H = int(H)
    
    # read the country
    S = sys.argv[3]
    #assert S.isinstance(S, str), 'Usage: country must be a string'
    
    # read text and put into an RDD
    data_path = sys.argv[4]
    assert os.path.isfile(data_path), 'File or folder not found'
    # creation of the RDD
    rawData = sc.textFile(data_path, minPartitions = K).cache()
    rawData.repartition(numPartitions = K)
    # split rows
    rawData = rawData.map(lambda row: row.split(','))
    
    # 1) print the number of elements of the RDD rawData
    print('Number of elements of RDD rawData: ', rawData.count())
    
    # 2) point
    productCustomer = point2(rawData)
    print('Number of (product, customer) with country filtered and quantity > 0: ', productCustomer.count())
    
    # 3) point
    productPopularity1 = productCustomer.mapPartitions(gather_customers)
    
    # 4) point
    productPopularity2 = productCustomer.mapToPair(lambda row: (row[0], 1)).reduceByKey(sum_of_1s)
    
    # 5) point
    if H > 0:
        list_popularity = list(productPopularity1.map(lambda (key, value): (value, key)).sortByKey(ascending = False).filter((x): x[0] > H))
    
    # 6) point
    elif H == 0:
        list1 = list(productPopularity1.sortByKey())
        print(list1)
        list2 = list(productPopularity2.sortByKey())
        print(list2)
        
    

    # global variables
    
    # print of the outputs
    
    
   
if __name__ == '__main__':
    main()    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
