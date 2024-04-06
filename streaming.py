from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
from pyspark import StorageLevel
import threading
import sys
import random
import numpy as np
import operator

#python "C:\Users\alecr\OneDrive\Escritorio\Master Data Science\Big Data Computing\H3\DistinctItemsExample.py" 5 30 1 8000 10 8888


# After how many items should we stop?
THRESHOLD = 10000000
    
def C_matrix_hash(key, a, b, p):
    global W
    #s = int(key) + a + b
    #random.seed(s)
    g = random.choice([-1, 1])
    return (((a*int(key) + b) % p) % W, g)

 
# Operations to perform after receiving an RDD 'batch' at time 'time'
def process_batch(time, batch):
    
    # We are working on the batch at time `time`.
    global streamLength, histogram, D, W, left, right, matrixC, p, a, b, matrixF, hash_dic
    
    #count the number of total items
    batch_size = batch.count()
    
    # If we already have enough points (> THRESHOLD), skip this batch.
    if streamLength[0]>=THRESHOLD:
        return
    streamLength[0] += batch_size
    
    # Extract the distinct items from the batch
    batch_items = batch.map(lambda s: (int(s), 1)).reduceByKey(lambda i1, i2: 1).collectAsMap()
    
    # Update the streaming state
    # Add an array of tuples (hash(d),g(d)) for i=0,..,D for each item
    
    for key in batch_items:
        if key not in histogram:
            histogram[key] = 1    
        
        if (key not in hash_dic) and (key >= left) and (key <= right):
            hash_vector = []
            for j in range(0,D):
                hash_vector.append(C_matrix_hash(key, a[j], b[j], p))
            hash_dic[key] = hash_vector 
     
    batch_items2 = (batch.filter(lambda x: int(x) in range(left, right+1))
                    .map(lambda s: (int(s), 1))
                    .groupByKey()
                    .mapValues(len)
                    .collectAsMap())
           
    for key,freq in batch_items2.items():
        
        if key not in matrixF:
            matrixF[key] = [freq]
    
        else:
            matrixF[key][0] += freq
            
        for i in range(D):
            h = hash_dic[key][i][0]
            g = hash_dic[key][i][1]
            matrixC[i, h] += freq * g        
            
    # If we wanted, here we could run some additional code on the global histogram
    if batch_size > 0:
        print("Batch size at time [{0}] is: {1}".format(time, batch_size))

    if streamLength[0] >= THRESHOLD:
        stopping_condition.set()
    

if __name__ == '__main__':
    assert len(sys.argv) == 7, "D:rows W:Columns left, right: Points of the interval K: number of top frequent items USAGE: port"

    # IMPORTANT: when running locally, it is *fundamental* that the
    # `master` setting is "local[*]" or "local[n]" with n > 1, otherwise
    # there will be no processor running the streaming computation and your
    # code will crash with an out of memory (because the input keeps accumulating).
    conf = SparkConf().setMaster("local[*]").setAppName("DistinctExample")
    # If you get an OutOfMemory error in the heap consider to increase the
    # executor and drivers heap space with the following lines:
    # conf = conf.set("spark.executor.memory", "4g").set("spark.driver.memory", "4g")
    
    
    # Here, with the duration you can control how large to make your batches.
    # Beware that the data generator we are using is very fast, so the suggestion
    # is to use batches of less than a second, otherwise you might exhaust the memory.
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 1)  # Batch duration of 1 second
    ssc.sparkContext.setLogLevel("ERROR")
    
    # TECHNICAL DETAIL:
    # The streaming spark context and our code and the tasks that are spawned all
    # work concurrently. To ensure a clean shut down we use this semaphore.
    # The main thread will first acquire the only permit available and then try
    # to acquire another one right after spinning up the streaming computation.
    # The second tentative at acquiring the semaphore will make the main thread
    # wait on the call. Then, in the `foreachRDD` call, when the stopping condition
    # is met we release the semaphore, basically giving "green light" to the main
    # thread to shut down the computation.
    # We cannot call `ssc.stop()` directly in `foreachRDD` because it might lead
    # to deadlocks.
    stopping_condition = threading.Event()
    
    
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # INPUT READING
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    # 1. Rows and columns count Sketch
    D = sys.argv[1]
    assert D.isdigit(), "D must be an integer"
    D = int(D)
    
    W = sys.argv[2]
    assert W.isdigit(), "W must be an integer"
    W = int(W)
    
    # 2. Points of the interval
    left = sys.argv[3]
    assert left.isdigit(), "Left must be an integer"
    left = int(left)
    
    right = sys.argv[4]
    assert right.isdigit(), "Right must be an integer"
    right = int(right)
    
    interval = [left, right]
    
    #3. Number of top frequent items
    K = sys.argv[5]
    assert K.isdigit(), "K must be an integer"
    K= int(K)
    
    #Port
    portExp = int(sys.argv[6])
    print("Receiving data from port =", portExp)
    
    
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&
    # DEFINING THE REQUIRED DATA STRUCTURES TO MAINTAIN THE STATE OF THE STREAM
    # &&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&

    streamLength = [0] # Stream length (an array to be passed by reference)
    histogram = {} # Hash Table for the distinct elements
    hash_dic = {} # Hash Table for the distinct elements in R
    matrixF = {} # Hash Table for the frequencies of the distinct elements in the interval R
    matrixC = np.zeros([D, W]) #Initialize a matrix for the counters

    
    #Parameters hash function
    #Define outside the process batch
    p = 8191
    a = [random.randint(1,p-1) for i in range(D)]
    b = [random.randint(0,p-1) for i in range(D)]
    

    # CODE TO PROCESS AN UNBOUNDED STREAM OF DATA IN BATCHES
    stream = ssc.socketTextStream("algo.dei.unipd.it", portExp, StorageLevel.MEMORY_AND_DISK)
    # For each batch, to the following.
    # BEWARE: the `foreachRDD` method has "at least once semantics", meaning
    # that the same data might be processed multiple times in case of failure.
    stream.foreachRDD(lambda time, batch: process_batch(time, batch))
    
    # MANAGING STREAMING SPARK CONTEXT
    print("Starting streaming engine")
    ssc.start()
    print("Waiting for shutdown condition")
    stopping_condition.wait()
    print("Stopping the streaming engine")
    # NOTE: You will see some data being processed even after the
    # shutdown command has been issued: This is because we are asking
    # to stop "gracefully", meaning that any outstanding work
    # will be done.
    ssc.stop(False, True)
    print("Streaming engine stopped")
    
    # COMPUTE AND PRINT FINAL STATISTICS
    print("D =", D,"W = ", W,"[left,right] =", interval, "K =", K, "Port =", portExp)
    print("Total number of items  =", streamLength[0])
    #print("Number of distinct items =", len(histogram))
    #largest_item = max(histogram.keys())
    #print("Largest item =", largest_item)
    
    
    #Calculated Estimated Frequencies
    #matrixF first position real frequency, second position estimated frequency, third position errors
    for key in hash_dic:
            list_values = []
            for j in range(0, D):
                value = matrixC[j, hash_dic[key][j][0]] * hash_dic[key][j][1]
                list_values.append(value)
                
            est_freq = np.median(list_values)
            error = np.abs((matrixF[key][0] - est_freq)/matrixF[key][0])
            matrixF[key].append(est_freq)
            matrixF[key].append(error)
            
 
    #Keep the top K if the matrix
    histogram_top = sorted(matrixF.items(), key=lambda x:x[1],reverse=True)
    histogram_top_K = histogram_top[:K]
    
  
    #Real and estimated second moment
    F2_true = 0
    F2_est = 0
    true_freq = 0
    
    for key in matrixF:
        F2_true += matrixF[key][0] ** 2
        true_freq += matrixF[key][0]
        
    f2 = []   
    for j in range(0, D):
        aux = 0
        for h in range(0, W):
            aux += matrixC[j , h] ** 2

        f2.append(aux)   
        
    F2_est = np.median(f2)
    
    
    print("Total number of items in", interval ,"=", true_freq)
    print("Number of distinct items in ", interval ,"=", len(matrixF))
    
    
    # Print the top K values
    avg_error = 0
    for idx in range(len(histogram_top_K)):
        avg_error += histogram_top_K[idx][1][2]
        if K <= 20:
            print("Item ", histogram_top_K[idx][0], "Freq = ", histogram_top_K[idx][1][0], "Est. Freq = ", histogram_top_K[idx][1][1])
    
    print("Avg err for top ", K, " = ", avg_error/K)
    
    print("F2 = ", F2_true/(true_freq**2))
    print("F2 Estimate = ", F2_est/(true_freq**2))

   

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    