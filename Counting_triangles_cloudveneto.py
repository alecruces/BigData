# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 20:26:14 2023

@author: José Chacón, Alejandra Cruces, Mario Tapia
"""

from pyspark import SparkContext, SparkConf
import sys
import numpy as np
import random
from collections import defaultdict
import time
   

# It compare the colors of each vertex from the edge
def hash_edges(edges, C, a, b, p):
    v_a, v_b = edges
    pairs_vertex = {}
    hC_a = ((a * v_a + b)% p) % C
    hC_b = ((a * v_b + b)% p) % C
    if hC_a == hC_b:
        color = hC_a
        pairs_vertex[edges] = color
        
    return [(pairs_vertex[key], key ) for key in pairs_vertex.keys()]

def triplets(edges, C, a, b, p):
    v_a, v_b = edges
    triplets = {}
    hC_a = ((a * v_a + b)% p) % C
    hC_b = ((a * v_b + b)% p) % C
    for i in range(0, C):
        k_i = tuple(np.sort([i, hC_a, hC_b]))
        triplets[k_i] = edges
    return [(key, triplets[key]) for key in triplets.keys()]
        


# Function for the MapPartitions with Index
def CountTriangles_PerPartitions(index, edges_pairs):
    list_edges = []
    for p in edges_pairs:
        list_edges.append(p)
    return [(index,CountTriangles(list_edges))] 

#Funciton to count the triangles from a list of edges
def CountTriangles(edges):
    # Create a defaultdict to store the neighbors of each vertex
    neighbors = defaultdict(set)
    for edge in edges:
        u, v = edge
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph.
    # To avoid duplicates, we count a triangle <u, v, w> only if u<v<w
    for u in neighbors:
        # Iterate over each pair of neighbors of u
        for v in neighbors[u]:
            if v > u:
                for w in neighbors[v]:
                    # If w is also a neighbor of u, then we have a triangle
                    if w > v and w in neighbors[u]:
                        triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

def CountTriangles2(colors_tuple, edges, rand_a, rand_b, p, num_colors):
    #We assume colors_tuple to be already sorted by increasing colors. Just transform in a list for simplicity
    colors = list(colors_tuple)  
    #Create a dictionary for adjacency list
    neighbors = defaultdict(set)
    #Creare a dictionary for storing node colors
    node_colors = dict()
    for edge in edges:

        u, v = edge
        node_colors[u]= ((rand_a*u+rand_b)%p)%num_colors
        node_colors[v]= ((rand_a*v+rand_b)%p)%num_colors
        neighbors[u].add(v)
        neighbors[v].add(u)

    # Initialize the triangle count to zero
    triangle_count = 0

    # Iterate over each vertex in the graph
    for v in neighbors:
        # Iterate over each pair of neighbors of v
        for u in neighbors[v]:
            if u > v:
                for w in neighbors[u]:
                    # If w is also a neighbor of v, then we have a triangle
                    if w > u and w in neighbors[v]:
                        # Sort colors by increasing values
                        triangle_colors = sorted((node_colors[u], node_colors[v], node_colors[w]))
                        # If triangle has the right colors, count it.
                        if colors==triangle_colors:
                            triangle_count += 1
    # Return the total number of triangles in the graph
    return triangle_count

def CountT_map(dic, a, b, p, C):
    return [(dic[0], CountTriangles2(dic[0], dic[1], a, b, p, C))]


#THE FIRST ALGORITHM
def MR_ApproxTCwithNodeColors(edges, C):
    p = 8191
    a = random.randint(1,p-1)
    b = random.randint(0,p-1)
    # Create Vertex-Color Dictionary
    # colors = hash_edges(edges, C, a, b, p)
    triangles_count = (edges.flatMap(lambda x: hash_edges(x, C, a, b, p))       # <-- MAP PHASE (R1)
                 .groupByKey()                                                  # <-- SHUFFLE+GROUPING
                 .mapValues(lambda x: CountTriangles(x))                        # <-- REDUCE PHASE (R1)
                 .values().sum())                                               # <-- REDUCE PHASE (R2)

    return (((C*C)*triangles_count))


# THE SECOND ALGORITHM

def MR_ExactTC(edges, C):
    p = 8191
    a = random.randint(1,p-1)
    b = random.randint(0,p-1)
    triangles_count = (edges.flatMap(lambda x: triplets(x, C, a, b, p))         # <-- MAP PHASE (R1)
                       .groupByKey()                                            # <-- SHUFFLE+GROUPING
                       .flatMap(lambda x: CountT_map(x, a, b, p, C))            # <-- REDUCE PHASE (R1)
                       .values().sum())                                         # <-- REDUCE PHASE (R2)

    return triangles_count



def main():
    
    # CHECKING NUMBER OF CMD LINE PARAMTERS
    #assert len(sys.argv) == 4, "Usage: python G002HW1.py <C> <R> <file_name>"

    # SPARK SETUP
    
    # SPARK SETUP
    conf = SparkConf().setAppName('CountingTrianglesByColors')
    conf.set("spark.locality.wait", "0s")
    sc = SparkContext(conf=conf)

    # INPUT READING

    # 1. Read number of colors
    C = sys.argv[1]
    #assert C.isdigit(), "C must be an integer"
    C = int(C)
    
    # 2. Read number of repetitions
    R = sys.argv[2]
    #assert R.isdigit(), "R must be an integer"
    R = int(R)

    # 3. Binary flag for selecting MR_ApproxTCwithNodeColors (0) or MR_ExactTC (1)
    F = sys.argv[3]
    F = int(F)
    
    # 4. Read input file and subdivide it into C random partitions
    data_path = sys.argv[4]
    #assert os.path.isfile(data_path), "File or folder not found"
    rawData = sc.textFile(data_path)
    edges = rawData.map(lambda x: x.split(','))\
                .map(lambda x: (int(x[0]),int(x[1])))\
                .repartition(numPartitions=32)\
                .cache()
    
 
    # SETTING GLOBAL VARIABLES
    edges_count = edges.count();
    
    # ALGORITHM 1: MR_ApproxTCwithNodeColors
    if F == 0:
        if R>1:
            list_count = []
            list_time = []
            for i in range(0, R):
                start = time.time()
                colors_triangles = MR_ApproxTCwithNodeColors(edges,C)
                end = time.time()
                list_count.append(colors_triangles)
                list_time.append(end-start)

        
        else:
            start = time.time()
            colors_triangles = MR_ApproxTCwithNodeColors(edges,C)
            end = time.time()
    
    elif F == 1:
        if R>1:
            list_count2 = []
            list_time2 = []
            for i in range(0, R):
                start2 = time.time()
                colors_triangles_exact = MR_ExactTC(edges,C)
                end2 = time.time()
                list_count2.append(colors_triangles_exact)
                list_time2.append(end2-start2)

    
        else:
            start2 = time.time()
            colors_triangles_exact = MR_ExactTC(edges,C)
            end2 = time.time()
    

    print("Dataset = ", data_path)
    print("Number of Edges = ", edges_count)
    print("Number of Colors = ", C)
    print("Number of Repetitions = ", R)
    
    if F == 0:
        print("Approximation algorithm with node coloring")
        if R>1:
            print("- Number of Triangles (median over ", R, " runs) = ", int(np.median(list_count)))
            print("- Running time (mean over ", R, " runs) = ", int(np.mean(list_time)*1000), " ms")
        else:
            print("- Number of Triangles = ", colors_triangles)
            print("- Running time =  ", int((end - start)*1000), " ms")


    elif F == 1:
        print("Exact algorithm with node coloring")
        if R>1:
            print("- Number of Triangles = " , list_count2[-1])
            print("- Running time (mean over ", R, " runs) = ", int(np.mean(list_time2)*1000), " ms")  
        else:
            print("- Number of Triangles = " , colors_triangles_exact)
            print("- Running time =  ", int((end2 - start2)*1000), " ms")
        


    
if __name__ == "__main__":
    main()
    
    