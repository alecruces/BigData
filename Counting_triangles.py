# -*- coding: utf-8 -*-
"""
Created on Thu Mar 30 20:26:14 2023

@author: José Chacón, Alejandra Cruces, Mario Tapia
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import sys
import os
import numpy as np
import random
import pandas as pd
from collections import defaultdict
import time

# Hash Function: Create a hash function to assign to each vertex a color
def hash_vertices(edges, C):
    p = 8191
    a = random.randint(1,p-1)
    b = random.randint(0,p-1)
    edges_elements = edges.flatMap(lambda x: x).collect()
    vertices = set(edges_elements)
    color_dict = {v : (((v * a) + b) % p) % C  for v in vertices}
    return color_dict

# It compare the colors of each vertex from the edge
def hash_edges(edges, color_dict):
    v_a, v_b = edges
    pairs_vertex = {}
    if color_dict[v_a] == color_dict[v_b]:
        color = color_dict[v_a]
        pairs_vertex[edges] = color
        
    return [(pairs_vertex[key], key ) for key in pairs_vertex.keys()]

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


#THE FIRST ALGORITHM
def MR_ApproxTCwithNodeColors(edges, C):
    # Create Vertex-Color Dictionary
    color_dict = hash_vertices(edges, C)
    triangles_count = (edges.flatMap(lambda x: hash_edges(x, color_dict))       # <-- MAP PHASE (R1)
                 .groupByKey()                                                  # <-- SHUFFLE+GROUPING
                 .mapValues(lambda x: CountTriangles(x))                        # <-- REDUCE PHASE (R1)
                 .values().sum())                                               # <-- REDUCE PHASE (R2)

    return (((C*C)*triangles_count))


# THE SECOND ALGORITHM

def MR_ApproxTCwithSparkPartitions(edges, C): 
    triangles_count = (edges.mapPartitionsWithIndex(CountTriangles_PerPartitions)        # <-- REDUCE PHASE (R1)
                .values().sum())                                                         # <-- REDUCE PHASE (R2)
    return ((C*C)*(triangles_count))



def main():
    
    # CHECKING NUMBER OF CMD LINE PARAMTERS
    assert len(sys.argv) == 4, "Usage: python G002HW1.py <C> <R> <file_name>"

    # SPARK SETUP
    
    # SPARK SETUP
    conf = SparkConf().setAppName('CountingTrianglesByColors')
    sc = SparkContext(conf=conf)

    # INPUT READING

    # 1. Read number of colors
    C = sys.argv[1]
    assert C.isdigit(), "C must be an integer"
    C = int(C)
    
    # 2. Read number of repetitions
    R = sys.argv[2]
    assert R.isdigit(), "R must be an integer"
    R = int(R)


    # 3. Read input file and subdivide it into C random partitions
    data_path = sys.argv[3]
    assert os.path.isfile(data_path), "File or folder not found"
    rawData = sc.textFile(data_path,minPartitions=C)
    edges = rawData.map(lambda x: x.split(','))\
                .map(lambda x: (int(x[0]),int(x[1])))\
                .repartition(numPartitions=C)\
                .cache()
    
 
    # SETTING GLOBAL VARIABLES
    edges_count = edges.count();
    
    
    # ALGORITHM 1: MR_ApproxTCwithNodeColors
    
    if R>1:
        list_count = []
        list_time = []
        for i in range(0, R):
            start1 = time.time()
            colors_triangles = MR_ApproxTCwithNodeColors(edges,C)
            end1 = time.time()
            list_count.append(colors_triangles)
            list_time.append(end1-start1)
        
    else:
        start1 = time.time()
        colors_triangles = MR_ApproxTCwithNodeColors(edges,C)
        end1 = time.time()

        
        
    #ALGORITHM 2: MR_ApproxTCwithSparkPartitions
    start2 = time.time()
    spark_triangles = MR_ApproxTCwithSparkPartitions(edges, C)
    end2 = time.time()

    #PRINTS
    print("Dataset =", data_path)
    print("Number of Edges = ", edges_count)
    print("Number of Colors", C)
    print("Number of Repetitions", R)
    
    print("Approximation through node coloring")
    if R>1:
        print("- Number of Triangles (median over ", R, " runs) = ", int(np.median(list_count)))
        print("- Running time (mean over ", R, " runs) = ", int(np.mean(list_time)*1000), " ms")
    else:    
        print("- Number of Triangles = ", colors_triangles)
        print("- Running time =  ", int((end1-start1)*1000), " ms")
        
    print("Approximation through Spark partitions")
    print("- Number of Triangles = " , spark_triangles)
    print("- Running time = ",  int((end2-start2)*1000), " ms")
    
    
if __name__ == "__main__":
    main()
    
    