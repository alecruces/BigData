# Exploring Distributed Triangle Counting and Stream Processing with Spark

## Description:
This project aims to implement and compare distributed algorithms for approximating the number of triangles in a graph using Apache Spark. It consists of several parts:

## Part 1: Distributed Triangle Counting with Apache Spark: Node Coloring vs. Spark Partitions
This part implements and compares two distributed algorithms:

- **MR_ApproxTCwithNodeColors**: Utilizes node coloring to assign colors to vertices and aggregates triangles based on these colors.
- **MR_ApproxTCwithSparkPartitions**: Employs Spark's partitioning mechanism to distribute computation efficiently across the cluster.

The implementation leverages Spark's parallel processing capabilities, utilizing functions such as `mapPartitionsWithIndex` and `groupByKey`, along with error handling for input parameters and file validation.

## Part 2: Triangle Counting in CloudVeneto Cluster
This part runs a Spark program on the CloudVeneto cluster to estimate the number of triangles in an undirected graph 𝐺=(𝑉,𝐸) using two algorithms:

- **MR_ApproxTCwithNodeColors**: Utilizes node coloring to assign colors to vertices and aggregates triangles based on these colors.
- **MR_ExactTC**: Precisely counts triangles in the graph by exhaustively examining all possible triangles.

Both algorithms leverage Apache Spark's parallel processing capabilities across the cluster. The code also includes functionality to measure execution time and provides options for performance evaluation.

## Part 3: Count Sketch with Spark Streaming
This part processes an unbounded stream of data in batches using Apache Spark's `StreamingContext`. It implements a sketch-based algorithm to estimate the distinct items in the stream and their frequencies.

## Keywords:
Big Data computing

## Software and Tools:
- Apache Spark
- Python (PySpark)
- CloudVeneto (OpenStack-based cloud)
- Streaming Spark's `StreamingContext`

## Note:
The data used in this project is private and cannot be uploaded.

## Files:
Code
