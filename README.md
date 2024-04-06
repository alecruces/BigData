# Big-Data-Computing
Exploring Distributed Triangle Counting and Stream Processing with Spark
a.	Part 1: Distributed Triangle Counting with Apache Spark: Node Coloring vs. Spark Partitions
Description: This project aims to implement and compare two distributed algorithms for approximating the number of triangles in a graph using Apache Spark. The code takes an input file containing graph edges and subdivides it into a specified number of random partitions. The first algorithm, MR_ApproxTCwithNodeColors, utilizes node coloring to assign colors to vertices and then aggregates triangles based on these colors. The second algorithm, MR_ApproxTCwithSparkPartitions, employs Spark's partitioning mechanism to distribute the computation across the cluster efficiently.
Throughout the implementation, the code leverages Spark's parallel processing capabilities, utilizing functions such as mapPartitionsWithIndex and groupByKey to distribute and aggregate computations. The code also includes error handling for input parameters and file validation.

Part 2: Triangle Counting in CloudVeneto Cluster
Description: This code runs a Spark program on the CloudVeneto cluster. As for Part 1, the objective is to estimate (approximately or exactly) the number of triangles in an undirected graph 𝐺=(𝑉,𝐸). More specifically, the program implement two algorithms.
The first algorithm, MR_ApproxTCwithNodeColors, utilizes node coloring to assign colors to vertices and aggregates triangles based on these colors. The second algorithm, MR_ExactTC, precisely counts triangles in the graph by exhaustively examining all possible triangles.

Both algorithms leverage the parallel processing capabilities of Apache Spark to distribute computation across the cluster. The code also includes functionality to measure the execution time and provides options to run multiple repetitions for performance evaluation.

Part 3: Count Sketch with Spark Streaming
Description: This code is designed to process an unbounded stream of data in batches using Apache Spark's StreamingContext. It implements a sketch-based algorithm to estimate the distinct items in the stream, as well as their frequencies.
		Keywords: Big Data computing

b.	Data: The data is private and cannot be uploaded here.
c.	Software and Tools:
i.	 Apache Spark 
ii.	Python: PySpark
iii.	CloudVeneto: It is an OpenStack-based cloud. It allows the instantiation of Virtual Machines (VMs).
iv.	Streaming Spark's StreamingContext,: This enables the processing of continuous data streams in real-time.
d.	Files: Code
e.	Technical Information: https://github.com/enricobolzonello/bigdata_homeworks?tab=readme-ov-file#organization-of-the-repository
![image](https://github.com/alecruces/Big-Data-Computing/assets/67338986/19a34a48-8f3f-4ef6-89e7-c8eee01109df)
