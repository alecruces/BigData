# Exploring Distributed Triangle Counting and Stream Processing with Spark

> Implementing and comparing distributed algorithms for triangle counting in graphs using Apache Spark.

## Description
This project aims to implement and compare distributed algorithms for approximating the number of triangles in a graph using Apache Spark. It consists of several parts:

<p align="center">
<img src="https://github.com/alecruces/BigData/assets/67338986/5de401e2-92b0-4340-b651-d2385adda1e1" alt="BigData" style="width:400px;height:auto;"/>
</p>

---

## Table of Contents

1. [About the Project](#about-the-project)
2. [Project Structure](#project-structure)
3. [Key Results](#key-results)
4. [Technologies Used](#technologies-used)
5. [Setup & Installation](#setup--installation)
6. [Usage](#usage)
7. [Contributing](#contributing)
8. [License](#license)
9. [Contact](#contact)

---

### About the Project

This project explores distributed computing techniques for approximating the number of triangles in a graph. Using Apache Spark, three main approaches are implemented: triangle counting with node coloring and Spark partitions, precise triangle counting on a cloud cluster, and stream processing with Count Sketch in Spark Streaming.

### Project Structure

The project consists of three parts, each addressing different aspects of distributed triangle counting and stream processing:

#### Part 1: Distributed Triangle Counting with Apache Spark (Node Coloring vs. Spark Partitions)
This part compares two approaches:

- **`MR_ApproxTCwithNodeColors`**: Uses node coloring to assign colors to vertices and aggregates triangles based on these colors.
- **`MR_ApproxTCwithSparkPartitions`**: Leverages Spark's native partitioning for efficient triangle aggregation across the cluster.

Key features in this part include Spark functions such as `mapPartitionsWithIndex` and `groupByKey`, along with error handling for input parameters and file validation.

#### Part 2: Triangle Counting on the CloudVeneto Cluster
This part implements triangle counting on the CloudVeneto cluster:

- **`MR_ApproxTCwithNodeColors`**: Reuses the node coloring algorithm for approximating triangles.
- **`MR_ExactTC`**: Performs an exact count of triangles by examining all possible triangles.

Both algorithms are designed to scale efficiently across the cluster using Spark, and they include functionality to measure execution time for performance evaluation.

#### Part 3: Count Sketch with Spark Streaming
Processes an unbounded data stream using Spark’s `StreamingContext`, implementing a Count Sketch algorithm to estimate distinct item frequencies in real time.

### Key Results

- **Algorithm Comparison**:
  - **Approximation with Node Colors** and **Spark Partitions** provide scalable solutions for triangle counting, with node coloring offering effective approximation when exact counting is computationally infeasible.
  - **Exact Triangle Counting** provides precise results but at a higher computational cost, making it suitable only for smaller datasets.
- **Streaming Efficiency**: The Count Sketch implementation in Spark Streaming offers real-time frequency estimates, suitable for applications with continuous data inflow.

### Technologies Used

> Tools and frameworks employed in this project.

- **Apache Spark**: Distributed processing framework for triangle counting and streaming.
- **Python (PySpark)**: Language and API for Spark.
- **CloudVeneto**: OpenStack-based cloud infrastructure for distributed processing.
- **Spark Streaming**: Used in Part 3 to handle streaming data with `StreamingContext`.

### Setup & Installation

Clone the repository and set up the environment to run each part of the project.

#### Prerequisites
- **Apache Spark** installed and configured.
- Access to **CloudVeneto** or another OpenStack-based cluster (for Part 2).
- Python dependencies as outlined in `requirements.txt`.

```bash
# Clone the repository
git clone https://github.com/username/BigData.git

# Navigate to the project directory
cd BigData

# Install dependencies
pip install -r requirements.txt

1. Part 1: `counting_triangles.py`
2. Part 2: `streaming.py`
3. Part 3: `counting_triangles_cloudveneto.py`
```

### Usage

The repository includes the following scripts for each part of the project:

- **Part 1: `counting_triangles.py`**  
  Script implementing triangle counting with Node Coloring and Spark Partitions.

- **Part 2: `counting_triangles_cloudveneto.py`**  
  Script to perform triangle counting on CloudVeneto, using both approximation and exact methods.

- **Part 3: `streaming.py`**  
  Script implementing Count Sketch for stream processing using Spark’s `StreamingContext`.

To run the project on your local Spark setup or on CloudVeneto, execute the scripts as follows:

```bash
# Part 1: Triangle Counting with Node Coloring and Spark Partitions
spark-submit counting_triangles.py

# Part 2: Triangle Counting on CloudVeneto
spark-submit counting_triangles_cloudveneto.py

# Part 3: Stream Processing with Count Sketch
spark-submit streaming.py
```

### Contributing
Contributions are welcome! Please see the contributing guidelines for more details.
