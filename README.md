This repo contains implementations for my Master's project, "Comparing and Improving The Minimum Spanning Tree Algorithms in MapReduce" (In Persian) at the CE Algorithms Lab of Sharif University of Technology.

# Abstract
In recent decades, we have faced the enormous growth of data and graph volumes. This requires modern ways of computation and storage systems and algorithms.
MapReduce is a known way of processing Big Data in a Parallel and primarily Distributed setting. Theoretical models (e.g., Massively Parallel Computation) for Algorithms using this paradigm are commonly measured by the number of rounds and amount of needed communication.
We study the Minimum Spanning Tree (MST) as a fundamental graph problem. This problem in MapReduce is harder for sparse graphs. We introduce an algorithm that performs well comparing previous studies, especially for sparse graphs.

We present an empirical study by implementing some algorithms using MapReduce, Apache Spark, and Scala; and experimenting in a distributed setting that we configured to compare them and find important input parameters. In this experiment, we use various graphs with up to a hundred million edges/vertices. Our algorithm showed improvements in the number of rounds and running time for most of the experiments.
