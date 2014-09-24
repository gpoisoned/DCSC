#Distributed Cache Example Hadoop 2.5#
Author: gpoisoned
Modified: Sep 23, 2014

This uses distributed cache mechanism to send a cache file across all mappers.

This includes a makefile to automate some stuff:

make clean -> removes .jar and .class files

make jar -> compiles the java code

make prepare -> prepares the hdfs with necessary directories and copies the input files
