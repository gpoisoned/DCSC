jar: DistCache.java
	hadoop com.sun.tools.javac.Main DistCache.java
	jar cf DistCache.jar DistCache*.class

prepare:
	-hdfs dfs -rm -r /user/user/input
	-hdfs dfs -rm -r /user/user/output
	-hdfs dfs -rm -r /user/user/cache
	-hdfs dfs -mkdir /user
	-hdfs dfs -mkdir /user/user
	-hdfs dfs -mkdir /user/user/input
	-hdfs dfs -mkdir /user/user/cache
	-hdfs dfs -put ./dist_cache/file1 /user/user/input
	-hdfs dfs -put ./dist_cache/file2 /user/user/input
	-hdfs dfs -put ./dist_cache/cache_file /user/user/cache

clean:
	rm *.class
	rm *.jar
