prepare:
	-hdfs dfs -mkdir /user
	-hdfs dfs -mkdir /user/user
	-hdfs dfs -mkdir /user/user/input
	-hdfs dfs -put ./cite75_99.txt /user/user/input
	-hdfs dfs -put ./apat63_99.txt /user/user/input

clean:
	-hdfs dfs -rm -r /user/user/input
	-hdfs dfs -rm -r /user/user/output
	-hdfs dfs -rm -r /user/user/intermediate-output
	-hdfs dfs -rm -r /user/user/littlepig
