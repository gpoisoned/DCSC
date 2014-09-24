jar: Patent.java
	hadoop com.sun.tools.javac.Main Patent.java
	jar cf Patent.jar Patent*.class

prepare:
	-hdfs dfs -rm -r /user/user/input
	-hdfs dfs -rm -r /user/user/output
	-hdfs dfs -mkdir /user
	-hdfs dfs -mkdir /user/user
	-hdfs dfs -mkdir /user/user/input
	-hdfs dfs -mkdir /user/user/cache
	-hdfs dfs -put /home/user/Dropbox/hadoop/patent/cite75_99.txt /user/user/input
	-hdfs dfs -put /home/user/Dropbox/hadoop/patent/apat63_99.txt /user/user/cache

clean:
	rm *.class
	rm *.jar
