# spark-example
some spark example

wordcount
===

	mvn clean package
	hadoop fs -rm -r output # remove output folder
	hadoop fs -rm word.txt # remove old txt file
	hadoop fs -put data/word.txt word.txt # put new txt file
	spark-submit --master yarn --deploy-mode cluster --class WordCount target/spark-sample-0.0.1.jar word.txt output # run application
	hadoop fs -cat output/* #show results


sshversion
===
	
	mvn clean package
	hadoop fs -rm -r output # remove output folder
	hadoop fs -rm ssh.json # remove old json file
	hadoop fs -put data/ssh.json ssh.json # put new json file
	spark-submit --master yarn --deploy-mode cluster --class SSHVersion target/spark-sample-0.0.1.jar ssh.json output # run application
	hadoop fs -cat output/* #show results