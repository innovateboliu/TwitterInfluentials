mvn clean compile assembly:single

hadoop jar target/bigdata-twitter-influential-1.0-SNAPSHOT-jar-with-dependencies.jar hdfs://localhost:8888/user/hduser/twitter/FlumeData.1391619050325 twitter/influential/tmp twitter/influential/output
