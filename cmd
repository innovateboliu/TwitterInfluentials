mvn clean compile assembly:single

hadoop dfs -rm -r twitter/influential/tmp
hadoop dfs -rm -r twitter/influential/output

hadoop jar target/bigdata-twitter-influential-1.0-SNAPSHOT-jar-with-dependencies.jar ranking.retweetcount.TwitterInfluentialRetweetCountDriver twitter/FlumeData* twitter/influential/tmp twitter/influential/output

hadoop dfs -get twitter/influential/output .

hadoop jar target/bigdata-twitter-influential-1.0-SNAPSHOT-jar-with-dependencies.jar ranking.retweetcount.TwitterInfluentialPageRankDriver 
