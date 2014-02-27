mvn clean compile assembly:single

hadoop dfs -rm -r twitter/influential/tmp
hadoop dfs -rm -r twitter/influential/output

hadoop jar target/bigdata-twitter-influential-1.0-SNAPSHOT-jar-with-dependencies.jar ranking.retweetcount.TwitterInfluentialRetweetCountDriver twitter/FlumeData* twitter/influential/tmp twitter/influential/output

hadoop dfs -get twitter/influential/output .

------------------------------------------------------------------
------------------------------------------------------------------
hadoop dfs -rm -r twitter/influential/pagerank/iteration_0

hadoop jar target/bigdata-twitter-influential-1.0-SNAPSHOT-jar-with-dependencies.jar ranking.pagerank.TwitterInfluentialGraphBuildingDriver 

------------------------------------------------------------------
------------------------------------------------------------------
hadoop dfs -rm -r twitter/influential/pagerank/iteration_1

hadoop jar target/bigdata-twitter-influential-1.0-SNAPSHOT-jar-with-dependencies.jar ranking.pagerank.TwitterInfluentialPageRankDriver 

hadoop dfs -ls twitter/influential/pagerank/iteration_0
hadoop dfs -get twitter/influential/pagerank/iteration_0 .
