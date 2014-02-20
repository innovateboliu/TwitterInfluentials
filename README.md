TwitterInfluentials
===================
1. [Shows the ranking top 20 Big Data influentials in the past month](#1).
2. [Shows the network representation of the top 20 influentials](#2).

Service URL:
------------
http://twitter-stat.herokuapp.com/

Detailed Explaination:
----------------
1. <a name="1"></a>**Top 20 Big Data influentials ranking**
    - **Algorithm**: First find the max retweet count of each tweet, then sum all max retweet count of each user, finally generate the total order ranking of all users.
    - **Equivalent Hive SQL**: 
    <pre>
      <code>
      SELECT t.retweeted_screen_name, sum(retweets) AS total_retweets, count(*) AS tweet_count
      FROM (SELECT retweeted_status.user.screen_name as retweeted_screen_name, retweeted_status.text, max(retweet_count) as retweets
            FROM tweets
            GROUP BY retweeted_status.user.screen_name, retweeted_status.text) t
      GROUP BY t.retweeted_screen_name
      ORDER BY total_retweets DESC
      LIMIT 20;
      </code>

    </pre>
    
2. <a name="2"></a>**Network of Top 20 Big Data influentials**
    - **Algorithm**: First build up the directed graph among the top 20 influentials. Secondly using page rank to calculate the score of each influential. Finally when scores are convergence, output the graph.
