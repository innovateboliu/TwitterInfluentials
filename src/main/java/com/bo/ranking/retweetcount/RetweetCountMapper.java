package com.bo.ranking.retweetcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

public class RetweetCountMapper extends Mapper<LongWritable, Text, CompositeKey, CompositeValue> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String content = value.toString();
		IntWritable retweetCount = new IntWritable();
		
		Status status;
		try {
			status = DataObjectFactory.createStatus(content);
			Status retweetStatus = status.getRetweetedStatus();
			if (retweetStatus != null) {
				retweetCount.set(retweetStatus.getRetweetCount());
				String screenName = retweetStatus.getUser().getScreenName();
				Long tweetId = retweetStatus.getId();
				context.write(new CompositeKey(screenName, tweetId), new CompositeValue(tweetId, retweetCount.get()));
			}
		} catch (TwitterException e) {
			e.printStackTrace();
		}
	}
}