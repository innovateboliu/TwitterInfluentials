package com.bo;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

public class GraphContructionMapper extends Mapper<LongWritable, Text, Text, Text>{
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			Status status = DataObjectFactory.createStatus(value.toString());
			String name = status.getUser().getScreenName();
			Status retweet = status.getRetweetedStatus();
			if (retweet != null) {
				String originalName = retweet.getUser().getScreenName();
				context.write(new Text(name), new Text(originalName));
			}
		} catch (TwitterException e) {
			e.printStackTrace();
		}
		
	}
}
