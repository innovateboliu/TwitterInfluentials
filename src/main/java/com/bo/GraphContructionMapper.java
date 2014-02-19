package com.bo;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

public class GraphContructionMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private Set<String> desiredScreenNames = new HashSet<String>();
	
	@Override
	public void setup(Context context) throws IOException {
		URI[] cacheFileUris = context.getCacheFiles();
		buildCache(cacheFileUris);
		System.out.println("!!!!!!!!!!!!!!!!!!!! set is "+ desiredScreenNames.toString());

	}
	
	private void buildCache(URI[] cacheFileUris) throws IOException {
		FileSystem fs = FileSystem.get(new Configuration());
		for (URI uri : cacheFileUris) {
			BufferedReader reader = null;
			try {
				reader = new BufferedReader(new InputStreamReader(fs.open(new Path(uri))));
				String line;
				while ((line = reader.readLine()) != null) {
					String content[] = line.split("\\s");
					desiredScreenNames.add(content[0].trim());
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (reader != null) {
					reader.close();
				}
			}
		}
	}
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		try {
			Status status = DataObjectFactory.createStatus(value.toString());
			String name = status.getUser().getScreenName();
			Status retweet = status.getRetweetedStatus();
			if (retweet != null && desiredScreenNames.contains(name)) {
				String originalName = retweet.getUser().getScreenName();
				if (desiredScreenNames.contains(originalName)) {
					context.write(new Text(name), new Text(originalName));
				}
			}
		} catch (TwitterException e) {
			e.printStackTrace();
		}
		
	}
}
