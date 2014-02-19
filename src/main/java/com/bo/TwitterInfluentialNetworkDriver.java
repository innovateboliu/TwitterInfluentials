package com.bo;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TwitterInfluentialNetworkDriver {

	
	public static void main(String[] args) throws Exception {
//		if (args.length != 3) {
//			System.out.println("usage: [cacheFile] [input] [output]");
//			System.exit(-1);
//		}

		Job initialJob = Job.getInstance();
		
		initialJob.addCacheFile(new URI("/user/hduser/twitter/influential/topnames"));
		initialJob.setJarByClass(TwitterInfluentialNetworkDriver.class);
		
		
		initialJob.setOutputKeyClass(Text.class);
		initialJob.setOutputValueClass(Text.class);

		initialJob.setMapperClass(GraphContructionMapper.class);
		initialJob.setReducerClass(GraphContructionReducer.class);

		initialJob.setInputFormatClass(TextInputFormat.class);
		initialJob.setOutputFormatClass(TextOutputFormat.class);
		
		initialJob.setMapOutputKeyClass(Text.class);
		initialJob.setMapOutputValueClass(Text.class);

		
		FileInputFormat.setInputPaths(initialJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(initialJob, new Path(args[1]));
		
		initialJob.setNumReduceTasks(3);
		
		
		boolean b = initialJob.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
	}
}

