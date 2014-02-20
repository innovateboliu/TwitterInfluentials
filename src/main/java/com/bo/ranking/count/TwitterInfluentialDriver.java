package com.bo.ranking.count;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TwitterInfluentialDriver {
	
	protected static final int K = 20;
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("usage: [input] [intermediate] [output]");
			System.exit(-1);
		}

		Job initialJob = Job.getInstance(new Configuration());
		
		initialJob.setJarByClass(TwitterInfluentialDriver.class);

		
		initialJob.setOutputKeyClass(Text.class);
		initialJob.setOutputValueClass(IntWritable.class);

		initialJob.setMapperClass(RetweetCountMapper.class);
		initialJob.setReducerClass(RetweetMaxRankReducer.class);

		initialJob.setInputFormatClass(TextInputFormat.class);
		initialJob.setOutputFormatClass(TextOutputFormat.class);
		
		initialJob.setMapOutputKeyClass(CompositeKey.class);
		initialJob.setMapOutputValueClass(CompositeValue.class);

		initialJob.setPartitionerClass(CompositeKeyPartitioner.class);
		initialJob.setGroupingComparatorClass(CompositeKeyGroupingComparator.class);
		initialJob.setSortComparatorClass(CompositeKeyComparator.class);
		
		FileInputFormat.setInputPaths(initialJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(initialJob, new Path(args[1]));
		
		initialJob.setNumReduceTasks(3);

		
		boolean b = initialJob.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
		Job finalJob = Job.getInstance(new Configuration());
		finalJob.setOutputKeyClass(Text.class);
		finalJob.setOutputValueClass(IntWritable.class);

		finalJob.setMapperClass(FinalRankingMapper.class);
		finalJob.setReducerClass(FinalRankingReducer.class);

		finalJob.setInputFormatClass(TextInputFormat.class);
		finalJob.setOutputFormatClass(TextOutputFormat.class);

		FileInputFormat.setInputPaths(finalJob, new Path(args[1]));
		FileOutputFormat.setOutputPath(finalJob, new Path(args[2]));

		finalJob.setMapOutputKeyClass(Text.class);
		finalJob.setMapOutputValueClass(IntWritable.class);
		finalJob.setJarByClass(TwitterInfluentialDriver.class);

		finalJob.setNumReduceTasks(1);
		
		b = finalJob.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
	}
}

