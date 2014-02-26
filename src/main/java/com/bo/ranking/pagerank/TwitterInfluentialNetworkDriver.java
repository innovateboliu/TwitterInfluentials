package com.bo.ranking.pagerank;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TwitterInfluentialNetworkDriver {
	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("usage: [cachedFile] [input] [output]");
			System.exit(-1);
		}

		Job initialJob = Job.getInstance();
		
		initialJob.addCacheFile(new URI("/user/hduser/twitter/influential/topnames"));
		initialJob.setJarByClass(TwitterInfluentialNetworkDriver.class);
		
		initialJob.setOutputKeyClass(NameScoreKey.class);
		initialJob.setOutputValueClass(Text.class);

		initialJob.setMapperClass(NetworkContructionMapper.class);
		initialJob.setReducerClass(NetworkContructionReducer.class);

		initialJob.setInputFormatClass(TextInputFormat.class);
		initialJob.setOutputFormatClass(TextOutputFormat.class);
		
		initialJob.setMapOutputKeyClass(Text.class);
		initialJob.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(initialJob, new Path(args[0]));
		FileOutputFormat.setOutputPath(initialJob, new Path(args[1]));
		
		boolean b = initialJob.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
		int iteration = 1;
		Configuration conf;
		while (!isConvergent()) {
			conf = new Configuration();
			conf.set("recursion.depth", iteration + "");
			
			Job iterativeJob = Job.getInstance(conf);
			iterativeJob.setJobName("Graph ranking " + iteration);

			iterativeJob.setMapperClass(NetworkRankMapper.class);
			iterativeJob.setReducerClass(NetworkRankReducer.class);
			iterativeJob.setJarByClass(TwitterInfluentialNetworkDriver.class);
			
			Path in = new Path("twitter/influential/ranking/iteration_" + (iteration - 1) + "/");
			Path out = new Path("twitter/influential/ranking/iteration_" + iteration);

			iterativeJob.setInputFormatClass(TextInputFormat.class);
			iterativeJob.setOutputFormatClass(TextOutputFormat.class);
			
			iterativeJob.setMapOutputKeyClass(Text.class);
			iterativeJob.setMapOutputValueClass(Text.class);
			
			iterativeJob.setOutputKeyClass(Text.class);
			iterativeJob.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(iterativeJob, in);
			FileOutputFormat.setOutputPath(iterativeJob, out);
			
			b = iterativeJob.waitForCompletion(true);
			if (!b) {
				throw new IOException("error with job!");
			}
			
			iteration++;
		}
	}
	
	//TODO: to implement proper convergence check
	private static boolean isConvergent() {
		return false;
	}
}

