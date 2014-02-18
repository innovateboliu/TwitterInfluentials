package com.bo;
//import java.io.IOException;
//import java.util.Comparator;
//import java.util.PriorityQueue;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Job;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Reducer;
//import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
//import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
//import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
//import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//
//import twitter4j.Status;
//import twitter4j.TwitterException;
//import twitter4j.json.DataObjectFactory;
//
//public class TwitterInfluential {
//	
//	private static final int K = 20;
//	
//	private static class  Pair<T, V> {
//		private T first;
//		private V second;
//		
//		public Pair(T first, V second) {
//			this.first = first;
//			this.second = second;
//		}
//	}
//	
//	public static class TwitterInitialMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//		
//		@Override
//		public void map(LongWritable key, Text value, Context context)
//				throws IOException, InterruptedException {
//			String content = value.toString();
//			IntWritable retweetCount = new IntWritable();
//			Text screenName = new Text();
//			Status status;
//			try {
//				status = DataObjectFactory.createStatus(content);
//				Status retweetStatus = status.getRetweetedStatus();
//				if (retweetStatus != null) {
//					retweetCount.set(retweetStatus.getRetweetCount());
//					screenName.set(retweetStatus.getUser().getScreenName());
//					context.write(screenName, retweetCount);
//				}
//			} catch (TwitterException e) {
//				e.printStackTrace();
//			}
//		}
//	}
//	
//	public static class TwitterInitialReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
//
//		private PriorityQueue<Pair<String, Integer>> pq;
//		
//		@Override 
//		public void setup(Context context){
//			pq = new PriorityQueue<Pair<String,Integer>>(K, new Comparator<Pair<String,Integer>>(){
//				public int compare(Pair<String, Integer> f1, Pair<String, Integer> f2) {  
//	                return f1.second - f2.second;  
//	            }
//			});
//		}
//		
//		@Override
//		public void reduce(Text key, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException{
//			int sum = 0;
//			for (IntWritable value : values) {
//				sum += value.get();
//			}
//			pq.add(new Pair<String, Integer>(key.toString(), sum));
//			if (pq.size() > K) {
//				pq.remove();
//			}
//		}
//		
//		@Override
//		public void cleanup(Context context) throws IOException, InterruptedException {
//			for (Pair<String, Integer> pair : pq) {
//				context.write(new Text(pair.first), new IntWritable(pair.second));
//			}
//		}
//	}
//	
//	public static class TwitterFinalMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
//		@Override
//		public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
//			String[] tokens = value.toString().split("\\s+");
//			context.write(new Text(tokens[0]), new IntWritable(Integer.parseInt(tokens[1])));
//		}
//	}
//	
//
//	public static void main(String[] args) throws Exception {
//		if (args.length != 3) {
//			System.out.println("usage: [input] [tmp] [output]");
//			System.exit(-1);
//		}
//
//		Job initialJob = Job.getInstance(new Configuration());
//		initialJob.setOutputKeyClass(Text.class);
//		initialJob.setOutputValueClass(IntWritable.class);
//
//		initialJob.setMapperClass(TwitterInitialMapper.class);
//		initialJob.setReducerClass(TwitterInitialReducer.class);
//
//		initialJob.setInputFormatClass(TextInputFormat.class);
//		initialJob.setOutputFormatClass(TextOutputFormat.class);
//		
//		initialJob.setMapOutputKeyClass(Text.class);
//		initialJob.setMapOutputValueClass(IntWritable.class);
//
//		FileInputFormat.setInputPaths(initialJob, new Path(args[0]));
//		FileOutputFormat.setOutputPath(initialJob, new Path(args[1]));
//		
//		initialJob.setNumReduceTasks(2);
//
//		initialJob.setJarByClass(TwitterInfluential.class);
//
//		boolean b = initialJob.waitForCompletion(true);
//		if (!b) {
//			throw new IOException("error with job!");
//		}
//		
//		Job finalJob = Job.getInstance(new Configuration());
//		finalJob.setOutputKeyClass(Text.class);
//		finalJob.setOutputValueClass(IntWritable.class);
//
//		finalJob.setMapperClass(TwitterFinalMapper.class);
//		finalJob.setReducerClass(TwitterInitialReducer.class);
//
//		finalJob.setInputFormatClass(TextInputFormat.class);
//		finalJob.setOutputFormatClass(TextOutputFormat.class);
//
//		FileInputFormat.setInputPaths(finalJob, new Path(args[1]));
//		FileOutputFormat.setOutputPath(finalJob, new Path(args[2]));
//
//		finalJob.setMapOutputKeyClass(Text.class);
//		finalJob.setMapOutputValueClass(IntWritable.class);
//		finalJob.setJarByClass(TwitterInfluential.class);
//
//		finalJob.setNumReduceTasks(1);
//		
//		b = finalJob.waitForCompletion(true);
//		if (!b) {
//			throw new IOException("error with job!");
//		}
//
//	}
//}
//
