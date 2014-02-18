package com.bo;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.PriorityQueue;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

public class TwitterInfluential {
	
	private static final int K = 20;
	
	private static class  Pair<T, V> {
		private T first;
		private V second;
		
		public Pair(T first, V second) {
			this.first = first;
			this.second = second;
		}
	}
	
	private static class CompositeKey implements WritableComparable<CompositeKey>{
		Pair<String, Long> pair;
		
		public CompositeKey(String userName, Long tweetId) {
			this.pair = new Pair<String, Long>(userName, tweetId);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			String screenName = WritableUtils.readString(in);
			Long tweetId = WritableUtils.readVLong(in);
			pair = new Pair<String, Long>(screenName, tweetId);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, pair.first);
			WritableUtils.writeVLong(out, pair.second);
		}

		@Override
		public int compareTo(CompositeKey o) {
			int diff = this.pair.first.compareTo(o.pair.first);
			if (diff != 0) {
				return diff;
			}
			return this.pair.second.compareTo(o.pair.second);
		}
		
	}
	
	private static class CompositeValue implements Writable{
		private long tweetId;
		private int count;
		
		public CompositeValue() {}
		
		public CompositeValue(long tweetId, int count) {
			this.tweetId = tweetId;
			this.count = count;
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			this.tweetId = WritableUtils.readVLong(in);
			this.count = WritableUtils.readVInt(in);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeVLong(out, tweetId);
			WritableUtils.writeVInt(out, count);
		}
	
	}

	public static class CompositeKeyComparator extends WritableComparator {
		@Override
		public int compare(Object l, Object r) {
			CompositeKey a = (CompositeKey)l;
			CompositeKey b = (CompositeKey)r;
			
			int diff = a.pair.first.compareTo(b.pair.first);
			if (diff != 0) {
				return diff;
			}
			return b.pair.second.compareTo(a.pair.second);
		}
	}
	
	public static class CompositeKeyGroupingComparator extends WritableComparator {
		@Override
		public int compare(Object l, Object r) {
			CompositeKey a = (CompositeKey)l;
			CompositeKey b = (CompositeKey)r;
			
			return a.pair.first.compareTo(b.pair.first);
		}
	}
	
	public static class RetweetCountMapper extends Mapper<LongWritable, Text, CompositeKey, CompositeValue> {
		
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
	
	public static class CompositeKeyPartitioner extends Partitioner<CompositeKey, CompositeValue> {
		@Override
		public int getPartition(CompositeKey key, CompositeValue value, int num) {
			int hash = key.pair.first.hashCode();
			return Math.abs(hash) % num;
		}
		
	}
	
	//output top K user and his/her retweet count
	public static class RetweetMaxRankReducer extends Reducer<CompositeKey, CompositeValue, Text, IntWritable> {
		private PriorityQueue<Pair<String, Integer>> pq;
		
		@Override
		public void setup(Context context) {
			pq = new PriorityQueue<Pair<String,Integer>>(K, new Comparator<Pair<String,Integer>>(){
				public int compare(Pair<String, Integer> f1, Pair<String, Integer> f2) {  
	                return f1.second - f2.second;  
	            }
			});
		}
		
		@Override
		public void reduce(CompositeKey key, Iterable<CompositeValue> values, Context context) {
			int maxRetweetCount = 0;
			int count = 0;
			CompositeValue preValue = new CompositeValue();

			for (CompositeValue value : values) {
				if (value.tweetId == preValue.tweetId) {
					maxRetweetCount = Math.max(maxRetweetCount, value.count);
				} else {
					count += maxRetweetCount;
					maxRetweetCount = value.count;
				}
				preValue = value;
			}
			count += maxRetweetCount;
			
			pq.add(new Pair<String, Integer>(key.pair.first, count));
			if (pq.size() > K) {
				pq.remove();
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Pair<String, Integer> pair : pq) {
				context.write(new Text(pair.first), new IntWritable(pair.second));
			}
		}
	}
	
	public static class FinalRankingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		@Override
		public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
			String[] tokens = value.toString().split("\\s+");
			context.write(new Text(tokens[0]), new IntWritable(Integer.parseInt(tokens[1])));
		}
	}
	
	public static class FinalRankingReducer extends Reducer<Pair<String, Integer>, IntWritable, Text, IntWritable> {

		private PriorityQueue<Pair<String, Integer>> pq;
		
		@Override 
		public void setup(Context context){
			pq = new PriorityQueue<Pair<String,Integer>>(K, new Comparator<Pair<String,Integer>>(){
				public int compare(Pair<String, Integer> f1, Pair<String, Integer> f2) {  
	                return f1.second - f2.second;  
	            }
			});
		}
		
		@Override
		public void reduce(Pair<String, Integer> compositeKey, Iterable<IntWritable> values, Context context)  throws IOException, InterruptedException{
			int sum = 0;
			for (IntWritable value : values) {
				sum += value.get();
			}
			pq.add(new Pair<String, Integer>(compositeKey.first.toString(), sum));
			if (pq.size() > K) {
				pq.remove();
			}
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			for (Pair<String, Integer> pair : pq) {
				context.write(new Text(pair.first), new IntWritable(pair.second));
			}
		}
	}

	
	public static void main(String[] args) throws Exception {
		if (args.length != 3) {
			System.out.println("usage: [input] [intermediate] [output]");
			System.exit(-1);
		}

		Job initialJob = Job.getInstance(new Configuration());
		
		initialJob.setJarByClass(TwitterInfluential.class);

		
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
		finalJob.setJarByClass(TwitterInfluential.class);

		finalJob.setNumReduceTasks(1);
		
		b = finalJob.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
	}
}

