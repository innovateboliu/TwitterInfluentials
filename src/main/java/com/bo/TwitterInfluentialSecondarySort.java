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
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.TotalOrderPartitioner;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.json.DataObjectFactory;

public class TwitterInfluentialSecondarySort {
	
	private static final int K = 20;
	
	private static class  Pair<T, V> {
		private T first;
		private V second;
		
		public Pair(T first, V second) {
			this.first = first;
			this.second = second;
		}
	}
	
	private static class CompositeKey implements WritableComparable<Pair<String, Integer>>{
		Pair<String, Integer> pair;
		@Override
		public void readFields(DataInput in) throws IOException {
			String hashtag = WritableUtils.readString(in);
			Integer count = WritableUtils.readVInt(in);
			pair = new Pair<String, Integer>(hashtag, count);
		}

		@Override
		public void write(DataOutput out) throws IOException {
			WritableUtils.writeString(out, pair.first);
			WritableUtils.writeVInt(out, pair.second);
		}

		@Override
		public int compareTo(Pair<String, Integer> o) {
			int diff = this.pair.first.compareTo(o.first);
			if (diff != 0) {
				return diff;
			}
			return this.pair.second.compareTo(o.second);
		}
		
	}

	public static class CompositeKeyComparator extends WritableComparator {
		@Override
		public int compare(Object l, Object r) {
			Pair<String, Integer> a = (Pair<String, Integer>)l;
			Pair<String, Integer> b = (Pair<String, Integer>)r;
			
			int diff = a.first.compareTo(b.first);
			if (diff != 0) {
				return diff;
			}
			return b.second.compareTo(a.second);
		}
	}
	
	public static class CompositeKeyGroupComparator extends WritableComparator {
		@Override
		public int compare(WritableComparable l, WritableComparable r) {
			CompositeKey a = (CompositeKey)l;
			CompositeKey b = (CompositeKey)r;
			
			return a.pair.first.compareTo(b.pair.first);
		}
	}
	
	public static class TwitterMapper extends Mapper<LongWritable, Text, Pair<String, Integer>, IntWritable> {
		
		@Override
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String content = value.toString();
			IntWritable retweetCount = new IntWritable();
			Text screenName = new Text();
			Status status;
			try {
				status = DataObjectFactory.createStatus(content);
				Status retweetStatus = status.getRetweetedStatus();
				if (retweetStatus != null) {
					retweetCount.set(retweetStatus.getRetweetCount());
					screenName.set(retweetStatus.getUser().getScreenName());
					context.write(new Pair<String, Integer>(screenName.toString(), retweetCount.get()), retweetCount);
				}
			} catch (TwitterException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static class TwitterReducer extends Reducer<Pair<String, Integer>, IntWritable, Text, IntWritable> {

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
		if (args.length != 2) {
			System.out.println("usage: [input] [output]");
			System.exit(-1);
		}

		Job job = Job.getInstance(new Configuration());
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		job.setPartitionerClass(TotalOrderPartitioner.class);
		job.setSortComparatorClass(CompositeKeyComparator.class);
		job.setGroupingComparatorClass(CompositeKeyGroupComparator.class);
		
		job.setMapperClass(TwitterMapper.class);
		job.setReducerClass(TwitterReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(CompositeKey.class);
		job.setMapOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
//		initialJob.setNumReduceTasks(2);

		job.setJarByClass(TwitterInfluentialSecondarySort.class);

		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
	}
}

