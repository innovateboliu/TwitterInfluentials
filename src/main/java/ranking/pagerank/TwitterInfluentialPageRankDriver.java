package ranking.pagerank;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import Utils.SecondaryComparator;
import Utils.SecondaryGroupComparator;
import Utils.SecondaryPartitioner;
import ranking.retweetcount.CompositeKeyComparator;
import ranking.retweetcount.CompositeKeyGroupingComparator;
import ranking.retweetcount.CompositeKeyPartitioner;

public class TwitterInfluentialPageRankDriver {
	
	private static int round = 1;
	
	public static void main(String[] args) throws Exception {
		
		int iteration = 1;
		Configuration conf;
		while (!isConvergent()) {
			conf = new Configuration();
			conf.set("recursion.depth", iteration + "");
			
			Job job = Job.getInstance(conf);
			job.setJobName("Graph ranking " + iteration);

			job.setMapperClass(PageRankMapper.class);
			job.setReducerClass(PageRankReducer.class);
			job.setJarByClass(TwitterInfluentialPageRankDriver.class);
			
			Path graphPath = new Path("twitter/influential/pagerank/graph/graph*");
			Path weightPath = new Path("twitter/influential/pagerank/iteration_" + (iteration - 1) + "/weight*");
			Path out = new Path("twitter/influential/pagerank/iteration_" + iteration);

			job.setOutputFormatClass(TextOutputFormat.class);
			
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			
			job.setPartitionerClass(SecondaryPartitioner.class);
			job.setGroupingComparatorClass(SecondaryGroupComparator.class);
			job.setSortComparatorClass(SecondaryComparator.class);

			MultipleInputs.addInputPath(job, graphPath, TextInputFormat.class);
			MultipleInputs.addInputPath(job, weightPath, TextInputFormat.class);
			FileOutputFormat.setOutputPath(job, out);
			
			boolean b = job.waitForCompletion(true);
			if (!b) {
				throw new IOException("error with job!");
			}
			
			iteration++;
		}
	}
	
	//TODO: to implement proper convergence check
	private static boolean isConvergent() {
		if (round > 0) {
			round--;
			return false;
		}
		return true;
	}
}

