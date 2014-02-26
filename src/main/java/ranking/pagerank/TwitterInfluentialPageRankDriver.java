package ranking.pagerank;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TwitterInfluentialPageRankDriver {
	
	private static int round = 1;
	
	public static void main(String[] args) throws Exception {
		
		int iteration = 1;
		Configuration conf;
		while (!isConvergent()) {
			conf = new Configuration();
			conf.set("recursion.depth", iteration + "");
			
			Job iterativeJob = Job.getInstance(conf);
			iterativeJob.setJobName("Graph ranking " + iteration);

			iterativeJob.setMapperClass(PageRankMapper.class);
			iterativeJob.setReducerClass(PageRankReducer.class);
			iterativeJob.setJarByClass(TwitterInfluentialPageRankDriver.class);
			
			Path in = new Path("twitter/influential/pagerank/iteration_" + (iteration - 1) + "/part*");
			Path out = new Path("twitter/influential/pagerank/iteration_" + iteration);

			iterativeJob.setInputFormatClass(TextInputFormat.class);
			iterativeJob.setOutputFormatClass(TextOutputFormat.class);
			
			iterativeJob.setMapOutputKeyClass(Text.class);
			iterativeJob.setMapOutputValueClass(Text.class);
			
			iterativeJob.setOutputKeyClass(Text.class);
			iterativeJob.setOutputValueClass(Text.class);

			FileInputFormat.setInputPaths(iterativeJob, in);
			FileOutputFormat.setOutputPath(iterativeJob, out);
			
			boolean b = iterativeJob.waitForCompletion(true);
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

