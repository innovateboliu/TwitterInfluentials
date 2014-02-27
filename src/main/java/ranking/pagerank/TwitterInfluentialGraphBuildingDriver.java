package ranking.pagerank;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class TwitterInfluentialGraphBuildingDriver {
	
	public static void main(String[] args) throws Exception {
//		if (args.length != 2) {
//			System.out.println("usage: [input] [output]");
//			System.exit(-1);
//		}

		Job job = Job.getInstance();
		
		job.addCacheFile(new URI("/user/hduser/twitter/influential/topnames"));
		job.setJarByClass(TwitterInfluentialGraphBuildingDriver.class);
		
		job.setOutputKeyClass(NameScoreKey.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(GraphContructionMapper.class);
		job.setReducerClass(GraphContructionReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("twitter/Flume*"));
		FileOutputFormat.setOutputPath(job, new Path("twitter/influential/pagerank/graph"));
		
		MultipleOutputs.addNamedOutput(job, "graph", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "weight", TextOutputFormat.class, Text.class, Text.class);
		boolean b = job.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
	}
}

