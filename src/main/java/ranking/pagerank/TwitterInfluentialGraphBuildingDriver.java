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

		Job initialJob = Job.getInstance();
		
		initialJob.addCacheFile(new URI("/user/hduser/twitter/influential/topnames"));
		initialJob.setJarByClass(TwitterInfluentialGraphBuildingDriver.class);
		
		initialJob.setOutputKeyClass(NameScoreKey.class);
		initialJob.setOutputValueClass(Text.class);

		initialJob.setMapperClass(GraphContructionMapper.class);
		initialJob.setReducerClass(GraphContructionReducer.class);

		initialJob.setInputFormatClass(TextInputFormat.class);
		initialJob.setOutputFormatClass(TextOutputFormat.class);
		
		initialJob.setMapOutputKeyClass(Text.class);
		initialJob.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(initialJob, new Path("twitter/Flume*"));
		FileOutputFormat.setOutputPath(initialJob, new Path("twitter/influential/pagerank/graph"));
		
		MultipleOutputs.addNamedOutput(initialJob, "graph", TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(initialJob, "weight", TextOutputFormat.class, Text.class, Text.class);
		boolean b = initialJob.waitForCompletion(true);
		if (!b) {
			throw new IOException("error with job!");
		}
		
	}
}

