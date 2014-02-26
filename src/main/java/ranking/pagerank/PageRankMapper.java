package ranking.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PageRankMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\\s");
		String[] tos = null;
		System.out.println("tokens " + tokens);
		
		if (tokens.length == 3) {
			tos = tokens[2].split(",");
			System.out.println("tos " + tokens);

		} else {
			System.out.println("return due to tokens");

			return;
		}
		
		String[] tmp = tokens[0].split(":");
		float score = Float.parseFloat(tmp[1]);

		int numTo = tos.length - 1;

		if (numTo < 1) {
			System.out.println("return due to numTo");

			return;
		}

		float val = score / numTo;

		for (int i = 1; i < tos.length; i++) {
			System.out.println("to " + tos[i]);
			System.out.println("to " + val);


			context.write(new Text(tos[i]), new Text(Float.toString(val)));
		}

	}
}
