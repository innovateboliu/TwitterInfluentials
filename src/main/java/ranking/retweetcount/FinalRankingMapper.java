package ranking.retweetcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;

public class FinalRankingMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	@Override
	public void map(LongWritable key, Text value, Context context)  throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\\s+");
		context.write(new Text(tokens[0]), new IntWritable(Integer.parseInt(tokens[1])));
	}
}
