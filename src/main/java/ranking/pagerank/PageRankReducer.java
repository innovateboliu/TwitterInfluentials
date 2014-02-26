package ranking.pagerank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PageRankReducer extends Reducer<Text, Text, Text, Text>{
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		float score = 0;
		for (Text value : values) {
			score += Float.valueOf(value.toString());
		}
		context.write(key, new Text(Float.toString(score)));
	}

}
