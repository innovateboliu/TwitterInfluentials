package ranking.pagerank;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class GraphContructionReducer extends Reducer<Text, Text, Text, Text>{
	
	private Set<String> set;
	
	MultipleOutputs<Text, Text> output;
	
	@Override
	public void setup(Context context) {
		set = new HashSet<String>();
		output = new MultipleOutputs(context);
	}
	
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{
		for (Text value : values) {
			set.add(value.toString());
		}
		
		StringBuilder to = new StringBuilder();
		
		for (String str : set) {
			to.append(",").append(str);
		}
		
//		context.write(new NameScoreKey(key.toString(), 1), new Text(to.toString()));
		
		output.write("graph", key, new Text(to.toString()));
		output.write("weight", key, new Text("1"));
	}
	
	@Override
	public void cleanup(Context context ) {
		
	}

}
