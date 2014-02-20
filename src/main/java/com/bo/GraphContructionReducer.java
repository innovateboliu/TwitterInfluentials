package com.bo;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class GraphContructionReducer extends Reducer<Text, Text, Text, Text>{
	
	private Set<String> set;
	
	@Override
	public void setup(Context context) {
		set = new HashSet<String>();
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
		
//		if (neighbors.length() > 0) {
//			neighbors.deleteCharAt(neighbors.length()-1);
//		}
//		
		to.insert(0, ":1,");
		
		context.write(key, new Text(to.toString()));
	}
	
	@Override
	public void cleanup(Context context ) {
		
	}

}
