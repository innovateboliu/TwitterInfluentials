package com.bo.ranking.network;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class NetworkRankMapper extends Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] tokens = value.toString().split("\\s");
		String[] tmp = tokens[0].split(":");
		String from = tmp[0];
		float score = Float.parseFloat(tmp[1]);

		int numTo = tokens.length - 1;

		if (numTo < 1) {
			return;
		}

		float val = score / numTo;

		for (int i = 1; i < tokens.length; i++) {
			context.write(new Text(tokens[i]), new Text(Float.toString(val)));
		}

	}
}
