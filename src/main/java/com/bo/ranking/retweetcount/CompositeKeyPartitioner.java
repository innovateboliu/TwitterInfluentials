package com.bo.ranking.retweetcount;

import org.apache.hadoop.mapreduce.Partitioner;

public class CompositeKeyPartitioner extends Partitioner<CompositeKey, CompositeValue> {
	@Override
	public int getPartition(CompositeKey key, CompositeValue value, int num) {
		int hash = key.pair.first.hashCode();
		return Math.abs(hash) % num;
	}
	
}
