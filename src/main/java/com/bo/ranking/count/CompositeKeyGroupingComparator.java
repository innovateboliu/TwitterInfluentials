package com.bo.ranking.count;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class  CompositeKeyGroupingComparator extends WritableComparator {
	protected CompositeKeyGroupingComparator() {
		super(CompositeKey.class, true);
	}
	@Override
	public int compare(WritableComparable  l, WritableComparable  r) {
		CompositeKey a = (CompositeKey)l;
		CompositeKey b = (CompositeKey)r;
		
		return a.pair.first.compareTo(b.pair.first);
	}
}