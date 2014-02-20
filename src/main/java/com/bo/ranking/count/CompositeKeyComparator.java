package com.bo.ranking.count;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CompositeKeyComparator extends WritableComparator {
	protected CompositeKeyComparator() {
		super(CompositeKey.class, true);
	}
	@Override
	public int compare(WritableComparable  l, WritableComparable  r) {
		CompositeKey a = (CompositeKey)l;
		CompositeKey b = (CompositeKey)r;
		
		int diff = a.pair.first.compareTo(b.pair.first);
		if (diff != 0) {
			return diff;
		}
		return b.pair.second.compareTo(a.pair.second);
	}
}