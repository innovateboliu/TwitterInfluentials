package Utils;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Partitioner;

public class SecondaryPartitioner<T1 extends WritableComparable<T1>, T2 extends WritableComparable<T2>> extends Partitioner<SecondaryCompositeKey<T1, T2>, Writable> {
	@Override
	public int getPartition(SecondaryCompositeKey<T1, T2> key, Writable value, int num) {
		return (key.key1.hashCode() & Integer.MAX_VALUE) % num;
	}
}
