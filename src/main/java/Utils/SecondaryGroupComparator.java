package Utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondaryGroupComparator <T1 extends WritableComparable<T1>, T2 extends WritableComparable<T2>> extends WritableComparator{
	protected SecondaryGroupComparator() {
		super(SecondaryCompositeKey.class, true);
	}
	@Override
	public int compare(WritableComparable  l, WritableComparable  r) {
		SecondaryCompositeKey<T1, T2> a = (SecondaryCompositeKey<T1, T2>)l;
		SecondaryCompositeKey<T1, T2> b = (SecondaryCompositeKey<T1, T2>)r;
		
		return a.key1.compareTo(b.key1);
	}
}
