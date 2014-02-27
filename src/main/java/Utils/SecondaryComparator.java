package Utils;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class SecondaryComparator<T1 extends WritableComparable<T1>, T2 extends WritableComparable<T2>> extends WritableComparator {
	protected SecondaryComparator() {
		super(SecondaryCompositeKey.class, true);
	}
	@Override
	public int compare(WritableComparable l, WritableComparable  r) {
		SecondaryCompositeKey<T1, T2> a = (SecondaryCompositeKey<T1, T2>)l;
		SecondaryCompositeKey<T1, T2> b = (SecondaryCompositeKey<T1, T2>)r;
		
		int diff = a.key1.compareTo(b.key1);
		if (diff != 0) {
			return diff;
		}
		return a.key2.compareTo(b.key2);
	}
}