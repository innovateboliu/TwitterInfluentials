package Utils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class SecondaryCompositeKey<T1 extends WritableComparable<T1>, T2 extends WritableComparable<T2>> implements WritableComparable<SecondaryCompositeKey<T1, T2>> {
	T1 key1;
	T2 key2;
	
	@Override
	public void readFields(DataInput in) throws IOException {
		key1.readFields(in);
		key2.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		key1.write(out);
		key2.write(out);
	}

	@Override
	public int compareTo(SecondaryCompositeKey<T1, T2> o) {
		int result = this.key1.compareTo(o.key1);
		if (result != 0) {
			return result;
		}
		
		return this.key2.compareTo(o.key2);
	}

}
