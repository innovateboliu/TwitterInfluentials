package ranking.retweetcount;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

public class CompositeKey implements WritableComparable<CompositeKey>{
	Pair<String, Long> pair;
	
	public CompositeKey() {}
	public CompositeKey(String userName, Long tweetId) {
		this.pair = new Pair<String, Long>(userName, tweetId);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		String screenName = WritableUtils.readString(in);
		Long tweetId = WritableUtils.readVLong(in);
		pair = new Pair<String, Long>(screenName, tweetId);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		WritableUtils.writeString(out, pair.first);
		WritableUtils.writeVLong(out, pair.second);
	}

	@Override
	public int compareTo(CompositeKey o) {
		int diff = this.pair.first.compareTo(o.pair.first);
		if (diff != 0) {
			return diff;
		}
		return this.pair.second.compareTo(o.pair.second);
	}
	
}